//! Resident workers and a frontier catalog, with a fresh resolver dataflow for each batch.

use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread::JoinHandle;

use super::{Error, Resolver};
use crate::runtime::container::Updates;

type Outputs = BTreeMap<String, Updates>;

struct Request {
    commands: Updates,
    reply: mpsc::Sender<Result<Outputs, String>>,
}

pub(super) struct Engine {
    senders: Vec<mpsc::Sender<Request>>,
    thread: Option<JoinHandle<Result<(), String>>>,
}

impl Engine {
    pub(super) fn new(resolver: &Resolver, initial: Updates, registered: Updates) -> Result<Self, Error> {
        if !resolver.program.root.exports.iter().any(|output| output.name == "initial_delta") {
            return Err(Error::Resolve("resident resolver needs initial_delta".into()));
        }
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..resolver.workers {
            let (sender, receiver) = mpsc::channel::<Request>();
            senders.push(sender);
            receivers.push(Mutex::new(receiver));
        }
        let mut program = resolver.program.clone();
        program.root.exports.retain(|output| matches!(output.name.as_str(), "aborted" | "registry_add" | "manifest_ordered" | "initial_delta" | "registered_delta"));
        crate::passes::optimize(&mut program);
        for import in &mut program.root.imports {
            if matches!(import.from, crate::scope_ir::Source::Input(0)) {
                import.from = crate::scope_ir::Source::Trace(crate::server::output_name("catalog", "current"));
            } else if matches!(import.from, crate::scope_ir::Source::Input(2)) {
                import.from = crate::scope_ir::Source::Trace(crate::server::output_name("catalog", "registered"));
            }
        }
        let mut catalog = crate::lower::lower_tree(crate::parse::pipe::parse(
            "let current = input 0 : (((int, int)), (List(List(int)))); output \"current\" = current; \
             let registered = input 1 : (((int, int)), (List(List(int)))); output \"registered\" = registered;"
        ));
        crate::passes::optimize(&mut catalog);
        let workers = resolver.workers;
        let barrier = Arc::new(Barrier::new(workers));
        let failure = Arc::new(Mutex::new(None::<String>));
        let (ready, receiver) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let guards = timely::execute(timely::Config::process(workers), move |worker| {
                let mut server = crate::server::Server::new();
                server.set_passes(crate::passes::Config::none());
                let installed = server.install(worker, "catalog", &catalog)
                    .and_then(|_| server.feed_columns("catalog", 0, shard(&initial, worker.index(), worker.peers()), None))
                    .and_then(|_| server.feed_columns("catalog", 1, shard(&registered, worker.index(), worker.peers()), None));
                if let Err(error) = installed { *failure.lock().unwrap() = Some(error); }
                barrier.wait();
                let status = failure.lock().unwrap().clone();
                if status.is_none() { server.tick(worker); }
                let _ = ready.send(status.clone().map_or(Ok(()), Err));
                if status.is_some() { return; }

                while let Ok(request) = receivers[worker.index()].lock().unwrap().recv() {
                    let staged = server.install(worker, "resolve", &program).and_then(|_| {
                        server.feed_columns("resolve", 1, shard(&request.commands, worker.index(), worker.peers()), None)
                    });
                    if let Err(error) = staged { *failure.lock().unwrap() = Some(error); }
                    // Every worker must finish staging successfully before any enters a collective tick.
                    barrier.wait();
                    if let Some(error) = failure.lock().unwrap().clone() {
                        let _ = request.reply.send(Err(error));
                        break;
                    }
                    server.tick(worker);
                    let outputs = program.root.exports.iter().filter(|output| !matches!(output.name.as_str(), "initial_delta" | "registered_delta")).map(|output| {
                        server.snapshot(worker, &crate::server::output_name("resolve", &output.name))
                            .map(|updates| (output.name.clone(), updates))
                    }).collect::<Result<Outputs, _>>();
                    if let Err(error) = &outputs { *failure.lock().unwrap() = Some(error.clone()); }
                    barrier.wait();
                    let status = failure.lock().unwrap().clone();
                    if let Some(error) = status {
                        let _ = request.reply.send(Err(error));
                        break;
                    }
                    // Absorb the local conclusion shard without gathering the catalog or delta on worker 0.
                    // Drop the resolver before advancing its imported base, so no old proposals are replayed.
                    let absorbed = server.load(worker, "catalog", 0, &crate::server::output_name("resolve", "initial_delta"), 1)
                        .and_then(|_| server.load(worker, "catalog", 1, &crate::server::output_name("resolve", "registered_delta"), 1))
                        .and_then(|_| server.drop_program(worker, "resolve"));
                    if let Err(error) = absorbed { *failure.lock().unwrap() = Some(error); }
                    barrier.wait();
                    if let Some(error) = failure.lock().unwrap().clone() {
                        let _ = request.reply.send(Err(error));
                        break;
                    }
                    server.tick(worker);
                    let _ = request.reply.send(outputs);
                }
            }).map_err(|error| error.to_string())?;
            for result in guards.join() { result.map_err(|error| error.to_string())?; }
            Ok(())
        });
        let mut engine = Self { senders, thread: Some(thread) };
        for _ in 0..workers {
            let status = receiver.recv().map_err(|_| Error::Resolve("resident resolver stopped during installation".into()))?;
            if let Err(error) = status {
                engine.stop();
                return Err(Error::Resolve(error));
            }
        }
        Ok(engine)
    }

    pub(super) fn resolve(&mut self, commands: Updates) -> Result<Outputs, Error> {
        if self.senders.is_empty() { return Err(Error::Resolve("resident resolver has stopped".into())); }
        let mut receivers = Vec::new();
        for sender in &self.senders {
            let (reply, receiver) = mpsc::channel();
            if sender.send(Request { commands: commands.clone(), reply }).is_err() {
                self.stop();
                return Err(Error::Resolve("resident resolver worker stopped".into()));
            }
            receivers.push(receiver);
        }
        let mut outputs = None;
        for (worker, receiver) in receivers.into_iter().enumerate() {
            match receiver.recv() {
                Ok(Ok(result)) => { if worker == 0 { outputs = Some(result); } }
                result => {
                    self.stop();
                    return Err(Error::Resolve(match result {
                        Ok(Err(error)) => error,
                        Err(_) => "resident resolver worker stopped".into(),
                        Ok(Ok(_)) => unreachable!(),
                    }));
                }
            }
        }
        super::resolve::public_outputs(outputs.ok_or_else(|| Error::Resolve("resident resolver returned no results".into()))?)
    }

    fn stop(&mut self) {
        self.senders.clear();
        if let Some(thread) = self.thread.take() { let _ = thread.join(); }
    }
}

impl Drop for Engine {
    fn drop(&mut self) { self.stop(); }
}

fn shard(updates: &Updates, index: usize, peers: usize) -> Updates {
    updates.select(&(index..updates.len()).step_by(peers).collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::resolve::{ids, frontiers};

    #[test]
    fn feed_errors_stop_and_join_workers() {
        let resolver = Resolver::new(2).unwrap();
        let wrong = Updates { keys: corgi::Value::int(vec![0]), vals: corgi::Value::Unit(1), diffs: vec![1] };
        let initial = Updates {
            keys: corgi::Value::Prod(vec![ids(std::iter::empty())]),
            vals: corgi::Value::Prod(vec![frontiers(std::iter::empty(), 0)]),
            diffs: Vec::new(),
        };
        assert!(Engine::new(&resolver, wrong.clone(), initial.clone()).is_err());
        let mut engine = Engine::new(&resolver, initial.clone(), initial).unwrap();
        assert!(engine.resolve(wrong.clone()).is_err());
        assert!(engine.resolve(wrong).is_err());
    }
}
