//! Per-input point capabilities and per-export progress. Program clocks are convenience only.

use super::*;

/// A time as a client writes it: its first coordinate alone when the others are zero.
pub(super) fn coordinates(time: &HostTime) -> String {
    if time.len() <= 1 { time.get(0).copied().unwrap_or(0).to_string() } else { format!("{:?}", time.to_vec()) }
}

pub(super) fn caught_up(probe: &ProbeHandle<HostTime>, point: &HostTime) -> bool {
    probe.with_frontier(|frontier| frontier.iter().all(|t| point.less_equal(t)))
}

impl Server {
    /// The default point for this program's next bulk advance.
    pub fn now(&self, prog: &str) -> Result<&HostTime, String> {
        Ok(&self.programs.get(&canonical_source_name(prog)).ok_or_else(|| format!("no program {prog:?}"))?.now)
    }

    /// This input's earliest accepted point; other inputs may have different frontiers.
    pub fn input_frontier(&self, prog: &str, input: usize) -> Result<&HostTime, String> {
        let installed = self.programs.get(&canonical_source_name(prog)).ok_or_else(|| format!("no program {prog:?}"))?;
        Ok(installed.inputs.get(&input).ok_or_else(|| format!("program {prog:?} has no input {input}"))?.time())
    }

    /// Advance just one positional input. Collective across workers; does not wait.
    /// Points are trimmed: omitted coordinates are zero, not inferred from the clock.
    pub fn advance_input(&mut self, prog: &str, input: usize, point: &HostTime) -> Result<(), String> {
        self.validate_feed(prog, input, point)?;
        self.programs.get_mut(&canonical_source_name(prog)).unwrap().inputs.get_mut(&input).unwrap().advance_to_point(point);
        Ok(())
    }

    /// Whether `prog` is piloted: a client moves its inputs, and `tick` leaves them be.
    pub fn is_piloted(&self, prog: &str) -> bool {
        self.programs.get(&canonical_source_name(prog)).is_some_and(|p| p.pace == Pace::Piloted)
    }

    /// Whether `trace` is published by a piloted program.
    pub fn trace_is_piloted(&self, trace: &str) -> bool {
        self.outputs.get(&canonical_source_name(trace)).is_some_and(|o| self.is_piloted(&o.owner))
    }

    /// The frontier `trace` has once it has caught up with every `tick` and `advance` so far:
    /// the current epoch, or for a piloted program's trace the least of the times that its
    /// inputs, and those of the programs it reads, stand at. A reader of the trace that has
    /// reached this frontier has every change the commands so far have made.
    pub fn target(&self, trace: &str) -> Result<Antichain<HostTime>, String> {
        let name = canonical_source_name(trace);
        let output = self.outputs.get(&name).ok_or_else(|| format!("no trace {trace:?}"))?;
        let mut frontier = Antichain::new();
        if self.trace_is_piloted(&name) {
            self.collect_frontier(&output.owner, &mut std::collections::HashSet::new(), &mut frontier);
        } else {
            frontier.insert(host_time(self.epoch));
        }
        Ok(frontier)
    }

    /// The time a reader of `trace` started now reads it as of: [`Server::import_as_of`], or
    /// for a piloted program's trace its own compaction frontier, which no epoch moves.
    pub fn read_as_of(&self, trace: &str) -> HostTime {
        match self.outputs.get(&canonical_source_name(trace)) {
            Some(output) if self.is_piloted(&output.owner) => output.since.clone(),
            _ => self.import_as_of(),
        }
    }

    /// Move one input of a piloted program to `time`, alone: no other input moves, of this
    /// program or of any other. This does not wait for what follows from it: [`Server::spin`]
    /// does, once, for as many inputs as were moved. Collective across workers.
    ///
    /// The input must be at or before `time`. What was fed to it at times before `time` takes
    /// effect; nothing may be fed to it before `time` afterwards.
    pub fn advance(&mut self, prog: &str, input: usize, time: &HostTime) -> Result<(), String> {
        if !self.is_piloted(prog) {
            return Err(format!("cannot advance input {prog:?}/{input}: `tick` moves this program's inputs; install it piloted to move them one at a time"));
        }
        let frontier = self.input_frontier(prog, input)?;
        if !frontier.less_equal(time) {
            return Err(format!("cannot advance input {prog:?}/{input} to {}: it stands at {}", coordinates(time), coordinates(frontier)));
        }
        self.advance_input(prog, input, time)
    }

    /// Run until every piloted program's outputs have caught up with their inputs, and let
    /// those outputs compact. Collective across workers.
    pub fn spin(&mut self, worker: &mut Worker) { self.settle_piloted(worker); }

    /// Run until every piloted program's outputs have caught up with what was commanded, then
    /// let each compact to just before its frontier, as `tick` compacts the rest to just before
    /// the epoch: [`Server::peek`] reads the last complete time, so it must stay distinct.
    pub(super) fn settle_piloted(&mut self, worker: &mut Worker) {
        let names: Vec<_> = self.outputs.iter().filter(|(_, o)| o.width == 1 && self.programs[&o.owner].pace == Pace::Piloted).map(|(n, _)| n.clone()).collect();
        for name in names {
            let frontier = self.target(&name).expect("a published trace has a frontier");
            self.wait_for_frontier(worker, &name, &frontier).expect("a width-one frontier fits a width-one trace");
            if let Some(least) = frontier.elements().iter().map(|t| t.get(0).copied().unwrap_or(0)).min() {
                let since = self.outputs[&name].since.join(&host_time(least.saturating_sub(1)));
                self.compact(&name, &since).expect("monotone compaction of a piloted trace");
            }
        }
    }

    /// Wait for this export alone to reach a point frontier. Advance all of its producers
    /// first: this does not advance imports or unrelated inputs. Collective across workers.
    pub fn wait_for(&self, worker: &mut Worker, trace: &str, point: &HostTime) -> Result<(), String> {
        let output = self.output(trace, point)?;
        while !caught_up(&output.probe, point) { worker.step_or_park(None); }
        Ok(())
    }

    /// Advance every input of one program, then wait for each of its outputs. Each input
    /// must accept the point; a rejected advance changes nothing. Waits for the frontier
    /// of local and imported inputs; imports are advanced separately. Keeps one system
    /// epoch of history, all other coordinates.
    pub fn advance_to(&mut self, worker: &mut Worker, prog: &str, point: &HostTime) -> Result<(), String> {
        let name = canonical_source_name(prog);
        let installed = self.programs.get(&name).ok_or_else(|| format!("no program {prog:?}"))?;
        if point.len() > installed.root_width || !installed.now.less_equal(point) {
            return Err(format!("invalid advance from {:?} to {point:?}", installed.now));
        }
        let inputs: Vec<_> = installed.inputs.keys().copied().collect();
        let exports = installed.exports.clone();
        for &input in &inputs {
            let frontier = installed.inputs[&input].time();
            if !frontier.less_equal(point) {
                return Err(format!("cannot advance input {name:?}/{input} from frontier {frontier:?} to requested target {point:?}"));
            }
            self.validate_feed(&name, input, point)?;
        }
        for input in inputs { self.advance_input(&name, input, point)?; }
        self.programs.get_mut(&name).unwrap().now = point.clone();
        let mut frontier = Antichain::new();
        self.collect_frontier(&name, &mut std::collections::HashSet::new(), &mut frontier);
        for export in exports {
            self.wait_for_frontier(worker, &export, &frontier)?;
            let since = self.outputs[&export].since.join(&host_time(point.get(0).copied().unwrap_or(0).saturating_sub(1)));
            self.compact(&export, &since)?;
        }
        Ok(())
    }

    /// Wait for an output to reach an antichain frontier. A widened import may remain
    /// at [s, 0] while a local input is at [s-1, e]; neither point subsumes the other.
    pub fn wait_for_frontier(&self, worker: &mut Worker, trace: &str, frontier: &Antichain<HostTime>) -> Result<(), String> {
        let output = self.outputs.get(&canonical_source_name(trace)).ok_or_else(|| format!("no trace {trace:?}"))?;
        if frontier.elements().iter().any(|t| t.len() > output.width) { return Err("frontier exceeds trace width".into()); }
        while output.probe.with_frontier(|f| f.iter().any(|t| !frontier.less_equal(t))) { worker.step_or_park(None); }
        Ok(())
    }

    // A conservative target derived from commanded capabilities, never worker-local probes.
    // Imports form a DAG (producers exist before consumers); the set avoids revisiting diamonds.
    pub(super) fn collect_frontier(&self, prog: &str, seen: &mut std::collections::HashSet<String>, frontier: &mut Antichain<HostTime>) {
        if !seen.insert(prog.to_string()) { return; }
        let installed = &self.programs[prog];
        for input in installed.inputs.values() { frontier.insert(input.time().clone()); }
        for trace in &installed.imports { self.collect_frontier(&self.outputs[trace].owner, seen, frontier); }
    }

    /// Program tick: advance coordinate zero by one using per-input advances and output waits.
    pub fn tick_program(&mut self, worker: &mut Worker, prog: &str) -> Result<(), String> {
        let mut next = self.now(prog)?.to_vec();
        if next.is_empty() { next.push(0); }
        next[0] = next[0].checked_add(1).ok_or("epoch overflow")?;
        self.advance_to(worker, prog, &HostTime::new(next.into()))
    }

    /// Discard distinctions before this point in one published trace. Collective and monotone.
    /// Readers/importers retain their own compaction holds. Reads below this floor are refused.
    pub fn compact(&mut self, trace: &str, since: &HostTime) -> Result<(), String> {
        let name = canonical_source_name(trace);
        let output = self.output(&name, since)?;
        if !output.since.less_equal(since) { return Err("compaction must advance monotonically".into()); }
        let frontier = Antichain::from_elem(since.clone());
        let trace = self.traces.get_mut(&name).unwrap();
        trace.set_logical_compaction(frontier.borrow());
        trace.set_physical_compaction(frontier.borrow());
        self.outputs.get_mut(&name).unwrap().since = since.clone();
        Ok(())
    }

    /// The declared width and readable history are independent of the stored point's length.
    pub fn trace_width(&self, trace: &str) -> Option<usize> {
        self.outputs.get(&canonical_source_name(trace)).map(|o| o.width)
    }

    pub fn trace_since(&self, trace: &str) -> Option<&HostTime> {
        self.outputs.get(&canonical_source_name(trace)).map(|o| &o.since)
    }

    pub(super) fn output(&self, name: &str, point: &HostTime) -> Result<&Output, String> {
        let output = self.outputs.get(&canonical_source_name(name)).ok_or_else(|| format!("no trace {name:?}"))?;
        if point.len() > output.width { return Err(format!("point {point:?} exceeds trace {name:?}'s width {}", output.width)); }
        Ok(output)
    }
}
