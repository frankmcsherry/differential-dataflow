//! High-fan-out join microbenchmark: `keys ⋈ data` where each `data` key has
//! `fan` vals, so the join's per-key val iteration is the dominant cost — the
//! regime where layout (flat vs columnar) matters most, unlike `spines`.
//!
//! `cargo run --release --example fanout -- <keys> <fan> <mode> [keytype]`
//! mode: col | vec | val ;  keytype: u64 (default) | str

use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Build the `keys ⋈ data` dataflow for key type `$K`, dispatching on `mode`.
macro_rules! build {
    ($scope:expr, $probe:expr, $mode:expr, $K:ty) => {{
        use differential_dataflow::Hashable;
        let (data_in, data) = $scope.new_collection::<($K, u64), isize>();
        let (keys_in, keys) = $scope.new_collection::<$K, isize>();
        let keys = keys.map(|k| (k, ()));
        match $mode {
            "col" => {
                use differential_dataflow::columnar::trace::{Batcher, Builder, Spine, ColChunk};
                use differential_dataflow::trace::implementations::chunker::ContainerChunker;
                use differential_dataflow::operators::arrange::arrangement::arrange_core;
                use timely::dataflow::channels::pact::Exchange;
                let dex = Exchange::new(|u: &(($K, u64), u64, isize)| (u.0).0.hashed().into());
                let data = arrange_core::<_, _,
                    ContainerChunker<ColChunk<($K, u64, u64, isize)>>,
                    Batcher<$K, u64, u64, isize>, Builder<$K, u64, u64, isize>, Spine<$K, u64, u64, isize>,
                >(data.inner, dex, "Data");
                let kex = Exchange::new(|u: &(($K, ()), u64, isize)| (u.0).0.hashed().into());
                let keys = arrange_core::<_, _,
                    ContainerChunker<ColChunk<($K, (), u64, isize)>>,
                    Batcher<$K, (), u64, isize>, Builder<$K, (), u64, isize>, Spine<$K, (), u64, isize>,
                >(keys.inner, kex, "Keys");
                keys.join_core(data, |_k, _, _| Option::<()>::None).probe_with($probe);
            }
            "vec" => {
                use differential_dataflow::trace::chunk::vec::{ChunkBatcher, ChunkBuilder, ChunkSpine, VecChunk};
                use differential_dataflow::trace::implementations::chunker::ContainerChunker;
                use differential_dataflow::operators::arrange::arrangement::arrange_core;
                use timely::dataflow::channels::pact::Exchange;
                let dex = Exchange::new(|u: &(($K, u64), u64, isize)| (u.0).0.hashed().into());
                let data = arrange_core::<_, _,
                    ContainerChunker<VecChunk<$K, u64, u64, isize>>,
                    ChunkBatcher<$K, u64, u64, isize>, ChunkBuilder<$K, u64, u64, isize>, ChunkSpine<$K, u64, u64, isize>,
                >(data.inner, dex, "Data");
                let kex = Exchange::new(|u: &(($K, ()), u64, isize)| (u.0).0.hashed().into());
                let keys = arrange_core::<_, _,
                    ContainerChunker<VecChunk<$K, (), u64, isize>>,
                    ChunkBatcher<$K, (), u64, isize>, ChunkBuilder<$K, (), u64, isize>, ChunkSpine<$K, (), u64, isize>,
                >(keys.inner, kex, "Keys");
                keys.join_core(data, |_k, _, _| Option::<()>::None).probe_with($probe);
            }
            "val" => {
                use differential_dataflow::operators::arrange::Arrange;
                use differential_dataflow::trace::implementations::ord_neu::{OrdValBatcher, RcOrdValBuilder, OrdValSpine};
                let data = data.arrange::<OrdValBatcher<$K, u64, _, isize>, RcOrdValBuilder<$K, u64, _, isize>, OrdValSpine<$K, u64, _, isize>>();
                let keys = keys.arrange::<OrdValBatcher<$K, (), _, isize>, RcOrdValBuilder<$K, (), _, isize>, OrdValSpine<$K, (), _, isize>>();
                keys.join_core(data, |_k, _, _| Option::<()>::None).probe_with($probe);
            }
            other => panic!("mode must be col | vec | val, got {other:?}"),
        }
        (data_in, keys_in)
    }};
}

fn main() {
    let keys: u64 = std::env::args().nth(1).unwrap().parse().unwrap();
    let fan: u64 = std::env::args().nth(2).unwrap().parse().unwrap();
    let mode: String = std::env::args().nth(3).unwrap();
    let keytype: String = std::env::args().nth(4).unwrap_or_else(|| "u64".into());
    println!("keys={keys} fan={fan} mode={mode} keytype={keytype} ({} data records)", keys * fan);

    let timer = ::std::time::Instant::now();
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = Handle::new();
        let me = worker.index();
        if keytype == "str" {
            let (mut data_in, mut keys_in) = worker.dataflow(|scope| build!(scope, &mut probe, mode.as_str(), String));
            if me == 0 {
                for k in 0..keys {
                    let ks = format!("{k:09}");
                    for v in 0..fan { data_in.insert((ks.clone(), v)); }
                    keys_in.insert(ks);
                }
            }
            data_in.advance_to(1); data_in.flush();
            keys_in.advance_to(1); keys_in.flush();
            while probe.less_than(data_in.time()) { worker.step(); }
        } else {
            let (mut data_in, mut keys_in) = worker.dataflow(|scope| build!(scope, &mut probe, mode.as_str(), u64));
            if me == 0 {
                for k in 0..keys {
                    for v in 0..fan { data_in.insert((k, v)); }
                    keys_in.insert(k);
                }
            }
            data_in.advance_to(1); data_in.flush();
            keys_in.advance_to(1); keys_in.flush();
            while probe.less_than(data_in.time()) { worker.step(); }
        }
        println!("{:?}\tjoin complete", timer.elapsed());
    }).unwrap();
}
