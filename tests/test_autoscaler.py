import argparse
import time
import ray
import collections
from ray.util.queue import Queue

from dpa_autoscaler.coordinator import MapReduceCoordinator
from dpa_autoscaler.config import WORKLOADS
from dpa_autoscaler.executors import map_func, Reducer
from dpa_autoscaler.autoscaler import AutoScaler


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--autoscale", action="store_true")
    parser.add_argument("--doubling", action="store_true")
    parser.add_argument("--workload", type=int, default=1)
    parser.add_argument("--threshold", type=float, default=0.2)
    parser.add_argument("--rounds", type=int, default=1)
    args = parser.parse_args()
    autoscale = args.autoscale
    wl = args.workload
    ch_type = "doubling" if args.doubling else "halving"
    threshold = args.threshold
    rounds = args.rounds
    print(
        f"Running workload={wl} with autoscale={autoscale}, ch type={ch_type}, threshold={threshold}, rounds={rounds}"
    )

    ray.init(ignore_reinit_error=True)

    NUM_MAPPERS = 4
    NUM_REDUCERS = 4

    out_queue = Queue()
    reduce_func = Reducer()

    autoscaler = AutoScaler.options(name="autoscaler").remote(
        num_reducers=NUM_REDUCERS,
        ch_type=ch_type,
        threshold=threshold,
        rounds=rounds,
    )

    coord = MapReduceCoordinator.options(name="coordinator").remote(
        WORKLOADS[f"wl{wl}"],
        NUM_MAPPERS,
        NUM_REDUCERS,
        map_func,
        reduce_func,
        out_queue,
        ch_type=ch_type,
        autoscale=autoscale,
    )

    ray.get(coord.run.remote())

    while not ray.get(coord.is_done.remote()):
        time.sleep(5)

    # print(ray.get(autoscaler.autoscaler_state.remote()))
    print(f"Running time is: {ray.get(coord.running_time.remote())}")

    d = collections.defaultdict(int)
    while not out_queue.empty():
        out = out_queue.get()
        print(out)
        for k, v in out.items():
            d[k] += v
    print(len(d))
    print(d)
