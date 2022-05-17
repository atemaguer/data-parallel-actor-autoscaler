import time
import ray

from ray.util.queue import Queue

from dpa_autoscaler.coordinator import MapReduceCoordinator
from dpa_autoscaler.config import data
from dpa_autoscaler.executors import map_func, reducer

ray.init(ignore_reinit_error=True)

print("Running experiments:")

for (num_mappers, reducers) in [(4, 10)]:
    out_queue = Queue()
    reduce_func = reducer()

    coord = MapReduceCoordinator.options(name="coordinator").remote(
        data, num_mappers, reducers, map_func, reduce_func, out_queue
    )

    coord.run.remote()

    while not ray.get(coord.is_done.remote()):
        time.sleep(1)

    print(f"Experiment using {num_mappers} mappers and {reducers} reducers: \n")
    print(f"Running time is: {ray.get(coord.running_time.remote())}")

    ray.kill(coord)
