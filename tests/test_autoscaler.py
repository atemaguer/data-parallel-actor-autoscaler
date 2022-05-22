import time
import ray
import collections

from ray.util.queue import Queue

from dpa_autoscaler.coordinator import MapReduceCoordinator
from dpa_autoscaler.config import data
from dpa_autoscaler.executors import map_func, reduce_func, reducer
from dpa_autoscaler.autoscaler import AutoScaler

ray.init(ignore_reinit_error=True)

NUM_MAPPERS = 4
NUM_REDUCERS = 4

out_queue = Queue()
reduce_func = reducer()

autoscaler = AutoScaler.options(name="autoscaler").remote(NUM_REDUCERS)

coord = MapReduceCoordinator.options(name="coordinator").remote(
    data, NUM_MAPPERS, NUM_REDUCERS, map_func, reduce_func, out_queue, autoscale=False
)

ray.get(coord.run.remote())

while not ray.get(coord.is_done.remote()):
    time.sleep(5)

# print(ray.get(autoscaler.autoscaler_state.remote()))
print(f"Running time is: {ray.get(coord.running_time.remote())}")

d = collections.defaultdict(int)
while not out_queue.empty():
    for k, v in out_queue.get().items():
        d[k] += v
print(len(d))
print(d)
