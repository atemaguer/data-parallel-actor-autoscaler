import time
import ray

from ray.util.queue import Queue

from dpa_autoscaler.coordinator import MapReduceCoordinator
from dpa_autoscaler.config import data
from dpa_autoscaler.executors import map_func, reduce_func, reducer
from dpa_autoscaler.autoscaler import AutoScaler

ray.init(ignore_reinit_error=True)

NUM_MAPPERS = 4
NUM_REDUCERS = 10

out_queue = Queue()
reduce_func = reducer()

coord = MapReduceCoordinator.options(name="coordinator").remote(
    data, NUM_MAPPERS, NUM_REDUCERS, map_func, reduce_func, out_queue, autoscale=True
)

ray.get(coord.run.remote())

autoscaler = ray.get_actor("autoscaler")

while not ray.get(coord.is_done.remote()):
    time.sleep(5)

print(ray.get(autoscaler.autoscaler_state.remote()))
