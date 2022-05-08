import time
import ray

from ray.util.queue import Queue

from dpa_autoscaler.coordinator import MapReduceCoordinator
from dpa_autoscaler.config import data
from dpa_autoscaler.executors import map_func, reducer
from dpa_autoscaler.autoscaler import AutoScaler

ray.init(address="auto", ignore_reinit_error=True)

NUM_MAPPERS = 2
NUM_REDUCERS = 4

out_queue = Queue()
red = reducer()
autoscaler = AutoScaler.options(name="autoscaler").remote()
coord = MapReduceCoordinator.options(name="coordinator").remote(
    data, NUM_MAPPERS, NUM_REDUCERS, map_func, red, out_queue
)


coord.run.remote()

while not ray.get(coord.is_done.remote()):
    print(ray.get(autoscaler.autoscaler_state.remote()))
    print(out_queue.size())
    time.sleep(1)
