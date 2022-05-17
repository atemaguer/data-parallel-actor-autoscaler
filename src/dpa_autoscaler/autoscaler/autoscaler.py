import ray
from dpa_autoscaler.allocation import ConsistentHashing


class AutoScalingPolicy:
    def __init__(self, *args):
        pass


@ray.remote
class AutoScaler:
    def __init__(self, num_reducers, *args):
        self.reducer_state = {}
        self.reducer_ids = []
        self.mapper_ids = []
        self.ch = ConsistentHashing(nodes=num_reducers)
        self.threshold = 100

    def register_reducer(self, reducer_id, *args):
        if reducer_id not in self.reducer_ids:
            self.reducer_ids.append(reducer_id)

    def register_mapper(self, mapper_id, *args):
        if mapper_id not in self.mapper_ids:
            self.mapper_ids.append(mapper_id)

    def update_reducer_state(self, reducer_id, state, *args):
        self.reducer_state[reducer_id] = state
        print(self.reducer_state)

    def autoscale(self, reducer_id, *args):
        pass

    def autoscaler_state(self):
        return (self.actor_state, self.reducer_ids, self.mapper_ids)
