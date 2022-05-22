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
        self.threshold = 300

    def register_reducer(self, reducer_id, *args):
        if reducer_id not in self.reducer_ids:
            self.reducer_ids.append(reducer_id)

    def register_mapper(self, mapper_id, *args):
        if mapper_id not in self.mapper_ids:
            self.mapper_ids.append(mapper_id)

    def update_reducer_state(self, reducer_id, queue_size, *args):
        self.reducer_state[reducer_id] = queue_size
        # print(f"queue size for reducer_id {reducer_id} is {queue_size}")
        if queue_size > min(self.reducer_state.values()) * 2 + 100:
            node_idx = int(reducer_id.split("-")[-1])
            # self.autoscale(reducer_id=reducer_id)
            print(f"halving tokens for {node_idx}")
            self.ch.halve_tokens_for_node(node_idx=node_idx)
        # print(self.reducer_state)

    def key_lookup(self, key):
        return self.ch.key_lookup(
            key
        )
    # def autoscale(self,node_idx):
    #     for mapper_id in self.mapper_ids:
    #         mapper = ray.get_actor(mapper_id)
    #         mapper.reschedule_output(node_idx=node_idx)

    def autoscaler_state(self):
        return (self.reducer_state, self.reducer_ids, self.mapper_ids)
