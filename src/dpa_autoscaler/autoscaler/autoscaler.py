import collections
import ray
from dpa_autoscaler.allocation import ConsistentHashing, ConsistentHashingDouble


class AutoScalingPolicy:
    def __init__(self, *args):
        pass


@ray.remote
class AutoScaler:
    def __init__(self, num_reducers, ch_type="halving", threshold=0.2, rounds=1, *args):
        self.reducer_state = {}
        self.reducer_hits = collections.defaultdict(int)
        self.reducer_triggers = collections.defaultdict(int)
        self.reducer_ids = []
        self.mapper_ids = []
        ch_cls = ConsistentHashing if ch_type == "halving" else ConsistentHashingDouble
        self.ch = ch_cls(nodes=num_reducers)
        self.threshold = threshold
        self.rounds = rounds

    def register_reducer(self, reducer_id, *args):
        if reducer_id not in self.reducer_ids:
            self.reducer_ids.append(reducer_id)

    def register_mapper(self, mapper_id, *args):
        if mapper_id not in self.mapper_ids:
            self.mapper_ids.append(mapper_id)

    def update_reducer_state(self, reducer_id, queue_size, *args):
        self.reducer_hits[reducer_id] += 1
        hits = self.reducer_hits[reducer_id]
        self.reducer_state[reducer_id] = queue_size
        # print(f"queue size for reducer_id {reducer_id} is {queue_size}")
        if self.reducer_triggers[reducer_id] > self.rounds:
            return
        first = len(self.reducer_state) == 1
        if first and hits == 1:
            return
        if first or queue_size > sorted(self.reducer_state.values())[-2] * (
            1 + self.threshold
        ):
            node_idx = int(reducer_id.split("-")[-1])
            # self.autoscale(reducer_id=reducer_id)
            print(f"updating tokens for node_id={node_idx}")
            self.ch.halve_tokens_for_node(node_idx=node_idx)
            self.reducer_triggers[reducer_id] += 1
        # print(self.reducer_state)

    def key_lookup(self, key):
        return self.ch.key_lookup(key)

    # def autoscale(self,node_idx):
    #     for mapper_id in self.mapper_ids:
    #         mapper = ray.get_actor(mapper_id)
    #         mapper.reschedule_output(node_idx=node_idx)

    def autoscaler_state(self):
        return self.reducer_state, self.reducer_ids, self.mapper_ids
