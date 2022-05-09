import ray


class AutoScalingPolicy:
    def __init__(self, *args):
        pass


@ray.remote
class AutoScaler:
    def __init__(self, *args):
        self.actor_state = {}
        self.threshold = 100

    def update_actor_state(self, actor_id, state, *args):
        self.actor_state[actor_id] = state

    def register_actor(self, actor_id, *args):
        pass

    def autoscale(self, actor_id, *args):
        pass

    def autoscaler_state(self):
        return self.actor_state
