import ray


@ray.remote
class AutoScaler:
    def __init__(self, *args):
        pass

    def update_actor_state(self, actor_id, *args):
        pass

    def register_actor(self, actor_id, *args):
        pass

    def autoscale(self, actor_id, *args):
        pass
