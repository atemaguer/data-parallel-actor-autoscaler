"""
Consistent hashing.
"""

from typing import Dict

import mmh3

from . import config


class ConsistentHashing:
    """
    - Nodes are identified by consecutive indices.
    - Each node gets a configurable (and modifiable) number of tokens (aka virtual nodes).
    - Nodes with more tokens are more likely to be responsible for a larger portion of the keyspace.
    - Can add/remove nodes to/from the back of the list of nodes.
    """

    def __init__(self, nodes: int, seed: int = config.MMH3_HASH_SEED):
        """
        Initialize consistent hashing.
        :param nodes: int number of nodes for initializing the ring. must be >= 1.
        :param seed: int seed for mmh3 hash.
        """
        if nodes < 1 or not isinstance(nodes, int):
            raise ValueError("nodes need to be int and at least 1.")
        if not isinstance(seed, int):
            raise ValueError("seed must be int")

        self.nodes = nodes
        self.seed = seed
        self.node_tokens = {idx: 1 for idx in range(self.nodes)}
        self.token_hashes = {}
        self._update_hashes()

    def key_lookup(self, key: str) -> int:
        """
        :param key: key to look up.
        :return: index of the node responsible for this node.
        this implementation is O(N), can we do better?!
        """
        key_hash = self._hash(key)
        return (
            self.node_with_smallest_hash
            if key_hash > max(self.token_hashes.values())
            else self._closest_node_after_key(key_hash=key_hash)
        )

    def update(self, node_idx: int, tokens: int) -> None:
        """
        Update the number of tokens allocated to a node and update the ring.
        :param node_idx: index of the node to update
        :param tokens: number of nodes to allocate to the node.
        """
        self._update_node_tokens(node_idx=node_idx, tokens=tokens)
        self._update_hashes()

    def add_node(self, tokens: int) -> None:
        """
        Append a new node.
        :param tokens: number of tokens to give to the node.
        """
        self.nodes += 1
        self.node_tokens[self.nodes - 1] = tokens
        self._update_hashes()

    def remove_last_node(self) -> None:
        """
        Remove the very last node and update the ring.
        """
        if self.nodes == 1:
            raise ValueError("there's only one node left!")

        del self.node_tokens[self.nodes - 1]
        self.nodes -= 1
        self._update_hashes()

    def update_batch(self, node_tokens: Dict[int, int]) -> None:
        """
        Update tokens in batch.
        :param node_tokens: a dict where each key is a node index and the
        corresponding value is the number of desired tokens for that index.
        Nodes not in the set of keys are left unchanged.
        The ring is updated after these changes.
        """
        for node_idx, tokens in node_tokens.items():
            self._update_node_tokens(node_idx=node_idx, tokens=tokens)
        self._update_hashes()

    def _hash(self, key: str) -> int:
        return mmh3.hash(key, seed=self.seed)

    def _update_hashes(self) -> None:
        self.token_hashes = {
            (node_idx, token_idx): self._hash(key=f"{node_idx}-{token_idx}")
            for node_idx in range(self.nodes)
            for token_idx in range(self.node_tokens[node_idx])
        }

    def _update_node_tokens(self, node_idx: int, tokens: int) -> None:
        if tokens < 1 or not isinstance(tokens, int):
            raise ValueError("tokens must be int and >= 1.")
        self.node_tokens[node_idx] = tokens

    @property
    def node_with_smallest_hash(self) -> int:
        return min((h, node_idx) for (node_idx, _), h in self.token_hashes.items())[1]

    def _closest_node_after_key(self, key_hash: int) -> int:
        return min(
            (abs(h - key_hash), node_idx)
            for (node_idx, _), h in self.token_hashes.items()
            if key_hash < h
        )[1]

    @property
    def total_tokens(self) -> int:
        return sum(self.node_tokens.values())
