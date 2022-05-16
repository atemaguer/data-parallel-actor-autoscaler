import unittest

import pandas as pd

from dpa_autoscaler.allocation import ConsistentHashing


class TestConsistentHashing(unittest.TestCase):
    @staticmethod
    def _stats(ch):
        # proportion of keys associated with each node
        print(
            pd.Series(
                [ch.key_lookup(key=str(idx)) for idx in range(10_000)]
            ).value_counts(normalize=True)
        )

    def test_ch(self):
        ch = ConsistentHashing(nodes=5)
        # assert ch.total_tokens == ch.nodes == 3
        self._stats(ch)
        ch.halve_tokens_for_node(node_idx=2)
        self._stats(ch)
        ch.halve_tokens_for_node(node_idx=2)
        self._stats(ch)
        ch.halve_tokens_for_node(node_idx=2)
        self._stats(ch)
