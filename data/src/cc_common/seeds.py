"""Deterministic RNG helpers. Owner: Stream 0 (lead).

Each notebook derives its own stream from (base seed, notebook id) so re-running one notebook
never shifts another's data. Partitioned generation uses child seeds per partition index.
"""
from __future__ import annotations

import numpy as np

NOTEBOOK_IDS = {"02": 2, "03": 3, "04": 4, "05": 5, "11": 11, "12": 12, "13": 13, "15": 15, "17": 17, "18": 18}


def rng(base_seed: int, notebook: str) -> np.random.Generator:
    return np.random.Generator(np.random.PCG64(np.random.SeedSequence([base_seed, NOTEBOOK_IDS[notebook]])))


def partition_rng(base_seed: int, notebook: str, partition_index: int) -> np.random.Generator:
    """Child generator for one mapInPandas partition; stable for a given partition index."""
    seq = np.random.SeedSequence([base_seed, NOTEBOOK_IDS[notebook], partition_index])
    return np.random.Generator(np.random.PCG64(seq))
