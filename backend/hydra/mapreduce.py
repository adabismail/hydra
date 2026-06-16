"""The actual word-count map/reduce logic plus the partitioning function.

This module is intentionally tiny and pure (no networking) so the data
transformation is trivial to read and test in isolation.
"""
from __future__ import annotations

import re
import zlib
from collections import Counter
from typing import Dict, Iterable, List, Tuple

_WORD_RE = re.compile(r"[a-z0-9']+")


def tokenize(text: str) -> List[str]:
    """Lowercase + split a blob of text into word tokens."""
    return _WORD_RE.findall(text.lower())


def partition_for(key: str, num_reduce: int) -> int:
    """Deterministic partitioner: hash(key) % R.

    We use CRC32 instead of Python's built-in ``hash`` because the built-in is
    randomised per-process (PYTHONHASHSEED), which would make different worker
    processes disagree on which reducer owns a key. CRC32 is stable everywhere.
    """
    return zlib.crc32(key.encode("utf-8")) % num_reduce


def map_chunk(text: str) -> Counter:
    """Map + combine: count word occurrences within a single chunk.

    Pre-aggregating here is the classic 'combiner' optimisation — it shrinks the
    intermediate data each map worker has to write and serve during shuffle.
    """
    return Counter(tokenize(text))


def partition_counts(counts: Counter, num_reduce: int) -> List[List[Tuple[str, int]]]:
    """Split combined counts into one bucket per reducer using the partitioner."""
    buckets: List[List[Tuple[str, int]]] = [[] for _ in range(num_reduce)]
    for word, count in counts.items():
        buckets[partition_for(word, num_reduce)].append((word, count))
    return buckets


def reduce_pairs(pairs: Iterable[Tuple[str, int]]) -> List[Tuple[str, int]]:
    """Sum counts per key and return them sorted by key."""
    totals: Dict[str, int] = {}
    for word, count in pairs:
        totals[word] = totals.get(word, 0) + count
    return sorted(totals.items())


# --- partition file serialization (one "word\tcount" per line) ------------
def dump_pairs(pairs: Iterable[Tuple[str, int]]) -> str:
    return "".join(f"{w}\t{c}\n" for w, c in pairs)


def load_pairs(blob: str) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for line in blob.splitlines():
        if not line.strip():
            continue
        word, count = line.rsplit("\t", 1)
        out.append((word, int(count)))
    return out
