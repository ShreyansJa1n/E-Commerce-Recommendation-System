"""Re-export: the generator lives in the package so `make demo` can use it."""

from recsys.ingest.synth import SynthStats, generate, ms

__all__ = ["SynthStats", "generate", "ms"]
