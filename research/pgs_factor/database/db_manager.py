"""Disabled compatibility stub for the retired ``pgs_factors`` writer."""


class PGSFactorDBManager:
    """Fail closed so research code cannot mutate compatibility views."""

    def __init__(self, *_args, **_kwargs):
        raise RuntimeError(
            "PGSFactorDBManager已停用；生产P/G写入必须通过"
            "alphahome.factors.FactorCoordinator并写入factors schema"
        )


__all__ = ["PGSFactorDBManager"]
