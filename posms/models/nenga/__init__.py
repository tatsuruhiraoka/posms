# posms/models/nenga/__init__.py
from __future__ import annotations
from typing import Any

def train_nenga_assembly(*args: Any, **kwargs: Any):
    from .assembly import train as _train
    return _train(*args, **kwargs)

def train_nenga_delivery(*args: Any, **kwargs: Any):
    from .delivery import train as _train
    return _train(*args, **kwargs)

__all__ = ["train_nenga_assembly", "train_nenga_delivery"]