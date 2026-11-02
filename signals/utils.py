"""
Signal Utils v11.0
"""
from models import Direction


def dir_from(value: float, threshold: float = 0.1) -> Direction:
    if value > threshold:
        return Direction.LONG
    if value < -threshold:
        return Direction.SHORT
    return Direction.NEUTRAL