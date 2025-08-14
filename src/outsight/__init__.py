from .run.core import Outsight
from .run.fixtures import Fixture

outsight = Outsight(entry_point_fixtures=True)
give = outsight.give
send = outsight.send

__all__ = [
    "Fixture",
    "Outsight",
    "outsight",
    "give",
    "send",
]
