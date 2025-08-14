from .run.core import Outsight

outsight = Outsight(entry_point_fixtures=True)
give = outsight.give
send = outsight.send

__all__ = [
    "Outsight",
    "outsight",
    "give",
    "send",
]
