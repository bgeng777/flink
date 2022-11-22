from abc import ABC, abstractmethod


class Condition(ABC):
    """
    The base class for all user-defined functions.
    """
    @abstractmethod
    def filter(self, value):
        """
        The filter method. Takes an element from the input data and return if it fulfills the
        condition.

        :param value: The input value.
        :return: true/false.
        """
        pass
