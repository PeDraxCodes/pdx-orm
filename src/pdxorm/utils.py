from typing import Any, Callable


def get_first_or_element[T](elem: list[T] | T) -> T:
    """
    Returns the first element of a list or the element itself if it is not a list.
    Raises ValueError if an empty list is provided.
    """
    if isinstance(elem, list):
        if len(elem) == 0:
            raise ValueError("List is empty")
        return elem[0]
    return elem


def get_as_tuple[T](value: T) -> tuple[T, ...]:
    """
    Returns the value as a tuple.
    If the value is None, returns an empty tuple.
    If the value is a list, returns a tuple of the elements in the list.
    Otherwise, returns a tuple with the value itself.
    """
    if value is None:
        return ()
    if isinstance(value, list):
        return tuple(value)
    return (value,)


def get_elements_as_list[T](data: T | list[T], consumer: Callable[[T], Any] = lambda x: x) -> list[Any]:
    """
    Applies the consumer function to the data and returns the result as a list.
    If the data is a list, applies the consumer to each element and returns a list of results.
    If the data is a single element, applies the consumer to it and returns a list with the result.
    """
    if isinstance(data, list):
        return [consumer(d) for d in data]
    return [consumer(data)]
