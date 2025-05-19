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
