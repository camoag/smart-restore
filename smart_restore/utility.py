import itertools


def chunk_list(iterable, size):
    """Split a single list into a list of size `size` tuples.

    ex: chunk_list([1, 2, 3, 4, 5], 2) == [(1, 2), (3, 4), (5,)]
    """
    it = iter(iterable)
    while group := list(itertools.islice(it, None, size)):
        yield group


def flatten_list(iterables):
    """Convert a list-of-lists to a flat list

    Example:

        flatten_list([["a", "b"], ["c"]]) == ["a", "b", "c"]

    """
    return list(itertools.chain.from_iterable(iterables))
