import re
from typing import Iterable, Any, Generator, List
from dataclasses import dataclass, astuple, field


def write_file(file_path, content):
    with open(file_path, "w") as f:
        f.write(content)


def batchify(
    iterable: Iterable[Any], batch_size: int = 1000
) -> Generator[Iterable[Any], None, None]:
    """yields batches of an iterable in batches of size n.

    Divides an iterable into batches of size n, yielding one batch at a time.

    Arguments:

        iterable: Iterable[Any]. Iterable (list, np.array, ...) to be split into
            batches. Must be an iterable that can be sliced (e.g. `iterable[0:5]`)
            and must have a length.

        batch_size: int = 1000. Size of each batch.

    Yields:

        Iterable[Any]. Batch from iterable.

    Example::

        >>> data = list(range(0,11))
        >>> for batch in batchify(data, 3):
        >>>     print(batch)
        [0, 1, 2]
        [3, 4, 5]
        [6, 7, 8]
        [9, 10]
    """
    assert isinstance(
        batch_size, int
    ), f"batch_size must be int, but received {type(batch_size)}"
    assert (
        batch_size > 0
    ), f"batch_size must be greater than 0, but received {batch_size}"
    l = len(iterable)
    for i in range(0, l, batch_size):
        yield iterable[i : min(i + batch_size, l)]


def camel_case2snake_case(s) -> str:
    """converts a camel-cased string to snake case."""
    s2 = re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()
    return s2


def snake_case2camel_case(s) -> str:
    """converts a snake-cased string to camel case."""
    s2 = "".join(w.title() for w in s.split("_"))
    return s2


def import_from(module: str, name: str) -> Any:
    module = __import__(re.sub("/", ".", module), fromlist=[name])
    return getattr(module, name)


@dataclass
class IntRange:
    min: int
    _min: int = field(init=False, repr=False)
    max: int
    _max: int = field(init=False, repr=False)

    @property
    def min(self) -> int:
        return self._min

    @min.setter
    def min(self, x: int) -> None:
        self._min = int(x)

    @property
    def max(self) -> int:
        return self._max

    @max.setter
    def max(self, x: int) -> None:
        self._max = int(x)

    @staticmethod
    def from_values(xs: List[int]):
        return IntRange(min(xs), max(xs))

    def to_values(self):
        return [self.min, self.max]
