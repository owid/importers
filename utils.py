import os
import re
import json
import requests
from typing import Any, Generator, List, Collection
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()
SITE_HOST = os.getenv("SITE_HOST")
SITE_SESSION_ID = os.getenv("SITE_SESSION_ID")


def write_file(file_path, content):
    with open(file_path, "w") as f:
        f.write(content)


def batchify(
    iterable: Collection[Any], batch_size: int = 1000
) -> Generator[Collection[Any], None, None]:
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
        yield iterable[i : min(i + batch_size, l)]  # type: ignore


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
    min: int  # type: ignore
    _min: int = field(init=False, repr=False)
    max: int  # type: ignore
    _max: int = field(init=False, repr=False)

    @property  # type: ignore
    def min(self) -> int:
        return self._min

    @min.setter
    def min(self, x: int) -> None:
        self._min = int(x)

    @property  # type: ignore
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


def assert_admin_api_connection() -> None:
    """raises an AssertionError if unable to successfully connect to the admin API."""
    res = False
    try:
        charts = json.loads(
            requests.get(
                f"{SITE_HOST}/admin/api/charts.json?limit=1",
                cookies={"sessionid": SITE_SESSION_ID},
            ).content
        )
        res = len(charts["charts"]) > 0
    except Exception:
        res = False
    assert res, (
        "Failed to connect to admin API, have you set SITE_HOST and "
        "SITE_SESSION_ID correctly in .env?"
    )
