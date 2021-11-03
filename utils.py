import os
import shutil
import re
import json
import datetime as dt
from typing import Any, Generator, List, Collection, Dict, Union
from dataclasses import dataclass, field
from dateutil.parser import parse
import requests
import pandas as pd

EPOCH_DATE = "2020-01-21"


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


def delete_input(dataset_dir: str, keep_paths: List[str] = None) -> None:
    """deletes all files and folders in `{DATASET_DIR}/input` EXCEPT for any
    file names in `keep_paths`.

    Arguments:

        keep_paths: List[str]. List of subpaths in `{dataset_dir}/input` that
            you do NOT want deleted. They will be temporarily move to `{dataset_dir}`
            and then back into `{dataset_dir}/input` after everything else in
            `{dataset_dir}/input` has been deleted.

    Returns:

        None.
    """
    if not keep_paths:
        keep_paths = []
    inpath = os.path.join(dataset_dir, "input")
    # temporarily moves some files out of the input directory so that they
    # are not deleted.
    for path in keep_paths:
        if os.path.exists(os.path.join(inpath, path)):
            os.rename(os.path.join(inpath, path), os.path.join(inpath, "..", path))
    if os.path.exists(inpath):
        shutil.rmtree(inpath)
        os.makedirs(inpath)
    # moves the kept files back into the input directory.
    for path in keep_paths:
        if os.path.exists(os.path.join(inpath, "..", path)):
            os.rename(os.path.join(inpath, "..", path), os.path.join(inpath, path))


def delete_output(dataset_dir: str, keep_paths: List[str] = None) -> None:
    """deletes all files in `{dataset_dir}/output` EXCEPT for any file
    names in `keep_paths`.

    Arguments:

        keep_paths: List[str]. List of subpaths in `{dataset_dir}/output` that
            you do NOT want deleted. They will be temporarily move to `{dataset_dir}`
            and then back into `{dataset_dir}/output` after everything else in
            `{dataset_dir}/output` has been deleted.

    Returns:

        None.
    """
    if not keep_paths:
        keep_paths = []
    # temporarily moves some files out of the output directory so that they
    # are not deleted.
    outpath = os.path.join(dataset_dir, "output")
    for path in keep_paths:
        if os.path.exists(os.path.join(outpath, path)):
            os.rename(os.path.join(outpath, path), os.path.join(outpath, "..", path))
    # deletes all remaining output files
    if os.path.exists(outpath):
        shutil.rmtree(outpath)
        os.makedirs(outpath)
    # moves the exception files back into the output directory.
    for path in keep_paths:
        if os.path.exists(os.path.join(outpath, "..", path)):
            os.rename(os.path.join(outpath, "..", path), os.path.join(outpath, path))


def get_distinct_entities(dataset_dir: str) -> List[str]:
    """retrieves a list of all distinct entities that contain at least
    on non-null data point that have been saved to disk in the
    `{dataset_dir}/output/datapoints` folder.

    Returns:

        entities: List[str]. List of distinct entity names.
    """
    path = os.path.join(dataset_dir, "output", "datapoints")
    fnames = [fname for fname in os.listdir(path) if fname.endswith(".csv")]
    entities = set({})
    for fname in fnames:
        df_temp = pd.read_csv(os.path.join(path, fname))
        entities.update(df_temp["country"].unique().tolist())

    entity_list = sorted(entities)
    assert pd.notnull(entity_list).all(), (
        "All entities should be non-null. Something went wrong during creation "
        f"of the `datapoints_{id}.csv` files in {path}."
    )
    return entity_list


def get_owid_variable(
    variable_id: Union[int, str], to_frame: bool = False
) -> Union[pd.DataFrame, dict]:
    res = requests.get(
        f"https://ourworldindata.org/grapher/data/variables/{variable_id}.json"
    )
    assert res.ok
    result = json.loads(res.content)
    if to_frame:
        result = owid_variables_to_frame(result)
    return result


def get_owid_variable_source(variable_id: Union[int, str]) -> dict:
    res = requests.get(
        f"https://ourworldindata.org/grapher/data/variables/{variable_id}.json"
    )
    assert res.ok
    result = json.loads(res.content)["variables"][str(variable_id)]
    for k in ["years", "entities", "values"]:
        del result[k]
    return result


def owid_variables_to_frame(owid_json: Dict[str, dict]) -> pd.DataFrame:
    entity_map = {int(k): v["name"] for k, v in owid_json["entityKey"].items()}
    frames = []
    for variable in owid_json["variables"].values():
        df = pd.DataFrame(
            {
                "year": variable["years"],
                "entity": [entity_map[e] for e in variable["entities"]],
                "variable": variable["name"],
                "value": variable["values"],
            }
        )
        if variable.get("display", {}).get("yearIsDay"):
            zero_day = parse(variable["display"].get("zeroDay", EPOCH_DATE)).date()
            df["date"] = df.pop("year").apply(lambda y: zero_day + dt.timedelta(days=y))
            df = df[["date", "entity", "variable", "value"]]

        frames.append(df)

    return pd.concat(frames)
