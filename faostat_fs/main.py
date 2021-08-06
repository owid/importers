"""
This script downloads selected datasets from FAO and uploads them into rhe Grapher DB.

Run it as 

```
python -m faostat_2021.main <walden-local-folder>
```

where `walden-local-folder` is the local fodler containing walden project (clone it from
https://github.com/owid/walden).
"""

import tempfile

from faostat_fs import download
from faostat_fs._parsers import get_parser_args


def main():
    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = download.main(tmp_dir)


if __name__ == "__main__":
    args = get_parser_args()
    main(args.catalog_folder)
