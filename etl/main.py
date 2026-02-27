import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]


from pydantic import TypeAdapter

from etl.cli import get_cli_args
from etl.config import validate_config, validate_dataset
from etl.database import Database, DatabaseConfig
from etl.dataset import DatasetsProcessor
from etl.models import Config, Dataset
from etl.transform import Transform
from etl.views import ViewsProcessor

CONFIG_SCHEMA_FILE = "config.json"
DATA_SCHEMA_FILE = "dataset.json"


def create_release(release: str, overwrite: bool = False) -> Path:
    release_path = Path(release)
    if release_path.exists():
        if overwrite:
            shutil.rmtree(release_path)
        else:
            raise ValueError(f"ERROR! Release {release_path!r} already exists!")
    release_path.mkdir(parents=True, exist_ok=False)
    return release_path


def load_data(path: str | Path) -> Any:
    path_obj = Path(path)
    with open(path_obj, "r") as fh:
        if path_obj.suffix in (".yml", ".yaml"):
            if yaml is None:
                raise ImportError("PyYAML is required to load YAML files")
            return yaml.safe_load(fh)
        elif path_obj.suffix == ".json":
            return json.load(fh)
        else:
            raise ValueError("Unsupported file format. Use .json or .yml/.yaml")


def validate_configs(config: Any, data: Any, schemas: str) -> None:
    """
    Validate config and dataset
    """
    config_schema = _load_schema(schemas, CONFIG_SCHEMA_FILE, "config")
    data_schema = _load_schema(schemas, DATA_SCHEMA_FILE, "data")
    # validate config
    validate_config(config, config_schema)
    # validate dataset
    validate_dataset(data, data_schema)


def _load_schema(schemas: str, schema_file_name: str, type: str) -> dict[str, Any]:
    schema_file = Path(schemas) / schema_file_name
    if not schema_file.exists():
        raise FileNotFoundError(
            f"Unable to find {type} schema. Expected: {schema_file}"
        )
    with open(schema_file, "rt") as fh:
        result: dict[str, Any] = json.load(fh)
        return result


def run_etl() -> None:
    print("Loading configs")
    cli = get_cli_args().parse_args(sys.argv[1:])
    if cli.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    config_dict = load_data(cli.config)
    data_dict = load_data(cli.data)

    print("Stage 0: Creating release directory")
    release_path = create_release(cli.release, overwrite=cli.force)

    print("Stage 1: Validating ETL configurations")
    validate_configs(config_dict, data_dict, cli.schema)
    datasets = TypeAdapter(list[Dataset]).validate_python(data_dict)
    configs = Config.model_validate(config_dict)

    print("Stage 2: Running first pass ETL")
    Transform(datasets=datasets, release_path=release_path).run()

    print("Stage 3: Creating dataset configuration files")
    DatasetsProcessor(
        datasets=datasets,
        views=configs.views,
        columns=configs.columns,
        release_path=release_path,
    ).run()

    print("Stage 4: Preconfiguring filter values")
    ViewsProcessor(
        views=configs.views,
        filters=configs.filters,
        datasets=datasets,
        columns=configs.columns,
        release_path=release_path,
    ).run()

    print("Stage 5: Creating final DuckDB configurations")
    with DatabaseConfig(release_path, cli.release, configs.views) as database:
        database.run()

    print("Stage 6: Copying data to DuckDB")
    with Database(release_path, cli.release) as database:
        database.run()

    print("Success!")
