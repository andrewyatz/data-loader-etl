from pydantic import TypeAdapter

from etl.config import validate_config, validate_dataset
from etl.main import load_data
from etl.models import Config, Dataset


def test_data_config(schema_dir, data_json):
    schema = load_data(schema_dir / "dataset.json")
    dataset = load_data(data_json)
    validate_dataset(datasets=dataset, schema=schema)


def test_view_config(config_dir, schema_dir):
    schema = load_data(schema_dir / "config.json")
    config = load_data(config_dir / "config.json")
    validate_config(config=config, schema=schema)


def test_models(config_dir):
    dataset = load_data(config_dir / "data.json")
    config = load_data(config_dir / "config.json")
    config_objs = Config.model_validate(config)
    assert config_objs
    datasets = TypeAdapter(list[Dataset]).validate_python(dataset)
    assert datasets
