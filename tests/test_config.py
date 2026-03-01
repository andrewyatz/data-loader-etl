import pytest
from pydantic import TypeAdapter

from etl.config import validate_config, validate_dataset
from etl.main import load_data
from etl.models import Config, Dataset

_has_yaml = True
try:
    import yaml  # noqa: F401
except ImportError:
    _has_yaml = False


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


def test_load_json5_config(config_dir):
    """JSON5 config parses identically to JSON."""
    json_config = load_data(config_dir / "config.json")
    json5_config = load_data(config_dir / "config.json5")
    assert json_config == json5_config


def test_load_json5_data(config_dir):
    """JSON5 data file parses identically to JSON."""
    json_data = load_data(config_dir / "data.json")
    json5_data = load_data(config_dir / "data.json5")
    assert json_data == json5_data


@pytest.mark.skipif(not _has_yaml, reason="PyYAML not installed")
def test_load_yaml_config(config_dir):
    """YAML config parses identically to JSON."""
    json_config = load_data(config_dir / "config.json")
    yaml_config = load_data(config_dir / "config.yaml")
    assert json_config == yaml_config


@pytest.mark.skipif(not _has_yaml, reason="PyYAML not installed")
def test_load_yaml_data(config_dir):
    """YAML data file parses identically to JSON."""
    json_data = load_data(config_dir / "data.json")
    yaml_data = load_data(config_dir / "data.yaml")
    assert json_data == yaml_data
