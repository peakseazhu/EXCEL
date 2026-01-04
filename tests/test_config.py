import pathlib

from src.config import load_config


def test_load_config():
    config = load_config(pathlib.Path('config/config.json'))
    assert config.project.name
    assert len(config.bi.charts) > 0