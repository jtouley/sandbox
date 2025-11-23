"""
Lean tests for configuration validation.
Tests the Pydantic models and config loading logic.
"""

from pathlib import Path

import pytest
from day05_configdriven.src.config_loader import (
    ChaosConfig,
    Config,
    GenerationConfig,
    SourceConfig,
    StrategiesConfig,
    interpolate_env_vars,
    load_config,
)


class TestChaosConfig:
    """Test chaos configuration validation"""

    def test_fixed_mode_with_floats(self):
        """Test fixed mode with single float values"""
        config = ChaosConfig(
            mode="fixed",
            dup_prob=0.1,
            slow_prob=0.15,
        )

        sampled = config.sample()
        assert sampled["dup_prob"] == 0.1
        assert sampled["slow_prob"] == 0.15

    def test_random_mode_with_ranges(self):
        """Test random mode samples within ranges"""
        config = ChaosConfig(
            mode="random",
            dup_prob=(0.05, 0.15),
            slow_prob=(0.05, 0.15),
        )

        sampled = config.sample()
        assert 0.05 <= sampled["dup_prob"] <= 0.15
        assert 0.05 <= sampled["slow_prob"] <= 0.15


class TestGenerationConfig:
    """Test generation configuration"""

    def test_scale_presets(self):
        """Test scale preset resolution"""
        config = GenerationConfig(scale="medium")
        assert config.scale == 10_000

        config = GenerationConfig(scale="large")
        assert config.scale == 1_000_000

    def test_custom_scale(self):
        """Test custom integer scale"""
        config = GenerationConfig(scale=50000)
        assert config.scale == 50000


class TestSourceConfig:
    """Test source configuration"""

    def test_valid_source_types(self):
        """Test all valid source types"""
        for source_type in ["pubsub", "polars_synthetic", "file", "dlt"]:
            config = SourceConfig(type=source_type)
            assert config.type == source_type

    def test_invalid_source_type(self):
        """Test invalid source type raises error"""
        with pytest.raises(ValueError):
            SourceConfig(type="invalid")


class TestStrategiesConfig:
    """Test strategies configuration"""

    def test_default_strategies(self):
        """Test default strategy list"""
        config = StrategiesConfig()
        assert "bronze_append" in config.enabled
        assert "polars_merge" in config.enabled

    def test_strategy_configs(self):
        """Test individual strategy configs"""
        config = StrategiesConfig()

        # Bronze append
        assert config.bronze_append.partition_by == ["partition_date"]
        assert config.bronze_append.hash_algorithm == "sha256"

        # Polars merge
        assert config.polars_merge.dedup_key == "message_id"
        assert config.polars_merge.keep == "last"


class TestConfig:
    """Test complete configuration"""

    def test_valid_config(self):
        """Test valid complete config"""
        config = Config()
        assert config.pipeline.name == "configdriven-pipeline"
        assert config.source.type == "pubsub"
        assert config.generation.mode == "chaos"

    def test_invalid_strategy_name(self):
        """Test invalid strategy name raises error"""
        with pytest.raises(ValueError):
            Config(strategies=StrategiesConfig(enabled=["invalid_strategy"]))


class TestEnvInterpolation:
    """Test environment variable interpolation"""

    def test_interpolate_simple_string(self, monkeypatch):
        """Test simple env var interpolation"""
        monkeypatch.setenv("TEST_VAR", "/path/to/data")

        result = interpolate_env_vars("${TEST_VAR}")
        assert result == "/path/to/data"

    def test_interpolate_dict(self, monkeypatch):
        """Test env var interpolation in dicts"""
        monkeypatch.setenv("RAW_PATH", "/data/raw")
        monkeypatch.setenv("DELTA_PATH", "/data/delta")

        data = {
            "raw": "${RAW_PATH}",
            "delta": "${DELTA_PATH}",
            "static": "value",
        }

        result = interpolate_env_vars(data)
        assert result["raw"] == "/data/raw"
        assert result["delta"] == "/data/delta"
        assert result["static"] == "value"

    def test_interpolate_nested(self, monkeypatch):
        """Test env var interpolation in nested structures"""
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        data = {"config": {"logging": {"level": "${LOG_LEVEL}"}}}

        result = interpolate_env_vars(data)
        assert result["config"]["logging"]["level"] == "DEBUG"

    def test_missing_env_var(self):
        """Test missing env var returns original string"""
        result = interpolate_env_vars("${NONEXISTENT_VAR}")
        assert result == "${NONEXISTENT_VAR}"


class TestConfigLoader:
    """Test config loading from file"""

    def test_load_default_config(self):
        """Test loading the default pipeline.yaml"""
        config_path = Path("labs/day05_configdriven/config/pipeline.yaml")

        if not config_path.exists():
            pytest.skip("Default config file not found")

        config = load_config(str(config_path))
        assert isinstance(config, Config)
        assert config.pipeline.name == "configdriven-pipeline"

    def test_load_nonexistent_config(self):
        """Test loading nonexistent config raises error"""
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/config.yaml")
