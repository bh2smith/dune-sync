from pathlib import Path
from unittest.mock import patch

from src import root_path
from src.args import Args


def test_args_default_values():
    """Test Args parser with default values (no command line arguments)."""
    with patch("sys.argv", ["script.py"]):
        args = Args.from_command_line()

        assert args.config == root_path.parent / "config.yaml"
        assert args.jobs is None


def test_args_custom_config():
    """Test Args parser with custom config path."""
    test_config = Path("/custom/path/config.yaml")
    with patch("sys.argv", ["script.py", "--config", str(test_config)]):
        args = Args.from_command_line()

        assert args.config == test_config
        assert args.jobs is None


def test_args_with_jobs():
    """Test Args parser with specific jobs."""
    with patch("sys.argv", ["script.py", "--jobs", "job1", "job2"]):
        args = Args.from_command_line()

        assert args.config == root_path.parent / "config.yaml"
        assert args.jobs == ["job1", "job2"]


def test_args_with_empty_jobs():
    """Test Args parser with empty jobs list."""
    with patch("sys.argv", ["script.py", "--jobs"]):
        args = Args.from_command_line()

        assert args.config == root_path.parent / "config.yaml"
        assert args.jobs is None


def test_args_with_all_options():
    """Test Args parser with both config and jobs specified."""
    test_config = Path("/custom/path/config.yaml")
    with patch(
        "sys.argv",
        ["script.py", "--config", str(test_config), "--jobs", "job1", "job2"],
    ):
        args = Args.from_command_line()

        assert args.config == test_config
        assert args.jobs == ["job1", "job2"]
