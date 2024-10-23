from pathlib import Path

from src.config import RuntimeConfig
from src.pipelines.dune_to_pg import create_pipeline


def main() -> None:
    root_path = Path(__file__).parent.parent
    config_file_path = root_path / "config.toml"
    config = RuntimeConfig.load_from_toml(config_file_path.absolute())

    pipeline = create_pipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
