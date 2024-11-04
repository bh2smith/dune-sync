# pragma: no cover
from pathlib import Path

import src as root
from src.config import Env, RuntimeConfig
from src.logger import log

env = Env.load()


def main() -> None:
    root_path = Path(root.__path__[0])
    config = RuntimeConfig.load_from_yaml(
        str((root_path.parent / "config.yaml").absolute())
    )
    # TODO: Async job execution https://github.com/bh2smith/dune-sync/issues/20
    for job in config.jobs:
        job.run()
        log.info("Job completed: %s", job)


if __name__ == "__main__":
    main()
