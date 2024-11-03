# pragma: no cover
from pathlib import Path

import src as root
from src.config import Env, RuntimeConfig
from src.recipe import dune_to_postgres, postgres_to_dune


def main() -> None:
    env = Env.load()
    root_path = Path(root.__path__[0])
    config = RuntimeConfig.load_from_toml(
        str((root_path.parent / "config.toml").absolute())
    )
    # TODO: Async job execution https://github.com/bh2smith/dune-sync/issues/20
    for d2l_job in config.dune_to_local_jobs:
        dune_to_postgres(env, d2l_job)
    for l2d_job in config.local_to_dune_jobs:
        postgres_to_dune(env, l2d_job)


if __name__ == "__main__":
    main()
