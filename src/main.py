from src.config import Env, RuntimeConfig
from src.dune_to_local.main import dune_to_postgres


def main() -> None:
    env = Env.load()
    config = RuntimeConfig.load_from_toml("config.toml")
    # TODO: Async job execution https://github.com/bh2smith/dune-sync/issues/20
    for job in config.jobs:
        dune_to_postgres(env, job)


if __name__ == "__main__":
    main()
