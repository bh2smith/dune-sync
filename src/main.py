from src.config import Env, RuntimeConfig
from src.dune_to_local.postgres import dune_to_postgres
from src.local_to_dune.postgres import postgres_to_dune


def main() -> None:
    env = Env.load()
    config = RuntimeConfig.load_from_toml("config.toml")
    # TODO: Async job execution https://github.com/bh2smith/dune-sync/issues/20
    for d2l_job in config.dune_to_local_jobs:
        dune_to_postgres(env, d2l_job)
    for l2d_job in config.local_to_dune_jobs:
        postgres_to_dune(env, l2d_job)


if __name__ == "__main__":
    main()
