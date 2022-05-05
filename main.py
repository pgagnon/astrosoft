import click


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


@click.command()
@click.option("--source", help="Connection string to source database", required=True)
@click.option(
    "--destination", help="Connection string to destination database", required=True
)
@click.option(
    "--mode",
    type=click.Choice(["live", "offline"]),
    help="Perform a live or offline migration",
    default="live",
)
@click.option(
    "--model",
    multiple=True,
    default=[
        "Connection",
        "DagRun",
        "Pool",
        "RenderedTaskInstanceFields",
        "SensorInstance",
        "SlaMiss",
        "TaskFail",
        "TaskInstance",
        "TaskReschedule",
        "Trigger",
        "Variable",
    ],
)
def migrate_model(source: str, destination: str, mode: str, model: str):
    import airflow.models as airflow_models

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine

    src_engine = create_engine(source, echo=True)

    with src_engine.begin() as tx:
        res = tx.execute("SELECT 1").fetchone()
        print(f"'src_engine SELECT 1': {res}")

    Source_Session = sessionmaker(bind=src_engine)

    for model_ in model:
        print(f"Querying {model_}")

        src_session = Source_Session()
        objs = src_session.query(getattr(airflow_models, model_)).all()

        if mode == "live":
            print(f"Merging {model_}")
            dst_engine = create_engine(destination, echo=True)

            with dst_engine.begin() as tx:
                res = tx.execute("SELECT 1").fetchone()
                print(f"'src_engine SELECT 1': {res}")

            Dest_Session = sessionmaker(bind=dst_engine)
            dst_session = Dest_Session()

            for lst in chunks(objs, 100):
                for obj in lst:
                    dst_session.merge(obj)
                    dst_session.commit()

            print(f"Done merging {model_} ({len(objs)} objects)")
        elif mode == "offline":
            import pickle, pathlib

            print(f"Saving {model_}")
            with pathlib.Path(f"{model_}.pickle").open("w+b") as fd:
                fd.write(pickle.dumps(objs))
            print(f"Saved {model_} ({len(objs)})")


def run():
    migrate_model()


if __name__ == "__main__":
    run()
