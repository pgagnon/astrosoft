import click


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

    Source_Session = sessionmaker(bind=create_engine(source))

    for model_ in model:
        print(f"Querying {model_}")

        src_session = Source_Session()
        objs = src_session.query(getattr(airflow_models, model_)).all()

        if mode == "live":
            print(f"Merging {model_}")
            Dest_Session = sessionmaker(bind=create_engine(destination))
            dst_session = Dest_Session()

            for obj in objs:
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
