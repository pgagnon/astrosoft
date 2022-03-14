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
def migrate_model(source: str, destination: str, mode: str):
    from airflow.models import (
        Connection,
        DagRun,
        Pool,
        RenderedTaskInstanceFields,
        SensorInstance,
        SlaMiss,
        TaskFail,
        TaskInstance,
        TaskReschedule,
        Trigger,
        Variable,
    )

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine

    Source_Session = sessionmaker(bind=create_engine(source))

    for model in [
        Connection,
        DagRun,
        Pool,
        RenderedTaskInstanceFields,
        SensorInstance,
        SlaMiss,
        TaskFail,
        TaskInstance,
        TaskReschedule,
        Trigger,
        Variable,
    ]:
        print(f"Extracting {model}")

        src_session = Source_Session()
        objs = src_session.query(model).all()

        if mode == "live":
            Dest_Session = sessionmaker(bind=create_engine(destination))
            dst_session = Dest_Session()

            for obj in objs:
                dst_session.merge(obj)
                dst_session.commit()


def run():
    migrate_model()


if __name__ == "__main__":
    run()
