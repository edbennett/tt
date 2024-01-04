from contextlib import contextmanager
from datetime import date as dt_date
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    and_,
    create_engine,
    or_,
    select,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Stint(Base):
    __tablename__ = "stint"
    id = Column(Integer, primary_key=True)  # noqa: A003
    start = Column(DateTime, nullable=False)
    end = Column(DateTime, nullable=False)
    project_id = Column(Integer, ForeignKey("project.id"), nullable=False)
    project = relationship("Project")
    description = Column(String, nullable=False)
    comment = Column(String, nullable=True)


class Project(Base):
    __tablename__ = "project"
    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)


@contextmanager
def session_scope(db_location):
    engine = create_engine(db_location)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = Session(expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def combine_date_time(date, time):
    return datetime.combine(
        dt_date.fromisoformat(date),
        datetime.strptime(time, "%H:%M:%S").time(),  # noqa: DTZ007
    ).astimezone()


def today():
    return datetime.now().astimezone().date()


def get_project(session, project_name):
    project = session.scalars(
        select(Project).where(Project.name == project_name)
    ).one_or_none()
    if not project:
        message = f"No such project {project_name}"
        raise ValueError(message)

    return project


@click.group()
@click.option(
    "--db_location",
    default=Path.home() / ".local/state/tt/tt.sqlite",
    type=click.Path(path_type=Path),
)
@click.pass_context
def cli(ctx, db_location):
    ctx.ensure_object(dict)
    ctx.obj["db_location"] = "sqlite:///" + str(db_location.absolute())


@cli.command()
@click.option("--date", default=today().isoformat())
@click.option("--start_time", default=None)
@click.option("--end_time", default="now")
@click.option("--duration", default=None, type=float)
@click.option("--project_name", required=True)
@click.option("--new_project", is_flag=True, default=False)
@click.option("--comment", default=None)
@click.argument("description", nargs=-1, required=True)
@click.pass_context
def add(  # noqa: PLR0913
    ctx,
    date,
    start_time,
    end_time,
    duration,
    project_name,
    new_project,
    comment,
    description,
):
    if end_time == "now":
        if date != today().isoformat():
            message = "If you specify a date, you have to specify a time"
            raise ValueError(message)
        end_dt = datetime.now(timezone.utc)
    else:
        end_dt = combine_date_time(date, end_time)

    if duration is not None and start_time is None:
        start_dt = end_dt - timedelta(minutes=duration)
    elif start_time is not None and duration is None:
        start_dt = combine_date_time(date, start_time)
    else:
        message = "One of --start_time and --duration must be specified"
        raise ValueError(message)

    if new_project:
        with session_scope(ctx.obj["db_location"]) as session:
            project = Project(name=project_name)
            session.add(project)

    with session_scope(ctx.obj["db_location"]) as session:
        project = get_project(session, project_name)
        stint = Stint(
            start=start_dt.astimezone(timezone.utc),
            end=end_dt.astimezone(timezone.utc),
            project_id=project.id,
            description=" ".join(description),
            comment=comment,
        )
        session.add(stint)


def date_limits(date):
    earliest = combine_date_time(date, "0:00:00")
    latest = (earliest + timedelta(days=1)).astimezone(timezone.utc)
    return earliest.astimezone(timezone.utc), latest


def stints_by_date(session, date):
    earliest, latest = date_limits(date)
    return session.scalars(
        select(Stint).where(
            or_(
                and_(Stint.start > earliest, Stint.start < latest),
                and_(Stint.end > earliest, Stint.end < latest),
            )
        )
    ).all()


def hours_by_date(session, date):
    earliest, latest = date_limits(date)
    stints = stints_by_date(session, date)
    durations = [
        min(latest, stint.end.astimezone(timezone.utc))
        - max(earliest, stint.start.astimezone(timezone.utc))
        for stint in stints
    ]
    return sum(durations, start=timedelta(0)).total_seconds() / 3600


@cli.command()
@click.option("--date", default=today().isoformat())
@click.pass_context
def hours(ctx, date):
    with session_scope(ctx.obj["db_location"]) as session:
        print(hours_by_date(session, date))


@cli.command()
@click.option("--date", default=today().isoformat())
@click.pass_context
def liststints(ctx, date):
    with session_scope(ctx.obj["db_location"]) as session:
        print(f"Stints for {date}")
        print("Start End   Dur. Project              Description")
        print("===== ===== ==== ==================== ============--..")
        for stint in sorted(
            stints_by_date(session, date), key=lambda stint: stint.start
        ):
            print(
                f"{stint.start.strftime('%H:%M')} "
                f"{stint.end.strftime('%H:%M')} "
                f"{int((stint.end - stint.start).total_seconds() / 60):4} "
                f"{stint.project.name:20} "
                f"{stint.description}"
            )


if __name__ == "__main__":
    cli(obj={})
