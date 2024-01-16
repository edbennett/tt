from contextlib import contextmanager
from datetime import date as dt_date
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep

import click
import CoreLocation
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    TypeDecorator,
    and_,
    create_engine,
    desc,
    or_,
    select,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class TimeStamp(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime, dialect):  # noqa: ARG002
        if value.tzinfo is None:
            message = "Can't put naive datetimes into the database"
            raise ValueError(message)

        return value.astimezone(timezone.utc)

    def process_result_value(self, value, dialect):  # noqa: ARG002
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)


class Stint(Base):
    __tablename__ = "stint"
    id = Column(Integer, primary_key=True)
    start = Column(TimeStamp, nullable=False)
    end = Column(TimeStamp, nullable=False)
    project_id = Column(Integer, ForeignKey("project.id"), nullable=False)
    project = relationship("Project")
    description = Column(String, nullable=False)
    comment = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)


class Project(Base):
    __tablename__ = "project"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)


class Mark(Base):
    __tablename__ = "mark"
    id = Column(Integer, primary_key=True)
    when = Column(TimeStamp, nullable=False)


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
    try:
        dt_time = datetime.strptime(time, "%H:%M:%S")  # noqa: DTZ007
    except ValueError:
        dt_time = datetime.strptime(time, "%H:%M")  # noqa: DTZ007

    return datetime.combine(
        dt_date.fromisoformat(date),
        dt_time.time(),
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


def get_latest_stint(session):
    return session.scalars(
        select(Stint)
        .where(Stint.end <= datetime.now(timezone.utc))
        .order_by(desc(Stint.end))
    ).first()


def get_latest_mark(session):
    return session.scalars(
        select(Mark)
        .where(Mark.when > get_latest_stint(session).end)
        .order_by(desc(Mark.when))
    ).first()


def get_location():
    time_for_location_manager_to_warm_up = 0.01
    location_manager = CoreLocation.CLLocationManager.new()
    location_manager.requestWhenInUseAuthorization()
    sleep(time_for_location_manager_to_warm_up)
    location = location_manager.location()
    if not location:
        return None, None
    coordinate = location.coordinate()
    return coordinate.latitude, coordinate.longitude


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
@click.option("--start_time", "--start", default=None)
@click.option("--end_time", "--end", default="now")
@click.option("--duration", default=None, type=float)
@click.option("--since_last", is_flag=True, default=False)
@click.option("--since_mark", is_flag=False, flag_value=True, default=False)
@click.option("--project_name", "--project", required=True)
@click.option("--new_project", is_flag=True, default=False)
@click.option("--comment", default=None)
@click.argument("description", nargs=-1, required=True)
@click.pass_context
def add(  # noqa: PLR0913, C901
    ctx,
    date,
    start_time,
    end_time,
    duration,
    since_last,
    since_mark,
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

    if (
        sum([since_last, (duration is not None), (start_time is not None), since_mark])
        != 1
    ):
        message = (
            "One of --since_last, --since_mark, --start_time, and --duration "
            "must be specified"
        )
        raise ValueError(message)

    if since_last:
        with session_scope(ctx.obj["db_location"]) as session:
            start_dt = get_latest_stint(session).end
    elif duration is not None and start_time is None:
        start_dt = end_dt - timedelta(minutes=duration)
    elif start_time is not None and duration is None:
        start_dt = combine_date_time(date, start_time)
    else:
        if not since_mark:
            message = "Unreachable code reached, open an issue"
            raise RuntimeError(message)
        with session_scope(ctx.obj["db_location"]) as session:
            mark = get_latest_mark(session)
            start_dt = mark.when
            if (
                datetime.now(timezone.utc) - start_dt > timedelta(hours=12)
                and since_mark != "force"
            ):
                message = (
                    f"Not using mark as it is old ({start_dt}). "
                    "Pass --since_mark=force to override."
                )
                raise ValueError(message)

    if new_project:
        with session_scope(ctx.obj["db_location"]) as session:
            project = Project(name=project_name)
            session.add(project)

    latitude, longitude = get_location()
    with session_scope(ctx.obj["db_location"]) as session:
        project = get_project(session, project_name)
        stint = Stint(
            start=start_dt.astimezone(timezone.utc),
            end=end_dt.astimezone(timezone.utc),
            project_id=project.id,
            description=" ".join(description),
            comment=comment,
            latitude=latitude,
            longitude=longitude,
        )
        session.add(stint)
        if since_mark:
            session.delete(mark)


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


@cli.command()
@click.pass_context
def mark(ctx):
    with session_scope(ctx.obj["db_location"]) as session:
        mark = Mark(when=datetime.now(timezone.utc))
        session.add(mark)


if __name__ == "__main__":
    cli(obj={})
