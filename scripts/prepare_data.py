"""Prepare trips data from GTFS feeds, aggregating stop-times into H3 hexagons and hourly bins, and save as Parquet"""

from pathlib import Path

from datetime import datetime

import click
import h3
import pandas as pd
from loguru import logger

H3_RESOLUTION = 8
DAY_OF_WEEK_COLS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def _get_hour(time_str: str) -> int:
    """Extract hour of day from GTFS time string

    Args:
        time_str (str): GTFS time string in format HH:MM:SS

    Returns:
        int: Hour of day (0-23) corresponding to the time, with times >= 24:00:00 wrapped around
    """

    return int(time_str.strip().split(":")[0]) % 24


def _get_service_dow(agency_dir: Path) -> pd.DataFrame | None:
    """Get active service days from calendar.txt for the agency, returning DataFrame with service_id and dow (0=Monday)

    Args:
        agency_dir (Path): Directory containing GTFS files for the agency

    Returns:
        pd.DataFrame | None: DataFrame with columns [service_id, dow] where dow=0 for Monday, or None if calendar.txt is missing or invalid
    """

    calendar_path = agency_dir / "calendar.txt"
    if not calendar_path.exists():
        return None
    df_calendar = pd.read_csv(calendar_path, dtype=str)
    if not all(c in df_calendar.columns for c in DAY_OF_WEEK_COLS):
        return None

    dow_map = {col: i for i, col in enumerate(DAY_OF_WEEK_COLS)}
    melted = df_calendar.melt(
        id_vars="service_id",
        value_vars=DAY_OF_WEEK_COLS,
        var_name="day",
        value_name="active",
    )
    active = melted[melted["active"] == "1"].copy()
    if active.empty:
        return None
    active["dow"] = active["day"].map(dow_map)

    return active[["service_id", "dow"]]


def _get_service_dow_from_calendar_dates(agency_dir: Path) -> pd.DataFrame | None:
    """Fallback: derive active days from calendar_dates.txt (exception_type=1)

    Args:
        agency_dir (Path): Directory containing GTFS files for the agency

    Returns:
        pd.DataFrame | None: DataFrame with columns [service_id, dow], or None if file is missing
    """

    path = agency_dir / "calendar_dates.txt"
    if not path.exists():
        return None
    df = pd.read_csv(path, dtype=str)
    if "exception_type" not in df.columns or "date" not in df.columns:
        return None

    added = df[df["exception_type"] == "1"].copy()
    if added.empty:
        return None
    added["dow"] = added["date"].apply(lambda d: datetime.strptime(d, "%Y%m%d").weekday())
    result = added[["service_id", "dow"]].drop_duplicates()

    return result if not result.empty else None


def _load_agency(agency_dir: Path) -> pd.DataFrame | None:
    """Load GTFS stop-times for one agency and return DataFrame with h3_index, hour_of_week, trip_id

    Args:
        agency_dir (Path): Directory containing GTFS files for the agency

    Returns:
        pd.DataFrame | None: DataFrame with columns [h3_index, hour_of_week, trip_id] or None if data is missing
    """

    required = ["stops.txt", "stop_times.txt", "trips.txt", "agency.txt"]
    if not all((agency_dir / f).exists() for f in required):
        logger.warning("Missing GTFS files in {}", agency_dir.name)
        return None

    agency_name = pd.read_csv(agency_dir / "agency.txt", dtype=str)["agency_name"].iloc[0]

    stops = pd.read_csv(
        agency_dir / "stops.txt",
        dtype=str,
        usecols=["stop_id", "stop_lat", "stop_lon"],
    )
    stop_times = pd.read_csv(
        agency_dir / "stop_times.txt",
        dtype=str,
        usecols=["trip_id", "stop_id", "departure_time"],
    )
    trips = pd.read_csv(
        agency_dir / "trips.txt",
        dtype=str,
        usecols=["trip_id", "service_id"],
    )

    service_active_days = _get_service_dow(agency_dir)
    if service_active_days is None:
        service_active_days = _get_service_dow_from_calendar_dates(agency_dir)
        if service_active_days is not None:
            logger.debug("{}: using calendar_dates.txt fallback", agency_dir.name)
    if service_active_days is None:
        logger.debug("{}: no calendar data, assuming all days", agency_dir.name)
        service_active_days = pd.DataFrame(
            [{"service_id": sid, "dow": d} for sid in trips["service_id"].unique() for d in range(7)]
        )
    if service_active_days.empty:
        logger.warning("No active services for {}", agency_dir.name)
        return None

    trip_dow = trips.merge(service_active_days, on="service_id")[["trip_id", "dow"]]
    df = stop_times.merge(trip_dow, on="trip_id").merge(stops, on="stop_id")
    df = df.dropna(subset=["departure_time", "stop_lat", "stop_lon"])
    if df.empty:
        return None

    df["hour"] = df["departure_time"].apply(_get_hour)
    df["hour_of_week"] = df["dow"] * 24 + df["hour"]
    df["h3_index"] = [
        h3.latlng_to_cell(float(la), float(lo), H3_RESOLUTION)
        for la, lo in zip(df["stop_lat"], df["stop_lon"])
    ]
    df["agency_name"] = agency_name

    return df[["h3_index", "hour_of_week", "trip_id", "agency_name"]]


def prepare_data(gtfs_dir: str, out_path: str) -> Path:
    """Process each agency's GTFS feed to extract stop-times, aggregate into H3 hexagons and hourly bins, and save combined data as Parquet

    Args:
        gtfs_dir (str): Directory containing GTFS feeds (one subdir per agency)
        out_path (str): Output path for trips Parquet file

    Raises:
        SystemExit: If no valid GTFS data is found or loaded

    Returns:
        Path: Path to saved trips Parquet file
    """

    frames: list[pd.DataFrame] = []
    for agency_dir in sorted(Path(gtfs_dir).iterdir()):
        if not agency_dir.is_dir():
            continue
        logger.info("Processing {}...", agency_dir.name)
        df = _load_agency(agency_dir)
        if df is not None:
            frames.append(df)
            logger.success("{}: {} stop-time records", agency_dir.name, len(df))

    if not frames:
        logger.error("No data loaded")
        raise SystemExit(1)

    combined = pd.concat(frames, ignore_index=True)

    res = (
        combined
        .groupby(["h3_index", "hour_of_week", "agency_name"])["trip_id"]
        .nunique()
        .reset_index(name="trip_count")
    )

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    res.to_parquet(out, index=False)
    logger.info("Trips data: {} rows, saved to {}", len(res), out)

    return out


@click.command()
@click.option("--gtfs-dir", default="gtfs", show_default=True)
@click.option("--out", default="data/trips.parquet", show_default=True)
def main(gtfs_dir: str, out: str) -> None:
    """Build H3 trips data from GTFS feeds"""
    prepare_data(gtfs_dir, out)


if __name__ == "__main__":
    main()
