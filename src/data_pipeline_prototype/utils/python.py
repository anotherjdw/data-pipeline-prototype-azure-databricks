import re
from datetime import UTC, datetime, timedelta


def validate_dates(date_ranges: list[str]):

    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    invalid_dates = [i for i in date_ranges if not date_pattern.match(i)]
    if invalid_dates:
        raise ValueError(f"Invalid dates: {invalid_dates}. Expected format: YYYY-MM-DD")


def generate_backfill_date_range(
    start_date: str,
    end_date: str,
) -> list[str]:

    if start_date is None or end_date is None:
        raise ValueError("backfill requires both start_date and end_date.")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        raise ValueError("start_date must be earlier than or the same as end_date.")

    if start.date() > datetime.now(UTC).date() or end.date() > datetime.now(UTC).date():
        raise ValueError(
            "start_date and end_date values must not take a value later than the current day."
        )

    date_range = [
        (start + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range((end - start).days + 1)
    ]

    return date_range


def generate_lookback_date_range(
    lookback_days: int = 1,
    include_current_date: bool = False,
) -> list[str]:

    if lookback_days < 0:
        raise ValueError("lookback_days must not be negative.")

    today = datetime.now(UTC).date()
    start = today - timedelta(days=lookback_days)
    end = today if (include_current_date or lookback_days == 0) else today - timedelta(days=1)

    date_range = [
        (start + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range((end - start).days + 1)
    ]

    return date_range


def resolve_processing_dates(
    processing_type: str,
    lookback_days: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:

    if processing_type == "lookback":
        if lookback_days is None:
            raise ValueError(
                "lookback PROCESSING_TYPE requires an integer value for lookback_days."
            )
        return generate_lookback_date_range(lookback_days=lookback_days)

    if processing_type == "backfill":
        return generate_backfill_date_range(
            start_date=start_date,
            end_date=end_date,
        )

    raise ValueError(f"PROCESSING_TYPE must be 'lookback' or 'backfill', not {processing_type!r}")
