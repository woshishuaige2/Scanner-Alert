"""
Usage:
python .\summarize_alert_run.py
python .\summarize_alert_run.py --symbol BIAF
python .\summarize_alert_run.py --date 2026-03-23 --start 08:00 --end 13:00
python .\summarize_alert_run.py --date 2026-03-23 --start 08:00 --end 13:00 --symbol BIAF
"""

import argparse
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterable, Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


HEADER_RE = re.compile(
    r"^\[(?P<timestamp>[\d\-: ]+)\] (?P<symbol>[A-Z.\-]+) \| "
    r"Session=(?P<session>[A-Z]+) \| Grade=(?P<grade>[A-Za-z ]+) \| Score=(?P<score>\d+)$"
)


@dataclass
class AlertRecord:
    timestamp: datetime
    symbol: str
    session: str
    grade: str
    score: int
    trigger: str
    score_factors: list[str]
    news_enabled: Optional[bool] = None
    headlines_fetched: int = 0
    same_day_headlines: int = 0
    meaningful_news_match: Optional[bool] = None
    news_match_reason: str = ""


def normalize_factor(raw_factor: str) -> str:
    factor = raw_factor.strip()
    if factor.startswith("RelVol "):
        return "RelVol"
    if factor.startswith("Local "):
        return "Local breakout"
    if factor.startswith("Session HOD breakout"):
        return "Session HOD breakout"
    if factor.startswith("Float "):
        return "Low float"
    if factor.startswith("Squeeze "):
        return "Squeeze"
    if factor.startswith("Held move"):
        return "Held move"
    if factor.startswith("No 1m H-L drawdown >="):
        threshold_match = re.search(r">=([0-9.]+)%", factor)
        threshold = threshold_match.group(1) if threshold_match else "X"
        return f"Drawdown < {threshold}%"
    if factor.startswith("Meaningful news"):
        return "Meaningful news"
    return factor


def parse_alert_blocks(log_path: Path) -> list[AlertRecord]:
    records: list[AlertRecord] = []
    lines = log_path.read_text(encoding="utf-8").splitlines()
    i = 0
    while i < len(lines):
        header_match = HEADER_RE.match(lines[i].strip())
        if not header_match:
            i += 1
            continue

        timestamp = datetime.strptime(header_match.group("timestamp"), "%Y-%m-%d %H:%M:%S")
        symbol = header_match.group("symbol")
        session = header_match.group("session")
        grade = header_match.group("grade")
        score = int(header_match.group("score"))

        trigger = ""
        score_factors: list[str] = []
        news_enabled: Optional[bool] = None
        headlines_fetched = 0
        same_day_headlines = 0
        meaningful_news_match: Optional[bool] = None
        news_match_reason = ""
        j = i + 1
        while j < len(lines):
            line = lines[j].rstrip()
            stripped = line.strip()
            if HEADER_RE.match(stripped):
                break
            if stripped.startswith("Trigger: "):
                trigger = stripped[len("Trigger: ") :]
            elif stripped == "Score factors:":
                j += 1
                while j < len(lines):
                    factor_line = lines[j].strip()
                    if not factor_line.startswith("- "):
                        j -= 1
                        break
                    factor_value = factor_line[2:].strip()
                    if factor_value != "None":
                        score_factors.append(factor_value)
                    j += 1
            elif stripped == "News debug:":
                j += 1
                while j < len(lines):
                    news_line = lines[j].strip()
                    if not news_line.startswith("- "):
                        j -= 1
                        break
                    news_value = news_line[2:].strip()
                    if news_value.startswith("Enabled: "):
                        news_enabled = news_value.split(": ", 1)[1] == "True"
                    elif news_value.startswith("Headlines fetched: "):
                        headlines_fetched = int(news_value.split(": ", 1)[1])
                    elif news_value.startswith("Same-day headlines: "):
                        same_day_headlines = int(news_value.split(": ", 1)[1])
                    elif news_value.startswith("Meaningful match found: "):
                        meaningful_news_match = news_value.split(": ", 1)[1] == "True"
                    elif news_value.startswith("Match reason: "):
                        news_match_reason = news_value.split(": ", 1)[1]
                    j += 1
            j += 1

        records.append(
            AlertRecord(
                timestamp=timestamp,
                symbol=symbol,
                session=session,
                grade=grade,
                score=score,
                trigger=trigger,
                score_factors=score_factors,
                news_enabled=news_enabled,
                headlines_fetched=headlines_fetched,
                same_day_headlines=same_day_headlines,
                meaningful_news_match=meaningful_news_match,
                news_match_reason=news_match_reason,
            )
        )
        i = j
    return records


def parse_time_arg(value: Optional[str]) -> Optional[time]:
    if not value:
        return None
    return datetime.strptime(value, "%H:%M").time()


def apply_filters(
    records: Iterable[AlertRecord],
    target_date: Optional[date] = None,
    start_time: Optional[time] = None,
    end_time: Optional[time] = None,
    symbol: Optional[str] = None,
) -> list[AlertRecord]:
    filtered: list[AlertRecord] = []
    symbol_upper = symbol.upper() if symbol else None
    for record in records:
        if target_date and record.timestamp.date() != target_date:
            continue
        if start_time and record.timestamp.time() < start_time:
            continue
        if end_time and record.timestamp.time() > end_time:
            continue
        if symbol_upper and record.symbol.upper() != symbol_upper:
            continue
        filtered.append(record)
    return filtered


def build_factor_counter(records: Iterable[AlertRecord]) -> Counter:
    counter: Counter = Counter()
    for record in records:
        for factor in record.score_factors:
            counter[normalize_factor(factor)] += 1
    return counter


def build_news_reason_counter(records: Iterable[AlertRecord]) -> Counter:
    counter: Counter = Counter()
    for record in records:
        if record.news_match_reason:
            counter[record.news_match_reason] += 1
    return counter


def build_news_status_counter(records: Iterable[AlertRecord]) -> Counter:
    counter: Counter = Counter()
    for record in records:
        if record.meaningful_news_match:
            counter["Meaningful match"] += 1
        elif record.same_day_headlines > 0:
            counter["Same-day headlines, no match"] += 1
        elif record.headlines_fetched > 0:
            counter["Fetched headlines, none same-day"] += 1
        else:
            counter["No headlines fetched"] += 1
    return counter


def configure_time_axis(ax, timestamps: list[datetime]):
    if not timestamps:
        return
    span_seconds = max((timestamps[-1] - timestamps[0]).total_seconds(), 1)
    if span_seconds <= 4 * 3600:
        locator = mdates.MinuteLocator(interval=15)
    elif span_seconds <= 10 * 3600:
        locator = mdates.MinuteLocator(interval=30)
    else:
        locator = mdates.HourLocator(interval=1)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.tick_params(axis="x", rotation=30)


def plot_cumulative_alerts(records: list[AlertRecord], output_path: Path, scope_label: str):
    timestamps = [record.timestamp for record in records]
    cumulative_counts = list(range(1, len(records) + 1))

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(timestamps, cumulative_counts, color="#145DA0", linewidth=2.2, marker="o", markersize=3)
    ax.set_title(f"Cumulative Alerts Over Time | {scope_label}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Alert count")
    ax.grid(True, alpha=0.25)
    configure_time_axis(ax, timestamps)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def plot_alerts_per_bucket(
    records: list[AlertRecord],
    output_path: Path,
    scope_label: str,
    bucket_minutes: int = 15,
):
    bucket_counts: Counter = Counter()
    for record in records:
        minute_floor = (record.timestamp.minute // bucket_minutes) * bucket_minutes
        bucket_start = record.timestamp.replace(minute=minute_floor, second=0, microsecond=0)
        bucket_counts[bucket_start] += 1

    times = sorted(bucket_counts)
    counts = [bucket_counts[t] for t in times]

    fig, ax = plt.subplots(figsize=(12, 5))
    bar_width = bucket_minutes / (24 * 60) * 0.85
    ax.bar(times, counts, width=bar_width, color="#2E8B57", edgecolor="#1F5D3A")
    ax.set_title(f"Alerts Per {bucket_minutes}-Minute Bucket | {scope_label}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Alerts in bucket")
    ax.grid(True, axis="y", alpha=0.25)
    configure_time_axis(ax, times)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def plot_cumulative_factor_counts(records: list[AlertRecord], output_path: Path, scope_label: str):
    category_names = [
        "RelVol",
        "Local breakout",
        "Session HOD breakout",
        "Low float",
        "Squeeze",
        "Held move",
        "Drawdown < 10.0%",
        "Drawdown < 5.0%",
        "Meaningful news",
    ]
    running_counts = {name: [] for name in category_names}
    timestamps = [record.timestamp for record in records]
    totals = {name: 0 for name in category_names}

    for record in records:
        record_categories = {normalize_factor(factor) for factor in record.score_factors}
        for name in category_names:
            if name in record_categories:
                totals[name] += 1
            running_counts[name].append(totals[name])

    active_categories = [name for name in category_names if totals[name] > 0]
    fig, ax = plt.subplots(figsize=(12, 6))
    for name in active_categories:
        ax.plot(timestamps, running_counts[name], linewidth=1.8, label=name)

    ax.set_title(f"Cumulative Score Factor Hits Over Time | {scope_label}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Alerts containing factor")
    ax.grid(True, alpha=0.25)
    configure_time_axis(ax, timestamps)
    if active_categories:
        ax.legend(loc="upper left", ncol=2, fontsize=9)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def plot_factor_totals(records: list[AlertRecord], output_path: Path, scope_label: str):
    counter = build_factor_counter(records)
    items = [(name, count) for name, count in counter.items() if count > 0]
    items.sort(key=lambda item: (-item[1], item[0]))
    names = [item[0] for item in items]
    counts = [item[1] for item in items]

    fig_height = max(4.5, 0.45 * max(len(names), 1))
    fig, ax = plt.subplots(figsize=(11, fig_height))
    ax.barh(names, counts, color="#CC5500")
    ax.set_title(f"Total Alerts By Score Factor | {scope_label}")
    ax.set_xlabel("Count")
    ax.set_ylabel("Score factor")
    ax.grid(True, axis="x", alpha=0.25)
    ax.invert_yaxis()
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def plot_news_status_totals(records: list[AlertRecord], output_path: Path, scope_label: str):
    counter = build_news_status_counter(records)
    labels = [
        "Meaningful match",
        "Same-day headlines, no match",
        "Fetched headlines, none same-day",
        "No headlines fetched",
    ]
    values = [counter.get(label, 0) for label in labels]

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(labels, values, color=["#1f77b4", "#ff7f0e", "#d62728", "#7f7f7f"])
    ax.set_title(f"News Status Totals | {scope_label}")
    ax.set_xlabel("News status")
    ax.set_ylabel("Alert count")
    ax.grid(True, axis="y", alpha=0.25)
    ax.tick_params(axis="x", rotation=15)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def write_text_summary(records: list[AlertRecord], output_path: Path, scope_label: str):
    factor_counter = build_factor_counter(records)
    news_reason_counter = build_news_reason_counter(records)
    news_status_counter = build_news_status_counter(records)
    grade_counter = Counter(record.grade for record in records)
    session_counter = Counter(record.session for record in records)
    symbol_counter = Counter(record.symbol for record in records)
    alerts_with_headlines = sum(1 for record in records if record.headlines_fetched > 0)
    alerts_with_same_day_news = sum(1 for record in records if record.same_day_headlines > 0)
    alerts_with_meaningful_news = sum(1 for record in records if record.meaningful_news_match)
    total_headlines_fetched = sum(record.headlines_fetched for record in records)
    total_same_day_headlines = sum(record.same_day_headlines for record in records)

    with output_path.open("w", encoding="utf-8") as handle:
        handle.write("Alert Run Summary\n")
        handle.write("=================\n")
        handle.write(f"Scope: {scope_label}\n")
        if records:
            handle.write(f"Window start: {records[0].timestamp}\n")
            handle.write(f"Window end:   {records[-1].timestamp}\n")
        handle.write(f"Total alerts: {len(records)}\n\n")

        handle.write("Grades:\n")
        for grade, count in sorted(grade_counter.items()):
            handle.write(f"- {grade}: {count}\n")
        handle.write("\n")

        handle.write("Sessions:\n")
        for session, count in sorted(session_counter.items()):
            handle.write(f"- {session}: {count}\n")
        handle.write("\n")

        handle.write("Top symbols:\n")
        for symbol, count in symbol_counter.most_common(15):
            handle.write(f"- {symbol}: {count}\n")
        handle.write("\n")

        handle.write("Score factors:\n")
        for factor, count in sorted(factor_counter.items(), key=lambda item: (-item[1], item[0])):
            handle.write(f"- {factor}: {count}\n")
        handle.write("\n")

        handle.write("News summary:\n")
        handle.write(f"- Alerts with any headlines fetched: {alerts_with_headlines}\n")
        handle.write(f"- Alerts with same-day headlines: {alerts_with_same_day_news}\n")
        handle.write(f"- Alerts with meaningful news match: {alerts_with_meaningful_news}\n")
        handle.write(f"- Total headlines fetched across alerts: {total_headlines_fetched}\n")
        handle.write(f"- Total same-day headlines across alerts: {total_same_day_headlines}\n")
        handle.write("\n")

        handle.write("News status totals:\n")
        for label, count in news_status_counter.items():
            handle.write(f"- {label}: {count}\n")
        handle.write("\n")

        handle.write("Top news match reasons:\n")
        for reason, count in news_reason_counter.most_common(10):
            handle.write(f"- {reason}: {count}\n")


def main():
    parser = argparse.ArgumentParser(description="Summarize scanner alert runs from temp_alert_score_audit.log.")
    parser.add_argument("--log", default="temp_alert_score_audit.log", help="Path to the audit log file.")
    parser.add_argument("--date", dest="date_str", help="Filter to one date in YYYY-MM-DD format.")
    parser.add_argument("--start", dest="start_str", help="Optional start time in HH:MM.")
    parser.add_argument("--end", dest="end_str", help="Optional end time in HH:MM.")
    parser.add_argument("--symbol", help="Optional symbol filter, e.g. LICN.")
    parser.add_argument(
        "--output-dir",
        default="out/alert_run_summary",
        help="Directory for generated plots and summary text.",
    )
    args = parser.parse_args()

    log_path = Path(args.log)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    target_date = datetime.strptime(args.date_str, "%Y-%m-%d").date() if args.date_str else None
    start_time = parse_time_arg(args.start_str)
    end_time = parse_time_arg(args.end_str)

    records = parse_alert_blocks(log_path)
    records = apply_filters(
        records=records,
        target_date=target_date,
        start_time=start_time,
        end_time=end_time,
        symbol=args.symbol,
    )
    records.sort(key=lambda record: record.timestamp)
    scope_label = f"Symbol {args.symbol.upper()}" if args.symbol else "All Symbols"

    if not records:
        raise SystemExit("No matching alert records found for the requested filters.")

    summary_path = output_dir / "summary.txt"
    write_text_summary(records, summary_path, scope_label)
    plot_cumulative_alerts(records, output_dir / "alerts_cumulative.png", scope_label)
    plot_alerts_per_bucket(records, output_dir / "alerts_per_15min.png", scope_label)
    plot_cumulative_factor_counts(records, output_dir / "factor_hits_cumulative.png", scope_label)
    plot_factor_totals(records, output_dir / "factor_totals.png", scope_label)
    plot_news_status_totals(records, output_dir / "news_status_totals.png", scope_label)

    factor_counter = build_factor_counter(records)
    meaningful_news_matches = sum(1 for record in records if record.meaningful_news_match)
    print(f"Generated summary for {len(records)} alerts ({scope_label}).")
    print(f"Output directory: {output_dir.resolve()}")
    print(f"Meaningful news matches: {meaningful_news_matches}")
    print("Score factor totals:")
    for factor, count in sorted(factor_counter.items(), key=lambda item: (-item[1], item[0])):
        print(f"- {factor}: {count}")


if __name__ == "__main__":
    main()
