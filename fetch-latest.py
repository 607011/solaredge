#!/usr/bin/env python3

from dotenv import dotenv_values
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS
import json
import pytz
import sys
from pprint import pprint
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BlockingScheduler
from argparse import ArgumentParser

from solaredgemonitor import SolarEdgeMonitor


ONE_SEC = timedelta(seconds=1)
MIN_DT = ONE_SEC
ONE_WEEK = timedelta(weeks=1)
MAX_LOOKBACK_BATTERY = ONE_WEEK
MAX_LOOKBACK_POWERDETAILS = timedelta(weeks=4)


def fetch_and_store_latest_power_details():
    """Fetch latest battery data and write it to InfluxDB."""

    config = dotenv_values(".env")
    tz = pytz.timezone(config["TIMEZONE"])
    bucket = config["INFLUXDB_BUCKET"]
    org = config["INFLUXDB_ORG"]
    influx_client = InfluxDBClient(
        url=config["INFLUXDB_URL"],
        token=config["INFLUXDB_TOKEN"],
        org=org,
    )

    # get most recent entry
    query_api = influx_client.query_api()
    query = f"""from(bucket: "{bucket}")
        |> range(start: -2w)
        |> filter(fn: (r) => r._measurement == "Production" or r._measurement == "Consumption")
        |> last()"""
    result = query_api.query(query=query)
    from_date = datetime(2199, 12, 31).astimezone(tz)
    for table in result:
        for record in table.records:
            t = record.get_time().astimezone(tz)
            if from_date > t:
                from_date = t

    influx_write_api = influx_client.write_api(write_options=ASYNCHRONOUS)

    mon = SolarEdgeMonitor(config)
    to_date = datetime.now(tz=tz)
    while from_date < to_date:
        end_date = from_date + MAX_LOOKBACK_POWERDETAILS
        meters, unit = mon.power_detail(from_date + MIN_DT, end_date)
        from_date = end_date

        for entry in meters:
            if "type" not in entry:
                print("data doesn’t contain type", file=sys.stderr)
                continue
            if "values" not in entry:
                print("data doesn’t contain values", file=sys.stderr)
                continue
            power_type = entry["type"]
            for measurement in entry["values"]:
                if "value" not in measurement:
                    continue
                ts = datetime.strptime(measurement["date"], "%Y-%m-%d %H:%M:%S")
                point = (
                    Point(power_type)
                    .tag("unit", unit)
                    .time(ts, WritePrecision.S)
                    .field("power", measurement["value"])
                )
                print(f"""{ts} {measurement["value"]:.0f} {unit} {power_type}""")
                influx_write_api.write(bucket, record=point)


def fetch_and_store_latest_battery_data():
    """Fetch latest battery data and write it to InfluxDB."""

    config = dotenv_values(".env")
    tz = pytz.timezone(config["TIMEZONE"])
    bucket = config["INFLUXDB_BUCKET"]
    org = config["INFLUXDB_ORG"]
    influx_client = InfluxDBClient(
        url=config["INFLUXDB_URL"],
        token=config["INFLUXDB_TOKEN"],
        org=org,
    )

    query_api = influx_client.query_api()
    query = f"""from(bucket: "{bucket}")
        |> range(start: -2w)
        |> last()"""
    result = query_api.query(query=query)
    from_date = datetime(2199, 12, 31).astimezone(tz)
    for table in result:
        for record in table.records:
            t = record.get_time().astimezone(tz)
            if from_date > t:
                from_date = t

    mon = SolarEdgeMonitor(config)
    measurements = []
    to_date = datetime.now(tz=tz)
    while from_date < to_date:
        end_date = from_date + MAX_LOOKBACK_BATTERY
        new_measurements = mon.batteries(from_date + MIN_DT, end_date)
        if new_measurements is not None:
            measurements += new_measurements
        from_date = end_date

    # pprint(measurements, indent=2)

    influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    for entry in measurements:
        if "modelNumber" not in entry:
            print("data doesn’t contain ‘modelNumber’", file=sys.stderr)
            continue
        if "serialNumber" not in entry:
            print("data doesn’t contain ‘serialNumber’", file=sys.stderr)
            continue
        model = entry["modelNumber"]
        serial_no = entry["serialNumber"]
        for telemetry in entry["telemetries"]:
            if "timeStamp" not in telemetry:
                print("telemetry data doesn’t contain timeStamp", file=sys.stderr)
                continue
            if "batteryPercentageState" not in telemetry:
                print(
                    "ERROR: telemetry data doesn’t contain ‘batteryPercentageState’",
                    file=sys.stderr,
                )
                continue
            if "power" not in telemetry:
                print("telemetry data doesn’t contain ‘power’", file=sys.stderr)
                continue
            ts = datetime.strptime(
                telemetry["timeStamp"], "%Y-%m-%d %H:%M:%S"
            ).astimezone(tz)
            point = (
                Point("battery")
                .tag("battery", f"""{model} ({serial_no})""")
                .time(ts, WritePrecision.S)
                .field("pct", telemetry["batteryPercentageState"])
                .field("watts", telemetry["power"])
            )
            influx_write_api.write(bucket, record=point)
            print(
                f"""{ts} {round(telemetry["power"])} W; {round(telemetry["batteryPercentageState"])} %"""
            )


def main():
    parser = ArgumentParser(
        prog="se-fetch-latest", description="Fetch latest data from SolarEdge inverter"
    )
    parser.add_argument("--once", action="store_true", help="fetch once, then exit")
    parser.add_argument("--mins", help="Fetch in intervals of MINS minutes", default=15)
    args = parser.parse_args()

    def bgfun():
        fetch_and_store_latest_battery_data()
        fetch_and_store_latest_power_details()

    bgfun()
    if args.once:
        return

    sched = BlockingScheduler()
    sched.add_job(bgfun, "cron", minute=f"""*/{args.mins}""")
    try:
        sched.start()
    except KeyboardInterrupt:
        print("Shutting down ...")
        sched.shutdown()


if __name__ == "__main__":
    main()
