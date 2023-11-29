#!/usr/bin/env python3

from dotenv import dotenv_values
from pprint import pprint
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS
import pytz
import sys
import json
from solaredgemonitor import SolarEdgeMonitor
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BlockingScheduler
from argparse import ArgumentParser


def _fetch_and_store_battery_history(
    from_date: datetime, filename: str = None, to_date: datetime = datetime.now()
) -> None:
    """Fetch and store battery data."""

    config = dotenv_values(".env")
    mon = SolarEdgeMonitor(config)
    measurements = []
    while from_date < to_date:
        end_date = from_date + timedelta(weeks=1)
        measurements += mon.batteries(from_date, end_date)
        from_date = end_date

    if filename is not None:
        with open(filename, "w+") as out:
            json.dump(measurements, out)


def _json_to_db(json_filename):
    """Read battery data from JSON file written with `fetch_and_store_battery_history()`
    and write it to InfluxDB.
    """

    config = dotenv_values(".env")
    bucket = config["INFLUXDB_BUCKET"]
    org = config["INFLUXDB_ORG"]
    influx_client = InfluxDBClient(
        url=config["INFLUXDB_URL"],
        token=config["INFLUXDB_TOKEN"],
        org=org,
    )
    influx_write_api = influx_client.write_api(write_options=ASYNCHRONOUS)

    buckets_api = influx_client.buckets_api()
    buckets_api.delete_bucket(buckets_api.find_bucket_by_name(bucket))
    created_bucket = buckets_api.create_bucket(bucket_name=bucket, org=org)
    print(created_bucket)

    with open(json_filename, "r") as batt_data:
        data = json.load(batt_data)
        for entry in data:
            model = entry["modelNumber"]
            serial_no = entry["serialNumber"]
            for telemetry in entry["telemetries"]:
                ts = datetime.strptime(telemetry["timeStamp"], "%Y-%m-%d %H:%M:%S")
                point = (
                    Point("battery")
                    .tag("battery", f"""{model} ({serial_no})""")
                    .time(ts, WritePrecision.S)
                    .field("pct", telemetry["batteryPercentageState"])
                    .field("watts", telemetry["power"])
                )
                influx_write_api.write(bucket, record=point)


def get_complete_battery_history() -> None:
    json_filename = "batteries.json"
    _fetch_and_store_battery_history(datetime(2023, 8, 10), json_filename)
    _json_to_db(json_filename)



def _fetch_and_store_powerdetail_history(
    from_date: datetime, to_date: datetime
) -> None:
    """Fetch and store power detail data."""

    config = dotenv_values(".env")

    bucket = config["INFLUXDB_BUCKET"]
    org = config["INFLUXDB_ORG"]
    influx_client = InfluxDBClient(
        url=config["INFLUXDB_URL"],
        token=config["INFLUXDB_TOKEN"],
        org=org,
    )
    influx_write_api = influx_client.write_api(write_options=ASYNCHRONOUS)

    mon = SolarEdgeMonitor(config)
    dt = timedelta(weeks=2)
    while from_date < to_date:
        end_date = from_date + dt
        meters, unit = mon.power_detail(from_date, end_date)
        from_date = end_date

        for entry in meters:
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
                influx_write_api.write(bucket, record=point)


def get_complete_powerdetail_history() -> None:
    config = dotenv_values(".env")
    tz = pytz.timezone(config["TIMEZONE"])
    from_date = datetime.strptime(config["INSTALLATION_DATE"], "%Y-%m-%d").astimezone(tz)
    to_date = datetime.now().astimezone(tz)
    _fetch_and_store_powerdetail_history(from_date, to_date)


ONE_SEC = timedelta(seconds=1)
MIN_DT = ONE_SEC
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


def main() -> None:
    parser = ArgumentParser(
        prog='pv-fetch',
        description='Fetch power details from SolarEdge inverter')
    parser.add_argument("--once", action="store_true", help="fetch once, then exit")
    parser.add_argument("--mins", help="Fetch in intervals of MINS minutes", default=15)
    args = parser.parse_args()

    def bgfun():
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
