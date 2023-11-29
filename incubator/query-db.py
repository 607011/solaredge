#!/usr/bin/env python3

from dotenv import dotenv_values
from influxdb_client import InfluxDBClient
import pytz


def query_db() -> None:
    config = dotenv_values(".env")
    tz = pytz.timezone(config["TIMEZONE"])
    bucket = config["INFLUXDB_BUCKET"]
    influx_client = InfluxDBClient(
        url=config["INFLUXDB_URL"],
        token=config["INFLUXDB_TOKEN"],
        enable_gzip=True,
        org=config["INFLUXDB_ORG"],
    )
    query_api = influx_client.query_api()
    query = f"""from(bucket:"{bucket}")
        |> range(start: -90d)
        |> filter(fn: (r) => r._measurement  == "Purchased" or r._measurement  == "Consumption")
        |> group(columns: ["_time"], mode: "by")"""
    result = query_api.query(query=query)
    for table in result:
        for record in table.records:
            print(
                f"""{record.get_time().astimezone(tz)} """
                f"""{record.get_value():.0f} """
                f"""{record.get_field()} """
                f"""{record.get_measurement()}"""
            )


def main() -> None:
    query_db()


if __name__ == "__main__":
    main()
