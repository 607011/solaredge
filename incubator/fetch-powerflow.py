#!/usr/bin/env python3

from dotenv import dotenv_values
from pprint import pprint
from influxdb_client import InfluxDBClient
import pytz
from solaredgemonitor import SolarEdgeMonitor


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
    # query_api = influx_client.query_api()
    # query = f"""from(bucket:"{bucket}")
    #     |> range(start: -1d)
    #     |> group(columns: ["_time"], mode: "by")"""
    # result = query_api.query(query=query)
    # for table in result:
    #     for record in table.records:
    #         print(
    #             f"""{record.get_time().astimezone(tz)} """
    #             f"""{record.get_value():.0f} """
    #             f"""{record.get_field()}"""
    #         )
    mon = SolarEdgeMonitor(config)
    data = mon.power_flow()
    pprint(data)



def main() -> None:
    query_db()


if __name__ == "__main__":
    main()
