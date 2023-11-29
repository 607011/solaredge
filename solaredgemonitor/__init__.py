import urllib.request
import urllib.parse
import urllib.error
import json
import sys
from datetime import date, datetime
from typing_extensions import Self

VERSION = "2023.11"
__version__ = VERSION
__all__ = ["BATTERY_STATES", "ALL_METERS", "SolarEdgeMonitor", "TimeUnit", "Meters"]

BASE_URL = "https://monitoringapi.solaredge.com"

BATTERY_STATES = {
    0: "Invalid",
    1: "Standby",
    2: "Therman Mgmt.",
    3: "Enabled",
    4: "FAULT",
    6: "UNKNOWN",
}


class TimeUnit:
    QuarterHour = "QUARTER_OF_AN_HOUR"
    Hour = "HOUR"
    Day = "DAY"
    Month = "MONTH"
    Year = "YEAR"


class Meters:
    Production = "PRODUCTION"
    Consumption = "CONSUMPTION"
    SelfConsumption = "SELFCONSUMPTION"
    FeedIn = "FEEDIN"
    Purchased = "PURCHASED"


ALL_METERS = ",".join(
    [
        Meters.Production,
        Meters.Consumption,
        Meters.SelfConsumption,
        Meters.FeedIn,
        Meters.Purchased,
    ]
)


class SolarEdgeMonitor:
    def __init__(self, config: dict) -> Self:
        self.site_id = config["SITE_ID"]
        self.api_key = config["API_KEY"]

    def query(self, url: str) -> dict | None:
        print(f"""SolarEdgeMonitor.query({url}""")
        try:
            r = urllib.request.urlopen(url)
            data = json.loads(r.read())
            return data
        except urllib.error.HTTPError as e:
            print(f"""HTTP Error {e.code}: “{e.msg}”""")
        except json.JSONDecodeError as e:
            print(f"""JSON Decode Error {e.code}: “{e.msg}”""")

    def init(self) -> None:
        data = self.equipment_list()
        if data is None:
            return
        try:
            self.inverter_ser_no = [d["serialNumber"] for d in data]
        except (KeyError, TypeError, ValueError) as err:
            print(err, file=sys.stderr)

    def energy(
        self, start_time: date, end_time: date, time_unit=TimeUnit.QuarterHour
    ) -> dict | None:
        params = urllib.parse.urlencode(
            {
                "startDate": start_time.strftime("%Y-%m-%d"),
                "endDate": end_time.strftime("%Y-%m-%d"),
                "timeUnit": time_unit,
                "api_key": self.api_key,
            },
            quote_via=urllib.parse.quote,
        )
        url = f"""{BASE_URL}/site/{self.site_id}/energy?{params}"""
        return self.query(url)

    def meters(
        self,
        start_time: datetime,
        end_time: datetime,
        time_unit: str = TimeUnit.QuarterHour,
    ) -> dict | None:
        params = urllib.parse.urlencode(
            {
                "startTime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "endTime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "timeUnit": time_unit,
                "api_key": self.api_key,
            },
            quote_via=urllib.parse.quote,
        )
        url = f"""{BASE_URL}/site/{self.site_id}/meters?{params}"""
        return self.query(url)

    def inventory(self) -> dict | None:
        url = f"""{BASE_URL}/site/{self.site_id}/inventory?api_key={self.api_key}"""
        return self.query(url)

    def power_flow(self) -> dict | None:
        url = f"""{BASE_URL}/site/{self.site_id}/currentPowerFlow?api_key={self.api_key}"""
        return self.query(url)

    def equipment_list(self) -> dict | None:
        """Return a list of inverters/SMIs in the specific site."""
        url = f"""{BASE_URL}/equipment/{self.site_id}/list?api_key={self.api_key}"""
        data = self.query(url)
        if data is None:
            return
        try:
            return data["reporters"]["list"]
        except (KeyError, TypeError, ValueError) as err:
            print(err, file=sys.stderr)

    # def equipment_data(self, inverter_ser_no: str, start_time: datetime, end_time: datetime):
    #     url = f"""{BASE_URL}/equipment/{self.site_id}/{inverter_ser_no}/data?api_key={self.api_key}&startTime={urllib.parse.quote(start_time.strftime("%Y-%m-%d %H:%M:%S"))}&endTime={urllib.parse.quote(end_time.strftime("%Y-%m-%d %H:%M:%S"))}"""
    #     return self.query(url)

    def sensors(self) -> dict | None:
        """
        Return a list of all the sensors in the site, and the device
        to which they are connected.
        """
        url = f"""{BASE_URL}/equipment/{self.site_id}/sensors?api_key={self.api_key}"""
        data = self.query(url)
        if data is None:
            return
        try:
            return data["SiteSensors"]
        except (KeyError, TypeError, ValueError) as err:
            print(err, file=sys.stderr)

    def power_detail(
        self,
        start_time: datetime,
        end_time: datetime,
        time_unit: str = TimeUnit.QuarterHour,
        meters: str = ALL_METERS,
    ) -> tuple[str, dict] | None:
        """
        Fetch detailed site power measurements from meters such as
        consumption, export (feed-in), import (purchase), etc.

        Calculated meter readings (also referred to as "virtual meters"),
        such as self-consumption, are calculated using the data measured
        by the meter and the inverters.

        This API is limited to one-month period. This means that the
        period between `start_time` and `end_time` should not exceed one
        month. If the period is longer, the system will generate error 403 
        with proper description.
        """
        params = urllib.parse.urlencode(
            {
                "startTime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "endTime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "timeUnit": time_unit,
                "meters": meters,
                "api_key": self.api_key,
            },
            safe=",:",
            quote_via=urllib.parse.quote,
        )
        url = f"""{BASE_URL}/site/{self.site_id}/powerDetails?{params}"""
        data = self.query(url)
        if data is None:
            return
        try:
            return data["powerDetails"]["meters"], data["powerDetails"]["unit"]
        except (KeyError, TypeError, ValueError) as err:
            print(err, file=sys.stderr)

    def batteries(self, start_time: datetime, end_time: datetime) -> dict | None:
        """
        Get detailed storage information from batteries: the state of
        energy, power and lifetime energy.
        """
        params = urllib.parse.urlencode(
            {
                "startTime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "endTime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "api_key": self.api_key,
            },
            safe=":",
            quote_via=urllib.parse.quote,
        )
        url = f"""{BASE_URL}/site/{self.site_id}/storageData?{params}"""
        data = self.query(url)
        if data is None:
            return
        try:
            return data["storageData"]["batteries"]
        except (KeyError, TypeError, ValueError) as err:
            print(err, file=sys.stderr)
        return
