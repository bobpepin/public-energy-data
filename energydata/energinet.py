import urllib.request
import json
import re
import datetime
from collections import namedtuple
from sys import intern
import pytz

field_mappings = {
    'SolarMWh': "production_solar",
    'SolarPower': "production_solar",
    "OffshoreWindPower": "production_wind_offshore",
    'OnshoreWindPower': "production_wind_onshore",
    'OnshoreWindMWh': "production_wind_onshore",
    'ThermalPowerMWh': "production_thermal",
    "ConsumptionkWh": "consumption"
}

def fetch_edh(descriptor, limit=0, offset=0):
    dataset = descriptor["dataset"]
    tz = pytz.timezone("Europe/Copenhagen")    
    start_dt = datetime.datetime.fromisoformat(descriptor["start_datetime"])
    start_str = start_dt.astimezone(tz).strftime("%Y-%m-%dT%H:%M")
    end_dt = datetime.datetime.fromisoformat(descriptor["end_datetime"])
    end_str = end_dt.astimezone(tz).strftime("%Y-%m-%dT%H:%M")
    url = f"https://api.energidataservice.dk/dataset/{dataset}?limit={limit}&offset={offset}&start={start_str}&end={end_str}"
    with urllib.request.urlopen(url) as fileobj:
        data = json.load(fileobj)    
    records = data["records"]
    return records
        
def fetch_metadata(descriptor):
    dataset = descriptor["dataset"]
    tz = pytz.timezone("Europe/Copenhagen")    
    start_dt = datetime.datetime.fromisoformat(descriptor["start_datetime"])
    start_str = start_dt.astimezone(tz).strftime("%Y-%m-%dT%H:%M")
    end_dt = datetime.datetime.fromisoformat(descriptor["end_datetime"])
    end_str = end_dt.astimezone(tz).strftime("%Y-%m-%dT%H:%M")
    url = f"https://api.energidataservice.dk/dataset/{dataset}?limit=1&offset=0&start={start_str}&end={end_str}"
    with urllib.request.urlopen(url) as fileobj:
        data = json.load(fileobj)
    total = data["total"]
    meta_url = f"https://api.energidataservice.dk/meta/dataset/{dataset}"
    with urllib.request.urlopen(meta_url) as fileobj:
        metadata = json.load(fileobj)
    column_names = [x["dbColumn"] for x in metadata["columns"]]
    if "HourUTC" in column_names or "Month" in column_names:
        column_names += ["datetime_start", "datetime_end"]
    if "MunicipalityNo" in column_names:
        column_names += ["region_code"]
    if "OffshoreWindLt100MW_MWh" in column_names or "OffshoreWindGe100MW_MWh" in column_names:
        column_names.append("production_wind_offshore")
    if "CentralPower" in column_names or "DecentralPower" in column_names:
        column_names.append("production_thermal")
    for f1, f2 in field_mappings.items():
        if f1 in column_names:
            column_names.append(f2)
    column_names = [intern(x) for x in column_names]
    return {
        "fields": column_names,
        "records_total": total
    }

def fetch(descriptor, chunk_size=8192):
    tz = pytz.timezone("Europe/Copenhagen")
    for record_data in fetch_edh(descriptor):
        record = {intern(k): v for k, v in record_data.items()}
        if "HourUTC" in record:
            dt = datetime.datetime.fromisoformat(record["HourUTC"]).replace(tzinfo=datetime.timezone.utc)
            record[intern("datetime_start")] = dt.astimezone(tz).isoformat()
            record[intern("datetime_end")] = (dt + datetime.timedelta(seconds=3600)).astimezone(tz).isoformat()
        if "Month" in record:
            this_month = datetime.datetime.fromisoformat(record["Month"]).date()
            next_month = datetime.date(
                this_month.year + this_month.month // 12,
                this_month.month % 12 + 1,
                1
            )
            record[intern("datetime_start")] = this_month.isoformat()
            record[intern("datetime_end")] = next_month.isoformat()
        if "MunicipalityNo" in record:
            municipality = record["MunicipalityNo"]
            record[intern("region_code")] = f"dk_mun_{municipality}"
        if "OffshoreWindLt100MW_MWh" in record or "OffshoreWindGe100MW_MWh" in record:
            val = 0
            if record.get("OffshoreWindLt100MW_MWh", 0) is not None:
                val += record.get("OffshoreWindLt100MW_MWh", 0)
            if record.get("OffshoreWindGe100MW_MWh", 0) is not None:
                val += record.get("OffshoreWindGe100MW_MWh", 0)
            record["production_wind_offshore"] = val
        if "CentralPower" in record or "DecentralPower" in record:
            val = 0
            if record.get("CentralPower", 0) is not None:
                val += record.get("CentralPower", 0)
            if record.get("DecentralPower", 0) is not None:
                val += record.get("DecentralPower", 0)
            record["production_thermal"] = val
        for f1, f2 in field_mappings.items():
            if f1 in record:
                val = record[f1]
                if val is None:
                    val = 0
                if f1.endswith("kWh") and val is not None:
                    val *= 1e-3
                record[f2] = val
        yield record


class EnergiDataServiceDk:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        
    def fetch_metadata(self):
        return fetch_metadata(self.descriptor)
    
    def fetch_data(self):
        return fetch(self.descriptor)
    