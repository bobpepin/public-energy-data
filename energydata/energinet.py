import urllib.request
import json
import re
import datetime
from collections import namedtuple
from sys import intern
import pytz


def fetch_edh(descriptor, limit, offset=0):
    resource_id = descriptor["resource_id"]
    url = f"https://api.energidataservice.dk/datastore_search?resource_id={resource_id}&limit={limit}&records_format=lists&offset={offset}"
    while True:
        fileobj = urllib.request.urlopen(url)
        data = json.load(fileobj)
        records = data["result"]["records"]
        if len(records) == 0:
            break
        yield data
        url = "https://api.energidataservice.dk" + data["result"]["_links"]["next"]

        
def fetch_metadata(descriptor):
    resource_id = descriptor["resource_id"]
    url = f"https://api.energidataservice.dk/datastore_search?resource_id={resource_id}&limit=0&records_format=lists&offset=0"
    with urllib.request.urlopen(url) as fileobj:
        data = json.load(fileobj)
    column_names = [x["id"] for x in data["result"]["fields"]]
    if "HourUTC" in column_names:
        column_names += ["datetime_start", "datetime_end"]
    if "MunicipalityNo" in column_names:
        column_names += ["region_code"]
    column_names = [intern(x) for x in column_names]
    total = data["result"]["total"]
    return {
        "fields": column_names,
        "count": total
    }


def fetch(descriptor, chunk_size=8192):
    tz = pytz.timezone("Europe/Copenhagen")
    for chunk in fetch_edh(descriptor, chunk_size):
        names = [intern(x["id"]) for x in chunk["result"]["fields"]]
        for values in chunk["result"]["records"]:
            record = dict(zip(map(intern, names), values))
            if "HourUTC" in record:
                dt = datetime.datetime.fromisoformat(record["HourUTC"]).replace(tzinfo=datetime.timezone.utc)
                record[intern("datetime_start")] = dt.astimezone(tz).isoformat()
                record[intern("datetime_end")] = (dt + datetime.timedelta(seconds=3600)).astimezone(tz).isoformat()
            if "MunicipalityNo":
                municipality = record["MunicipalityNo"]
                record[intern("region_code")] = f"dk_mun_{municipality}"            
            yield record


class EnergiDataServiceDk:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        
    def fetch_metadata(self):
        return fetch_metadata(self.descriptor)
    
    def fetch_data(self):
        return fetch(self.descriptor)
    