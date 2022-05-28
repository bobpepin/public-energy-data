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
    prefetch = next(iter(fetch_edh(descriptor, 2)))
    column_names = ["datetime_start", "datetime_end", "region_code"] + [x["id"] for x in prefetch["result"]["fields"]]
    column_names = [intern(x) for x in column_names]
    total = prefetch["result"]["total"]
    return {
        "field_names": column_names,
        "count": total
    }


def fetch(descriptor, chunk_size=8192):
    for chunk in fetch_edh(descriptor, chunk_size):
        names = [intern(x["id"]) for x in chunk["result"]["fields"]]
        for values in chunk["result"]["records"]:
            record = dict(zip(names, values))
            dt = datetime.datetime.fromisoformat(record["HourUTC"])
            record[intern("datetime_start")] = dt.isoformat()
            record[intern("datetime_end")] = (dt + datetime.timedelta(seconds=3600)).isoformat()
            municipality = record["MunicipalityNo"]
            record[intern("region_code")] = f"DK-mun-{municipality}"            
            yield format_record(record)


class EnergiDataServiceDk:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        
    def fetch_metadata(self):
        return fetch_metadata(self.descriptor)
    
    def fetch_data(self):
        return fetch(self.descriptor)
    