import urllib.request
import json
import re
import datetime
from sys import intern
import pytz


def fetch_esett(descriptor):
    start_dt = datetime.datetime.fromisoformat(descriptor["start_datetime"])
    start_str = start_dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_dt = datetime.datetime.fromisoformat(descriptor["end_datetime"])
    end_str = end_dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    query = urllib.parse.urlencode({
        "start": start_str,
        "end": end_str,
        "MBA": descriptor["MBA"]
    })
    url = f"https://opendata.esett.com/api/EXP14/{descriptor['dataset']}?{query}"    
    with urllib.request.urlopen(url) as fh:
        data = json.load(fh)
    return data


class ESett:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.data = None
        
    def fetch_metadata(self):
        self.cache_esett()
        total = len(self.data)
        if total == 0:
            return {fields: [], total: 0}
        else:
            return {
                "fields": ["datetime_start", "datetime_end"] + list(self.data[0].keys()),
                "total": total
            }
    
    def cache_esett(self):
        if self.data is None:
            self.data = fetch_esett(self.descriptor)

    def fetch_data(self):
        self.cache_esett()
        tz = pytz.timezone("Europe/Copenhagen")
        prev_timestamp = ""
        for row in self.data:
            record = {intern(k): v for k, v in row.items()}
            timestamp = row["timestamp"]
            is_dst = timestamp != prev_timestamp
            dt = datetime.datetime.fromisoformat(timestamp)
            start_loc = tz.localize(dt, is_dst=is_dst)
            end_loc = (start_loc.astimezone(datetime.timezone.utc) + datetime.timedelta(hours=1)).astimezone(tz)
            record[intern("datetime_start")] = start_loc.isoformat()
            record[intern("datetime_end")] = end_loc.isoformat()
            prev_timestamp = timestamp
            yield record