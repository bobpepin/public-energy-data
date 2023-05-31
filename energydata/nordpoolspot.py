import urllib.request
import json
import re
import datetime
from sys import intern
import pytz

def fetch_date(date, entity):
    date_str = datetime.datetime.fromisoformat(date).strftime("%d-%m-%Y")
    url = f"https://www.nordpoolgroup.com/api/marketdata/page/194?currency=,EUR,EUR,EUR&endDate={date_str}&entityName={entity}"
    response = urllib.request.urlopen(url)
    data = json.load(response)
    return data

def extract_rows(data):
    rows = []
    for row in data["data"]["Rows"]:
        if row["IsExtraRow"] or row["Parent"] is not None:
            continue
        row_dict = {}
        for column in row["Columns"]:
            if column["Name"] == "":
                continue
            row_dict[column["Name"]] = column["Value"]
        if len(row_dict) > 0:
            rows.append(row_dict)    
    return rows

def extract_dates(product_str):
    tz = pytz.timezone("Europe/Copenhagen")
    date_match = re.match(r"PH-(\d{8})-(\d{2})(X?)", product_str)
    date_str, hour_str, dst_flag = date_match.groups()
    is_dst = (dst_flag != "X")
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    start_time = datetime.time(hour=int(hour_str)-1)
    dt = datetime.datetime.combine(date, start_time)
    start_loc = tz.localize(dt, is_dst=is_dst) # is_dst is ignored unless there is ambiguity
    end_loc = (start_loc.astimezone(datetime.timezone.utc) + datetime.timedelta(hours=1)).astimezone(tz)
    return start_loc.isoformat(), end_loc.isoformat()

numerical_columns = ["High", "Low", "Last", "Avg", "Volume"]

def format_row(row):
    record = {}
    dt_start, dt_end = extract_dates(row["Product"])
    record["start_datetime"] = dt_start
    record["end_datetime"] = dt_end
    for col, val in row.items():
        if col in numerical_columns:
            val = float(val.replace(",", "."))
        record[intern(col)] = val
    return record

class NordpoolSpot:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        if descriptor.get("dataset") != "intraday":
            raise ValueError('descriptor["dataset"] needs to have value "intraday"')
        step = datetime.timedelta(days=1)
        start_date = datetime.datetime.fromisoformat(descriptor["start_datetime"])
        end_date = datetime.datetime.fromisoformat(descriptor["end_datetime"])
        dates = [start_date + i*step for i in range(int((end_date - start_date)/step)+1)]
        self.request_dates = [date.isoformat() for date in dates]
        self.zone = descriptor["zone"]
        
    def fetch_metadata(self):
        return {
            "fields": ["datetime_start", "datetime_end", "Product"] + numerical_columns
        }
    
    def fetch_data(self):
        for request_date in self.request_dates:
            data = fetch_date(request_date, self.zone)
            rows = extract_rows(data)
            for row in rows:
                record = format_row(row)
                record["zone"] = self.zone
                yield record