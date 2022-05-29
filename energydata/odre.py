import urllib.request
import urllib.parse
import json
import datetime
from sys import intern
import pytz

physical_timesteps = {
    "1/4 Horaire": 15*60,
    "1/2 Horaire": 30*60,
    "Horaire": 60*60,
    "Journalier": 24*60*60
}

def compute_end_datetime(start_datetime, dataset_info):
    time_step_name = dataset_info["metas"]["pas-temporel"]
    if time_step_name in physical_timesteps:
        delta = datetime.timedelta(seconds=physical_timesteps[time_step_name])
        return start_datetime + delta
    elif time_step_name == "Mensuel":
        if start_datetime.month == 12:
            return start_datetime.replace(year=start_datetime.year+1, month=1)
        else:
            return start_datetime.replace(month=start_datetime.month+1)
    elif time_step_name == "Trimestriel":
        if start_datetime.month >= 9:
            return start_datetime.replace(year=start_datetime.year+1, month=(start_datetime.month + 4 - 12))
        else:
            return start_datetime.replace(month=start_datetime.month+4)
    elif time_step_name == "Annuel":
        return start_datetime.replace(year=start_datetime.year+1)
    else:
        raise ValueError(f"Unsupported time step: {time_step_name!r}")

def fetch_dataset_info(descriptor):
    dataset = descriptor["dataset"]
    url = f"https://odre.opendatasoft.com/api/datasets/1.0/{dataset}/"
    with urllib.request.urlopen(url) as response:
        data = json.load(response)
    return data

def extract_metadata(dataset_info):
    metadata = {
        "fields": dataset_info["fields"],
        "dataset_records_count": dataset_info["metas"]["records_count"]
    }
    return metadata

def iter_months(start_datetime, end_datetime):
    date = datetime.datetime(start_datetime.year, start_datetime.month, 1)
    while date < end_datetime:
        yield date.date()
        if date.month < 12:
            date = date.replace(month=date.month+1)
        else:
            date = date.replace(year=date.year+1, month=1)


field_mappings = {
    'pompage': "consumption_phes",
    'bioenergies': "production_biomass",
    'thermique': "production_thermal",
    'eolien': "production_wind",
    'solaire': "production_solar",
    'consommation': "consumption",
    'nucleaire': "production_nuclear",
    'ech_physiques': "import",
    'hydraulique': "production_hydro",
    "stockage_batterie": "consumption_battery",
    "destockage_batterie": "production_battery",
    "eolien_terrestre": "production_wind_onshore",
    "eolien_offshore": "production_wind_offshore"
}

def format_record(record_info, dataset_info):
    record = {intern(k): v for k, v in record_info["fields"].items()}
    record[intern("recordid")] = record_info["recordid"]
    record[intern("record_timestamp")] = record_info["record_timestamp"]
    if "date_heure" in record and "date" in record and "heure" in record:
        utc_datetime = datetime.datetime.fromisoformat(record["date_heure"].replace("Z", ""))
        try:
            local_date = datetime.datetime.strptime(record["date"], "%d/%m/%Y").date()
        except ValueError:
            local_date = datetime.datetime.strptime(record["date"], "%Y-%m-%d").date()
        local_time = datetime.datetime.strptime(record["heure"], "%H:%M").time()
        local_datetime = datetime.datetime.combine(local_date, local_time)
        tz_offset = datetime.timezone(local_datetime - utc_datetime)
        start_datetime = local_datetime.replace(tzinfo=tz_offset)
        end_datetime = compute_end_datetime(start_datetime, dataset_info)
        record[intern("start_datetime")] = start_datetime.isoformat()
        record[intern("end_datetime")] = end_datetime.isoformat()
    elif "date" in record:
        local_datetime = datetime.datetime.fromisoformat(record["date"])
        if "heure" in record:
            local_time = datetime.datetime.strptime(record["heure"], "%H:%M").time()
            local_date = local_datetime.date()
            local_datetime = datetime.datetime.combine(local_date, local_time)
        tz = pytz.timezone(dataset_info["metas"]["timezone"])
        start_datetime = tz.localize(local_datetime)
        end_datetime = tz.localize(compute_end_datetime(local_datetime, dataset_info))
        record[intern("start_datetime")] = start_datetime.isoformat()
        record[intern("end_datetime")] = end_datetime.isoformat()        
    if "code_insee_region" in record:
        code = record["code_insee_region"]
        record["region_code"] = f"fr_reg_{code}"
    for f1, f2 in field_mappings.items():
        if f1 in record:
            record[f2] = record[f1]
    return record

def fetch_odre_dt_facet(descriptor, date_heure, dataset_info):
    base_url = "https://odre.opendatasoft.com/api/records/1.0/download/"
    parameters = {
        "dataset": descriptor["dataset"],
        "format": "json",
        "refine.date_heure": date_heure,
        "format": "json"
    }
    for k, v in descriptor.get("refine", {}).items():
        parameters[f"refine.{k}"] = v
    url = base_url + "?" + urllib.parse.urlencode(parameters, doseq=True)
    with urllib.request.urlopen(url) as response:
        data = json.load(response)
        for record in data:
            yield format_record(record, dataset_info)
    
def fetch_odre(descriptor):
    dataset_info = fetch_dataset_info(descriptor)
    start_datetime = datetime.datetime.fromisoformat(descriptor["start_datetime"])
    end_datetime = datetime.datetime.fromisoformat(descriptor["end_datetime"])
    months = list(iter_months(start_datetime, end_datetime))
    for month in months:
        dt_facet = month.strftime("%Y/%m")
        yield from fetch_odre_dt_facet(descriptor, dt_facet, dataset_info)
        

class ODREDataset:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        
    def fetch_records(self):
        return fetch_odre(self.descriptor)
    
    def fetch_metadata(self):
        dataset_info = fetch_dataset_info(self.descriptor)
        return extract_metadata(dataset_info)