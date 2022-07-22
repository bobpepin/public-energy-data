# public-energy-data

This package provides a unified Python API to access publicly available data from TSOs and similar organizations. 

The goal is to enable the development of open source data science tools that work across countries and providers.


Example
=======
API: 
```
import energydata.odre

descriptor = {
    "dataset": "consommation-quotidienne-brute-regionale",
    "start_datetime": "2018-03-01",
    "end_datetime": "2018-04-01"
}
dataset = energydata.odre.ODREDataset(descriptor)
record_iter = dataset.fetch_records()
records = list(record_iter)
```

See the notebooks in the `notebooks` folder for more examples.


Datasets
========

Implemented
----------------
- France Regions Consumption (`odre` module)
- Energinet Municipalities Consumption (`energinet` module)
- Energinet Municipalities Production
- France Regions (?) Production
- Energinet Network Topology (`energinet_netdata` module)

In Progress (Notebooks)
-----------------------------
- Nordpool
- EPEX


Planned
----------
- DMI
- ODRE Network topology (electricity & gas)
- German MaST (?)
- ENTSO-E
- Danmarks Statistic
- France Population 
- France Climate
- World Bank Energy Networks / Data
- US Data State-level production and consumption
- US population
- US climate
- US grid topology
- Mineral prices (Lithum, Copper, Cobalt)
- Mineral recycling


Design Principles
=========


Standard column names:
- region_code, format: `<ISO country code, lowercase>_<region type>_<region identifier>`
- start_datetime: start of time interval on which data are aggregated, local time in ISO format with timezone offset
- end_datetime: end of time interval on which data are aggregated, local time in ISO format with timezone offset

Design principles:
- All records should be JSON serializable.
- All dates and times in local time, iso format with offset from UTC
- Time intervals are inclusive start datetime, exclusive end datetime
- Standard column names in English, lower case, words separated by underscores


## Notes
Transform local datetimes into UTC for doing arithmetic
