Principles
=========

identifiers are English, lowercase, separated by underscores

Schema: relation name, column names, column metadata
Data: generator, yielding {relation name: namedtuple of values}

API: 
```
dataset = energydata.odre.ODREDataset(descriptor)
record_iter = dataset.fetch_records()
records = list(record_iter)
```

Column names:
- region_code, format: `<ISO country code, lowercase>_<region type>_<region identifier>`
- start_datetime: start of time interval on which data are aggregated, local time in ISO format with timezone offset
- end_datetime: end of time interval on which data are aggregated, local time in ISO format with timezone offset


Datasets
========
- France Regions Consumption (`odre` mnodule)
- Energinet Municipalities Consumption (`energinet` module)


TODO
-------

- DMI
- Energinet Municipalities Production
- France Regions (?) Production
- ODRE Network topology (electricity & gas)
- German MaST (?)
- ENTSO-E
- Energinet Network Topology
- Danmarks Statistic
- France Population 
- France Climate
- World Bank Energy Networks / Data
- Nordpool
- EPEX
- US Data State-level production and consumption
- US population
- US climate
- US grid topology