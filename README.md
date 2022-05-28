# public-energy-data
Unified Python API for accessing publicly available data from the energy sector

## Principles:
Descriptors should be JSON serializeable
Records shoilds be JSON serializeable
Unified API as much as possible
Add columns with standardized column names and units
All dates and times in local time, iso format with offset from UTC
Time intervals are inclusive start datetime, exclusive end datetime
Standard column names in lower case, words separated by underscores

## Notes
Transform local datetimes into UTC for doing arithmetic
