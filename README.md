# StatsCanPy

[![Unit Tests](https://github.com/deepwaterpaladin/statscanpy/actions/workflows/qa-tests.yml/badge.svg)](https://github.com/deepwaterpaladin/statscanpy/actions/workflows/qa-tests.yml)

[![Upload Python Package](https://github.com/deepwaterpaladin/statscanpy/actions/workflows/python-publish.yml/badge.svg)](https://github.com/deepwaterpaladin/statscanpy/actions/workflows/python-publish.yml)

Basic package for querying & downloading [StatsCan](https://www.statcan.gc.ca/en/start) data by table name. Saves data into a dataframe (`Pandas` or `PySpark`).

Allows for querying datasets via plain text search or table ID.

## Installation

`pip install statscanpy`

## Usage

### Basic Usage

```python
  from statscanpy import StatsCanPy
  statscan = StatsCanPy(path="/data/saved/here", isSpark=True)
```

### Getting Table ID from Table Name

```python
  await statscan.get_table_id_from_name("Household spending, Canada, regions and provinces")
```

### Getting Table Data from Table Name

```python
  await statscan.get_table_from_name("Household spending, Canada, regions and provinces")
```

### Searching for Table(s) by String

```python
  statscan.find_table_id_from_name("GDP", limit=15)
```

## Further Reading

- [StatsCan Data](https://www150.statcan.gc.ca/n1/en/type/data?MM=1)
- [StatsCan API](https://www.statcan.gc.ca/en/developers/wds/user-guide)
