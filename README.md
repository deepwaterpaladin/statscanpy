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

  # if isSpark==True, data returns will be in PySpark; otherwise it will return as a pandas.DataFrame
  statscan = StatsCanPy(path="/data/saved/here", isSpark=True)
```

### Getting Table ID from Table Name

```python
  statscan.get_table_id_from_name("Railway industry operating statistics by mainline companies")
  >>> TOP MATCH:
      Railway industry operating statistics by mainline companies: 23-10-0055-01
      Accessible at: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2310005501
```

### Getting Table Data from Table Name

```python
  await statscan.get_table_from_name("Household spending, Canada, regions and provinces")
  >>> TOP MATCH:
      Household spending, Canada, regions and provinces: 11-10-0222-01
      Accessible at: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1110022201
      DataFrame[REF_DATE: date, GEO: string, ...]
```

### Searching for Table(s) by String

```python
  statscan.find_table_id_from_name("GDP", limit=15)
  >>> TOP 15 MATCHES:
      1. Gross domestic product (GDP) at basic prices, by industry, monthly, growth rates: 36-10-0434-02
      Accessible at: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610043402
      2. Gross domestic product, expenditure-based, provincial and territorial, annual: 36-10-0222-01
      Accessible at: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3610022201
      ...
```

## Further Reading

- [StatsCan Data](https://www150.statcan.gc.ca/n1/en/type/data?MM=1)
- [StatsCan API](https://www.statcan.gc.ca/en/developers/wds/user-guide)
