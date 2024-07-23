# StatsCanPy

Basic package for querying & downloading [StatsCan](https://www.statcan.gc.ca/en/start) data by table name. Saves data into a dataframe (`Pandas` or `PySpark`).

## Installation

`pip install statscanpy`

## Usage

### Basic Usage

```python
  from statscanpy.StatsCanPy import StatsCanPy
  statscan = StatsCanPy()
```

### Getting Table from Table Name

```python
  sc.get_table_id_from_name("Gross domestic product (GDP) at basic prices, by industry, monthly, growth rates", limit=15)
```

### Searching for Table(s) by String

```python
  sc.find_table_id_from_name("GDP", limit=15)
```

## Further Reading

- [StatsCan Data](https://www150.statcan.gc.ca/n1/en/type/data?MM=1)
- [StatsCan API](https://www.statcan.gc.ca/en/developers/wds/user-guide)
