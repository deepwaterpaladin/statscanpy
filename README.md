# StatsCanPy
Basic package for accessing StatsCan data.

## Installation
`pip install statscanpy`

## Usage

### Basic Usage
```
  from statscanpy.StatsCanPy import StatsCanPy
  statscan = StatsCanPy()
```

### Getting Table from Table Name
```
  sc.get_table_id_from_name("Gross domestic product (GDP) at basic prices, by industry, monthly, growth rates", limit=15)
```

### Searching for Table(s) by String
```
  sc.find_table_id_from_name("GDP", limit=15)
```
