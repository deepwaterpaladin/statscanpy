import pytest
from pyspark.sql import DataFrame
from statscanpy.StatsCanPy import StatsCanPy

@pytest.fixture
def stats_can():
    '''Fixture to initialize StatsCanPy instance with test data path.'''
    return StatsCanPy(path='./test_data', isSpark=False)

@pytest.fixture
def spark_stats_can():
    '''Fixture to initialize StatsCanPy instance with test data path.'''
    return StatsCanPy(path='./test_data', isSpark=True)

@pytest.fixture
def test_table_name():
    return "Chartered banks, foreign currency assets and liabilities, at month-end, Bank of Canada"

def test_initialization_default():
    '''Test default initialization of StatsCanPy.'''
    instance = StatsCanPy()
    assert instance.path == './temp'
    assert instance.isSpark is True

def test_initialization_custom():
    '''Test custom initialization of StatsCanPy.'''
    instance = StatsCanPy(path='/custom/path', isSpark=False)
    assert instance.path == '/custom/path'
    assert instance.isSpark is False

def test_get_table_id_from_name(stats_can, test_table_name):
    '''
    Test retrieving table ID by table name.

    Ensures that a valid table ID is returned for an existing table name.
    '''
    table_id = stats_can.get_table_id_from_name(test_table_name)
    assert isinstance(table_id, str)


@pytest.mark.asyncio
async def test_get_table_from_name_as_panda(stats_can, test_table_name):
    '''
    Test retrieving a table as a Pandas DataFrame by table name.

    Ensures that a DataFrame is returned for a valid table ID.
    '''
    table_id = stats_can.get_table_id_from_name(test_table_name)
    if isinstance(table_id, str):
        df = await stats_can.get_table_from_name(test_table_name)
        assert df is not None

@pytest.mark.asyncio
async def test_get_table_from_name_as_spark(spark_stats_can, test_table_name):
    '''
    Test retrieving a table as a PySpark DataFrame by table name.

    Ensures that a DataFrame is returned for a valid table ID.
    '''
    
    
    df = await spark_stats_can.get_table_from_name(test_table_name)
    assert df is not None
    assert isinstance(df, DataFrame)

@pytest.mark.asyncio
async def test_get_table_id_from_name_pandas(stats_can, test_table_name):
    '''
    Test asserting valid table ID is found.
    '''
    table_id = stats_can.get_table_id_from_name(test_table_name)
    assert isinstance(table_id, str)

@pytest.mark.asyncio
async def test_get_table_id_from_name_spark(spark_stats_can, test_table_name):
    '''
    Test asserting valid table ID is found.
    '''
    table_id = spark_stats_can.get_table_id_from_name(test_table_name)
    assert isinstance(table_id, str)

@pytest.mark.asyncio
async def test_get_table_from_name_no_match_pandas(stats_can):
    '''Test finding table IDs for a nonexistent table name.'''
    with pytest.raises(Exception):
        await stats_can.get_table_from_name("nonexistent_table")

@pytest.mark.asyncio
async def test_get_table_from_name_no_match_spark(spark_stats_can):
    '''Test finding table IDs for a nonexistent table name.'''
    with pytest.raises(Exception):
        await spark_stats_can.get_table_from_name("nonexistent_table")

def test_filesystem_permissions():
    '''Test handling of inaccessible paths.'''
    with pytest.raises(PermissionError):
        StatsCanPy(path='/restricted_path', isSpark=False)