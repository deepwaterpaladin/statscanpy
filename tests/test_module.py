from statscanpy import StatsCanPy
import pytest


def test_isSpark_is_true():
    sc = StatsCanPy("temp")
    assert sc.isSpark == True


def test_isSpark_is_false():
    sc1 = StatsCanPy("temp", False)
    assert sc1.isSpark == False
