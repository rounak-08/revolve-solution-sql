import pytest
from .solution_start import initilize_spark

def test_initilize_spark():
    val = initilize_spark()
    assert val != None
