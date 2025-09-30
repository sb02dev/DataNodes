"""Runs tests on the test-graphs"""
import pytest
import pandas as pd

from tests import testhelper  # pylint: disable=import-error


@pytest.mark.parametrize(
    ["test_name", "exec_pin", "result_pins_with_expected"],
    [
        ('sqlite_001_simple', 'GenericDBConn_inExec', [
            ('SQLQuery1_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
            ('SQLQuery2_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
            ('SQLQuery3_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
        ]),
        ('sqlite_002_getvalue', 'GenericDBConn_inExec', [
            ('GetValue_out', 'aaa')
        ]),
        ('sqlite_003_foreachrowdf', 'GenericDBConn_inExec', [
            ('getVar_out', ';0:aaa;1:bbb')
        ])
    ]
)
def test_sqlite_graph(pycnv, testfolder, test_name, exec_pin, result_pins_with_expected):
    """Runs the given graph and checks the given pin results against
    their expected values
    """
    gman = testhelper.run_graph_and_return(
        pycnv, testfolder, test_name, exec_pin)
    for pinname, value in result_pins_with_expected:
        res = gman.findPinByName(pinname).getData()
        if isinstance(res, pd.DataFrame):
            assert res.equals(value), \
                f"Wrong value ({value}) returned for pin '{pinname}'"
        else:
            assert res == value, \
                f"Wrong value ({value}) returned for pin '{pinname}'"


@pytest.mark.parametrize(
    ["test_name", "result_pins_with_expected"],
    [
        ('sqlite_001_simple', [
            ('SQLQuery1_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
            ('SQLQuery2_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
            ('SQLQuery3_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
        ]),
        ('sqlite_002_getvalue', [
            ('GetValue_out', 'aaa')
        ]),
        ('sqlite_003_foreachrowdf', [
            ('getVar_out', ';0:aaa;1:bbb')
        ])
    ]
)
def test_sqlite_cnv(pycnv, testfolder, test_name, result_pins_with_expected):
    """Converts the given graph to python, runs the python and checks the
    given pin results against their expected values
    """
    mem = testhelper.run_graph_in_python(pycnv, testfolder, test_name)
    # get output & assert
    for pinname, value in result_pins_with_expected:
        if isinstance(mem[pinname], pd.DataFrame):
            assert mem[pinname].equals(value), \
                f"Wrong value ({value}) returned for pin '{pinname}'"
        else:
            assert mem[pinname] == value, \
                f"Wrong value ({value}) returned for pin '{pinname}'"
