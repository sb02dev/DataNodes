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
        ]),
        ('sqlite_004_pandasupload', 'GenericDBConn_inExec', [
            ('SQLQuery1_result', pd.DataFrame(
                [[1, 'aaa'], [2, 'bbb']], columns=['id', 'name'])),
        ]),
        ('sqlite_005_pandassql', 'GenericDBConn_inExec', [
            ('PandasSQLQuery_result', pd.DataFrame(
                [[2]], columns=['count(*)'])),
        ]),
        ('sqlite_006_pandassqlparam', 'GenericDBConn_inExec', [
            ('PandasSQLQuery_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
        ]),
        ('sqlite_007_sqlparam', 'GenericDBConn_inExec', [
            ('SQLQuery1_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
        ]),
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
        ]),
        ('sqlite_004_pandasupload', [
            ('SQLQuery1_result', pd.DataFrame(
                [[1, 'aaa'], [2, 'bbb']], columns=['id', 'name'])),
        ]),
        ('sqlite_005_pandassql', [
            ('PandasSQLQuery_result', pd.DataFrame(
                [[2]], columns=['count(*)'])),
        ]),
        ('sqlite_006_pandassqlparam', [
            ('PandasSQLQuery_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
        ]),
        ('sqlite_007_sqlparam', [
            ('SQLQuery1_result', pd.DataFrame(
                [[1, 'aaa']], columns=['id', 'name'])),
        ]),
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


@pytest.mark.parametrize(
    ["test_name", "exec_pin", "result_pins_with_expected"],
    [
        ('excel_001_loadtable', 'OpenExcel_inExec', [
            ('LoadExcelTable_out', pd.DataFrame(
                [[1.0, 'aaa', 110.0],
                 [2.0, 'bbb', 120.0],
                 [3.0, 'ccc', 130.0]], columns=['id', 'name', 'value'])),
        ]),
        ('excel_002_loadrange', 'OpenExcel_inExec', [
            ('LoadExcelRange_out', pd.DataFrame(
                [[1.0, 'aaa'],
                 [2.0, 'bbb']], columns=['id', 'name'])),
        ]),
        ('excel_003_updatetable', 'OpenExcel_inExec', [
            ('LoadExcelTable_out', pd.DataFrame(
                [[1.0, 'aaa', 110.0],
                 [2.0, 'bbb', 120.0]], columns=['id', 'name', 'value'])),
        ]),
        ('excel_004_refreshtable', 'OpenExcel_inExec', [
            ('LoadExcelTable_out', pd.DataFrame(
                [[1.0, '1*'],
                 [2.0, '2**'],
                 [3.0, '3***']], columns=['id', 'name'])),
        ]),
    ]
)
def test_excel_graph(pycnv, testfolder, test_name, exec_pin, result_pins_with_expected):
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
        ('excel_001_loadtable', [
            ('LoadExcelTable_out', pd.DataFrame(
                [[1.0, 'aaa', 110.0],
                 [2.0, 'bbb', 120.0],
                 [3.0, 'ccc', 130.0]], columns=['id', 'name', 'value'])),
        ]),
        ('excel_002_loadrange', [
            ('LoadExcelRange_out', pd.DataFrame(
                [[1.0, 'aaa'],
                 [2.0, 'bbb']], columns=['id', 'name'])),
        ]),
        ('excel_003_updatetable', [
            ('LoadExcelTable_out', pd.DataFrame(
                [[1.0, 'aaa', 110.0],
                 [2.0, 'bbb', 120.0]], columns=['id', 'name', 'value'])),
        ]),
        ('excel_004_refreshtable', [
            ('LoadExcelTable_out', pd.DataFrame(
                [[1.0, '1*'],
                 [2.0, '2**'],
                 [3.0, '3***']], columns=['id', 'name'])),
        ]),
    ]
)
def test_excel_cnv(pycnv, testfolder, test_name, result_pins_with_expected):
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
