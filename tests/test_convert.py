"""Runs tests on the test-graphs"""
import pytest
from tests import testhelper  # pylint: disable=import-error

@pytest.mark.parametrize("test_name", testhelper.get_test_names('conn'))
def test_conn(pycnv, testfolder, test_name):
    """Tests all graphs from the parameters"""
    testhelper.run_export_and_compare(pycnv, testfolder, "conn_"+test_name)
