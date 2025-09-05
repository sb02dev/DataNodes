"""Runs tests on the test-graphs"""
import pytest
from tests import testhelper  # pylint: disable=import-error

@pytest.mark.parametrize("test_name", testhelper.get_test_names('whatever'))
def test_whatever(pycnv, testfolder, test_name):
    """Tests all graphs from the parameters"""
    testhelper.run_export_and_compare(pycnv, testfolder, "whatever_"+test_name)
