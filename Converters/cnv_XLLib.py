"""Converters for the XLLib"""  # pylint: disable=invalid-name
from typing import TYPE_CHECKING

from PyFlow.Core import NodeBase
from PyFlow.Core.Common import DEFAULT_OUT_EXEC_NAME

try:
    from PyFlow.Packages.PythonExporter.Exporters.converter_base import (  # pylint: disable=import-error, no-name-in-module # type: ignore
        ConverterBase
    )
except ImportError:
    from PythonExporter.Exporters.converter_base import ConverterBase

if TYPE_CHECKING:
    from PythonExporter.Exporters.implementation import PythonExporterImpl


class PyCnvXLLib(ConverterBase):  # type: ignore
    """A converter class for the XLLib conversion"""

    #######################
    ### File operations ###
    #######################

    @staticmethod
    def OpenExcel(exporter: 'PythonExporterImpl',
                  node: NodeBase,
                  inpnames: list[str],  # pylint: disable=unused-argument
                  *args, **kwargs):  # pylint: disable=unused-argument
        """Convert OpenExcel node type"""
        if not exporter.is_node_function_processed(node):
            exporter.add_import('xlwings', alias='xw')
            exporter.add_sys_function("""def openExcel(path):
    \"\"\"Open an Excel file with xlwings\"\"\"
    return xw.Book(path, update_links=False)
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(f"{exporter.get_out_list(node, post=' = ')}" + \
                          f"openExcel({', '.join(inpnames)})\n")
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, 'outExec')


    @staticmethod
    def CloseExcel(exporter: 'PythonExporterImpl',
                   node: NodeBase,
                   inpnames: list[str],  # pylint: disable=unused-argument
                   *args, **kwargs):  # pylint: disable=unused-argument
        """Convert CloseExcel node type"""
        if not exporter.is_node_function_processed(node):
            exporter.add_import('xlwings', alias='xw')
            exporter.add_sys_function("""def closeExcel(book):
    \"\"\"Close an xlwings workbook\"\"\"
    if book is not None:
        book.close()
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(f"{exporter.get_out_list(node, post=' = ')}" +
                          f"closeExcel({', '.join(inpnames)})\n")
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, 'outExec')


    ########################
    ### Table operations ###
    ########################

    @staticmethod
    def LoadExcelTable(exporter: 'PythonExporterImpl',
                       node: NodeBase,
                       inpnames: list[str],  # pylint: disable=unused-argument
                       *args, **kwargs):  # pylint: disable=unused-argument
        """Convert LoadExcelTable nodes"""
        # export function definition
        if not exporter.is_node_function_processed(node):
            exporter.add_import('xlwings', alias='xw')
            exporter.add_import('pandas', alias='pd')
            exporter.add_sys_function(
"""def loadExcelTable(book, table_name):
    \"\"\"Reads an Excel Table object and returns the data as a Pandas DataFrame\"\"\"
    if book is not None:
        for ws in book.sheets:
            for t in ws.tables:
                if t.name==table_name:
                    data = t.range.options(pd.DataFrame, index=False).value
                    return data
    return None
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(
            f"{exporter.get_out_list(node, post=' = ')}loadExcelTable({', '.join(inpnames)})\n"
        )
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, DEFAULT_OUT_EXEC_NAME)
