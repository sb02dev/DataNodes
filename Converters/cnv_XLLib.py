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
"""def loadExcelTable(book, table_name, index):
    \"\"\"Reads an Excel Table object and returns the data as a Pandas DataFrame\"\"\"
    if book is not None:
        for ws in book.sheets:
            for t in ws.tables:
                if t.name==table_name:
                    data = t.range.options(pd.DataFrame, index=index).value
                    return data
    return None
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(
            f"{exporter.get_out_list(node, post=' = ')}loadExcelTable({', '.join(inpnames)})\n"
        )
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, DEFAULT_OUT_EXEC_NAME)


    @staticmethod
    def LoadExcelRange(exporter: 'PythonExporterImpl',
                       node: NodeBase,
                       inpnames: list[str],  # pylint: disable=unused-argument
                       *args, **kwargs):  # pylint: disable=unused-argument
        """Convert LoadExcelRange nodes"""
        # export function definition
        if not exporter.is_node_function_processed(node):
            exporter.add_import('xlwings', alias='xw')
            exporter.add_import('pandas', alias='pd')
            exporter.add_sys_function(
"""def loadExcelRange(book, sheet_name, range_name, num_header_rows, index, expand):
    \"\"\"Reads an Excel Range of which top-left cell or whole range
    is given and returns the data as a Pandas DataFrame\"\"\"
    if book is None:
        return None
    sh: xw.Sheet = book.sheets[sheet_name]
    if range_name and range_name not in ['None', 'UsedRange']:
        rng = sh.range(range_name)
    else:
        rng = sh.used_range
    if expand=='':
        expand=None
    return rng.options(pd.DataFrame, header=num_header_rows, index=index, expand=expand).value
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(
            f"{exporter.get_out_list(node, post=' = ')}loadExcelRange({', '.join(inpnames)})\n"
        )
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, DEFAULT_OUT_EXEC_NAME)


    @staticmethod
    def UpdateExcelTable(exporter: 'PythonExporterImpl',
                         node: NodeBase,
                         inpnames: list[str],  # pylint: disable=unused-argument
                         *args, **kwargs):  # pylint: disable=unused-argument
        """Convert UpdateExcelTable nodes"""
        # export function definition
        if not exporter.is_node_function_processed(node):
            exporter.add_import('typing', imports=['cast'])
            exporter.add_import('xlwings', alias='xw')
            exporter.add_import('pandas', alias='pd')
            exporter.add_sys_function(
"""def updateExcelTable(wb: xw.Book | None,
                     sheet_name: str,
                     table_name: str,
                     data: pd.DataFrame,
                     index: bool):
    \"\"\"Updates an Excel Table object with new data given as a Pandas DataFrame\"\"\"
    if wb is None:
        return
    if sheet_name in wb.sheet_names:
        ws = cast(xw.Sheet, wb.sheets[sheet_name])
        if table_name in [t.name for t in ws.tables]:
            t = cast(xw.main.Table, ws.tables[table_name])
        else:
            t = cast(xw.main.Table, ws.tables.add(source=ws['A1'], name=table_name))
    else:
        ws = cast(xw.Sheet, wb.sheets.add())
        ws.name = sheet_name
        t = cast(xw.main.Table, ws.tables.add(source=ws['A1'], name=table_name))
    t.update(data, index=index)
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(
            f"{exporter.get_out_list(node, post=' = ')}updateExcelTable({', '.join(inpnames)})\n"
        )
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, DEFAULT_OUT_EXEC_NAME)


    @staticmethod
    def RefreshExcelTable(exporter: 'PythonExporterImpl',
                         node: NodeBase,
                         inpnames: list[str],  # pylint: disable=unused-argument
                         *args, **kwargs):  # pylint: disable=unused-argument
        """Convert RefreshExcelTable nodes"""
        # export function definition
        if not exporter.is_node_function_processed(node):
            exporter.add_import('xlwings', alias='xw')
            exporter.add_import('pandas', alias='pd')
            exporter.add_sys_function(
"""def refreshExcelTable(wb: xw.Book | None, table_name: str):
    \"\"\"Refreshes an Excel Table object (if it is a Query object)\"\"\"
    if wb is None:
        return
    for ws in wb.sheets:
        for t in ws.tables:
            if t.name==table_name:
                bgquery = t.api.QueryTable.BackgroundQuery
                try:
                    t.api.QueryTable.BackgroundQuery = False
                    t.api.Refresh()
                finally:
                    t.api.QueryTable.BackgroundQuery = bgquery
                return
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(
            f"{exporter.get_out_list(node, post=' = ')}refreshExcelTable({', '.join(inpnames)})\n"
        )
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, DEFAULT_OUT_EXEC_NAME)
