"""Excel (xlwings) Tools Nodes"""  # pylint: disable=invalid-name
from typing import cast
from PyFlow.Core.Common import NodeTypes, NodeMeta, PinSpecifiers, PinOptions
from PyFlow.Core import FunctionLibraryBase, IMPLEMENT_NODE

from ..constants import XLLIB_HEADER_COLOR

# pylint: disable=wrong-import-order
import xlwings as xw
import pandas as pd
# pylint: enable=wrong-import-order


class XLLib(FunctionLibraryBase):
    """Excel (xlwings) Tools Nodes FunctionLibrary"""

    def __init__(self, packageName):  # pylint: disable=useless-parent-delegation
        super().__init__(packageName)

    #######################
    ### File operations ###
    #######################
    @staticmethod
    @IMPLEMENT_NODE(returns=('XLWBookPin', None,  # type: ignore
                             { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'ExcelTools|File',
                        NodeMeta.KEYWORDS: ['excel', 'workbook', 'file', 'table'],
                        NodeMeta.HEADER_COLOR: XLLIB_HEADER_COLOR
                    })
    def OpenExcel(path=('StringPin', '')):  # pylint: disable=invalid-name
        """Opens an Excel Workbook with XLWings"""
        return xw.Book(path, update_links=False)


    @staticmethod
    @IMPLEMENT_NODE(returns=None,  # type: ignore
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'ExcelTools|File',
                        NodeMeta.KEYWORDS: ['excel', 'workbook', 'file'],
                        NodeMeta.HEADER_COLOR: XLLIB_HEADER_COLOR
    })
    def CloseExcel(book=('XLWBookPin', None,  # pylint: disable=invalid-name
                         { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable})):
        """Close an Excel Workbook"""
        if book is None:
            return
        book.get_value().close()


    # TODO: add other specific database connections

    ########################
    ### Table operations ###
    ########################

    @staticmethod
    @IMPLEMENT_NODE(returns=('DataFramePin', None,  # type: ignore
                             {PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable}),
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'ExcelTools|Tables',
                        NodeMeta.KEYWORDS: ['excel', 'workbook', 'file', 'table'],
                        NodeMeta.HEADER_COLOR: XLLIB_HEADER_COLOR
                    })
    def LoadExcelTable(book=('XLWBookPin', None,
                             { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                       table=('StringPin', ''),
                       index=('BoolPin', False)
                      ):
        """Reads an Excel Table object and returns the data as a Pandas DataFrame"""
        if book is None:
            return None
        for ws in book.get_value().sheets:
            for t in ws.tables:
                if t.name==table:
                    data = t.range.options(pd.DataFrame, index=index).value
                    return data
        return None


    @staticmethod
    @IMPLEMENT_NODE(returns=('DataFramePin', None,  # type: ignore
                             {PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable}),
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'ExcelTools|Tables',
                        NodeMeta.KEYWORDS: ['excel', 'workbook', 'file', 'table', 'range'],
                        NodeMeta.HEADER_COLOR: XLLIB_HEADER_COLOR
                    })
    def LoadExcelRange(book=('XLWBookPin', None,
                             { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                       sheet_name=('StringPin', ''),
                       range_name=('StringPin', ''),
                       num_header_rows=('IntPin', 1),
                       index=('BoolPin', False),
                       expand=('StringPin', '',
                               { PinSpecifiers.VALUE_LIST: ['', 'table', 'down', 'right']})
                       ):
        """Reads an Excel Range of which top-left cell or whole range
        is given and returns the data as a Pandas DataFrame"""
        if book is None:
            return None
        sh: xw.Sheet = book.get_value().sheets[sheet_name]
        if range_name and range_name not in ['None', 'UsedRange']:
            rng = sh.range(range_name)
        else:
            rng = sh.used_range
        if expand=='':
            expand=None
        return rng.options(pd.DataFrame, header=num_header_rows, index=index, expand=expand).value


    @staticmethod
    @IMPLEMENT_NODE(returns=None,  # type: ignore
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'ExcelTools|Tables',
                        NodeMeta.KEYWORDS: ['excel', 'workbook', 'file', 'table', 'range', 'update', 'write'],
                        NodeMeta.HEADER_COLOR: XLLIB_HEADER_COLOR
                    })
    def UpdateExcelTable(book=('XLWBookPin', None,
                               { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                         sheet_name=('StringPin', ''),
                         table_name=('StringPin', ''),
                         data=('DataFramePin', '',
                               { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                         index=('BoolPin', False)
                         ):
        """Updates an Excel Table object with new data given as a Pandas DataFrame"""
        if book is None:
            return
        wb = cast(xw.Book, book.get_value())
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


    @staticmethod
    @IMPLEMENT_NODE(returns=None,  # type: ignore
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'ExcelTools|Tables',
                        NodeMeta.KEYWORDS: ['excel', 'workbook', 'file', 'table', 'range', 'refresh'],
                        NodeMeta.HEADER_COLOR: XLLIB_HEADER_COLOR
                    })
    def RefreshExcelTable(book=('XLWBookPin', None,
                               { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                         table_name=('StringPin', '')
                         ):
        """Refreshes an Excel Table object (if it is a Query object)"""
        if book is None:
            return
        wb = cast(xw.Book, book.get_value())
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
