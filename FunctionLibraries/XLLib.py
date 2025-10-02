"""Excel (xlwings) Tools Nodes"""  # pylint: disable=invalid-name
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
                      ):
        """Reads an Excel Table object and returns the data as a Pandas DataFrame"""
        if book is None:
            return None
        for ws in book.get_value().sheets:
            for t in ws.tables:
                if t.name==table:
                    data = t.range.options(pd.DataFrame, index=False).value
                    return data
        return None
