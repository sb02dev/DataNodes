"""Database Tools Nodes"""  # pylint: disable=invalid-name
from PyFlow.Core.Common import NodeTypes, NodeMeta, PinSpecifiers, PinOptions
from PyFlow.Core import FunctionLibraryBase, IMPLEMENT_NODE
from sqlalchemy.engine import create_engine, URL

from ..constants import DB_HEADER_COLOR, PDLIB_HEADER_COLOR

# pylint: disable=wrong-import-order
import pandas as pd
# pylint: enable=wrong-import-order


class DBLib(FunctionLibraryBase):
    """Database Tools Nodes FunctionLibrary"""

    def __init__(self, packageName):  # pylint: disable=useless-parent-delegation
        super().__init__(packageName)

    ############################
    ### Database connections ###
    ############################
    @staticmethod
    @IMPLEMENT_NODE(returns=('DBEnginePin', None),  # type: ignore
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'DatabaseTools|Server',
                        NodeMeta.KEYWORDS: ['Database', 'DB', 'Server', 'SQL'],
                        NodeMeta.HEADER_COLOR: DB_HEADER_COLOR
                    })
    def SQLServerConn(db_host=('StringPin', ''),  # pylint: disable=invalid-name
                     db_name=('StringPin', ''),
                     trusted_conn=('BoolPin', False)):
        """Create a DB Connection to be used in query nodes"""
        engine = create_engine(
            URL.create('mssql+pyodbc', query={
                'odbc_connect': f"Driver=SQL Server;Server={db_host};" + \
                                f"Database={db_name};" + \
                                f"Trusted_Connection={'yes' if trusted_conn else 'no'};"}),
                fast_executemany=False,
                use_setinputsizes=False
        )
        return engine


    @staticmethod
    @IMPLEMENT_NODE(returns=('DBEnginePin', None),  # type: ignore
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'DatabaseTools|Server',
                        NodeMeta.KEYWORDS: ['Database', 'DB', 'Server', 'SQL'],
                        NodeMeta.HEADER_COLOR: DB_HEADER_COLOR
    })
    def GenericDBConn(connection_url=('StringPin', '')):  # pylint: disable=invalid-name
        """Create a DB Connection to be used in query nodes"""
        engine = create_engine(connection_url)
        return engine


    # TODO: add other specific database connections

    ####################
    ### Data loaders ###
    ####################

    @staticmethod
    @IMPLEMENT_NODE(returns=('DataFramePin', None,  # type: ignore
                             {PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable}),
                    nodeType=NodeTypes.Callable,
                    meta={
                        NodeMeta.CATEGORY: 'DatabaseTools|Pandas',
                        NodeMeta.KEYWORDS: [],
                        NodeMeta.HEADER_COLOR: PDLIB_HEADER_COLOR
                    })
    def ReadCSV(path=('StringPin', None),
                delimiter=('StringPin', ';'),
                decimal=('StringPin', '.')
               ):
        """Reads a CSV into a Pandas DataFrame"""
        return  pd.read_csv(path, delimiter=delimiter, decimal=decimal)


    ############################
    ###  Value helper nodes  ###
    ############################

    @staticmethod
    @IMPLEMENT_NODE(returns=('AnyPin', None),  # type: ignore
                    nodeType=NodeTypes.Pure,
                    meta={
                        NodeMeta.CATEGORY: 'DatabaseTools|Pandas',
                        NodeMeta.KEYWORDS: [],
                        NodeMeta.HEADER_COLOR: PDLIB_HEADER_COLOR,
                    })
    def GetValue(df=('DataFramePin', None,
                     { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                 to_locate=('AnyPin', '<value>'),
                 column=('StringPin', '<columnname>')
                ):
        """Gets a value from Pandas DataFrame (locate by index field
        and return value of a specific column)"""
        if not isinstance(df, pd.DataFrame):
            return None
        return df.loc[to_locate][column]


    @staticmethod
    @IMPLEMENT_NODE(returns=('AnyPin', None),  # type: ignore
                    nodeType=NodeTypes.Pure,
                    meta={
                        NodeMeta.CATEGORY: 'DatabaseTools|Pandas',
                        NodeMeta.KEYWORDS: [],
                        NodeMeta.HEADER_COLOR: PDLIB_HEADER_COLOR,
                    })
    def GetSeriesValue(series=('SeriesPin', None,
                               { PinSpecifiers.DISABLED_OPTIONS: PinOptions.Storable }),
                       to_locate=('AnyPin', '<value|columnname>')):
        """Gets a value from Pandas Series (locate by index field)"""
        if not isinstance(series, pd.Series):
            return None
        return series.loc[to_locate]


    @staticmethod
    @IMPLEMENT_NODE(returns=('BoolPin', False),  # type: ignore
                    nodeType=NodeTypes.Pure,
                    meta={
                        NodeMeta.CATEGORY: 'DatabaseTools|Pandas',
                        NodeMeta.KEYWORDS: [],
                        NodeMeta.HEADER_COLOR: PDLIB_HEADER_COLOR,
                    })
    def pdisnull(value=('AnyPin', None)):
        """Returns pandas.isnull() for the value"""
        return pd.isnull(value)
