"""Database Tools Nodes"""
from PyFlow.Core.Common import NodeTypes, NodeMeta
from PyFlow.Core import FunctionLibraryBase, IMPLEMENT_NODE
from sqlalchemy.engine import create_engine, URL
from ..constants import DB_HEADER_COLOR

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
