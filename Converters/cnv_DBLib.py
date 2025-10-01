"""Converters for the DBLib"""  # pylint: disable=invalid-name
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


class PyCnvDBLib(ConverterBase):  # type: ignore
    """A converter class for the DBLib conversion"""

    ############################
    ### Database connections ###
    ############################

    @staticmethod
    def SQLServerConn(exporter: 'PythonExporterImpl',
                 node: NodeBase,
                 inpnames: list[str],  # pylint: disable=unused-argument
                 *args, **kwargs):  # pylint: disable=unused-argument
        """Convert SQLServerConn node type"""
        if not exporter.is_node_function_processed(node):
            exporter.add_import('sqlalchemy', imports=['create_engine', 'URL'])
            exporter.add_sys_function("""def connect_sqlserver(db_host, db_name, trusted_conn):
    \"\"\"Connect to an SQL server according to the parameters\"\"\"
    engine = create_engine(
        URL.create('mssql+pyodbc', query={'odbc_connect':
            f"Driver=SQL Server;Server={db_host};" + \\
            f"Database={db_name};" + \\
            f"Trusted_Connection={'yes' if trusted_conn else 'no'};"}
        ),
        fast_executemany=False,
        use_setinputsizes=False
    )
    return engine""")
            exporter.set_node_function_processed(node)
        exporter.add_call(f"{exporter.get_out_list(node, post=' = ')}" + \
                          f"connect_sqlserver({', '.join(inpnames)})\n")
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, 'outExec')


    @staticmethod
    def GenericDBConn(exporter: 'PythonExporterImpl',
                      node: NodeBase,
                      inpnames: list[str],  # pylint: disable=unused-argument
                      *args, **kwargs):  # pylint: disable=unused-argument
        """Convert GenericDBConn node type"""
        if not exporter.is_node_function_processed(node):
            exporter.add_import('sqlalchemy', imports=['create_engine', 'URL'])
            exporter.add_sys_function("""def connect_genericdb(connection_url):
    \"\"\"Connect to a generic database according to the connection url\"\"\"
    engine = create_engine(connection_url)
    return engine""")
            exporter.set_node_function_processed(node)
        exporter.add_call(f"{exporter.get_out_list(node, post=' = ')}" +
                          f"connect_genericdb({', '.join(inpnames)})\n")
        exporter.set_node_processed(node)
        exporter.call_named_pin(node, 'outExec')


    ####################
    ### Data loaders ###
    ####################

    @staticmethod
    def PandasUpload(exporter: 'PythonExporterImpl',
                     node: NodeBase,
                     inpnames: list[str],  # pylint: disable=unused-argument
                     *args, **kwargs):  # pylint: disable=unused-argument
        """Convert PandasUpload nodes"""
        # export function definition
        if not exporter.is_node_function_processed(node):
            exporter.add_sys_function(
"""def uploadPandas(conn, df, tablename, with_index, if_exists):
    if not with_index:
        df = df.reset_index(drop=True)
    df.to_sql(tablename, conn, method='multi', index=with_index, if_exists=if_exists)
""")
            exporter.set_node_function_processed(node)
        exporter.add_call(
            f"{exporter.get_out_list(node, post=' = ')}uploadPandas({', '.join(inpnames)})\n"
        )
        exporter.call_named_pin(node, DEFAULT_OUT_EXEC_NAME)


    #########################
    ### Data manipulation ###
    #########################

    @staticmethod
    def func_ReadCSV(exporter: 'PythonExporterImpl',
                     node: NodeBase,
                     *args, **kwargs):  # pylint: disable=unused-argument
        """Convert ReadCSV nodes"""
        exporter.add_import('pandas', alias='pd')
        return "    return pd.read_csv(path, delimiter=delimiter, decimal=decimal)\n"


    ############################
    ###  Value helper nodes  ###
    ############################

    @staticmethod
    def func_GetValue(exporter: 'PythonExporterImpl',
                      node: NodeBase,
                      *args, **kwargs):  # pylint: disable=unused-argument
        """Convert GetValue nodes"""
        exporter.add_import('pandas', alias='pd')
        return """    if not isinstance(df, pd.DataFrame):
        return None
    return df.loc[to_locate][column]
"""


    @staticmethod
    def call_GetSeriesValue(exporter: 'PythonExporterImpl',
                            node: NodeBase,
                            inpnames: list[str],  # pylint: disable=unused-argument
                            *args, **kwargs):  # pylint: disable=unused-argument
        """Convert GetSeriesValue nodes"""
        return f"{exporter.get_out_list(node, post=' = ')}{inpnames[0]}[{inpnames[1]}]\n"


    @staticmethod
    def call_pdisnull(node: NodeBase,
                      exporter: 'PythonExporterImpl',
                      inpnames: list[str],  # pylint: disable=unused-argument
                      *args, **kwargs):  # pylint: disable=unused-argument
        """Convert pdisnull nodes"""
        exporter.add_import('pandas', alias='pd')
        return f"{exporter.get_out_list(node, post=' = ')}pd.isnull({inpnames[0]})\n"
