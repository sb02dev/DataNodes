"""Patches for pandasql package to add parameterized queries"""

from pandasql.sqldf import (
    PandaSQL as pSQL,
    extract_table_names,
    write_table,
    get_outer_frame_variables,
    PandaSQLException
)
from sqlalchemy.exc import DatabaseError, ResourceClosedError
import pandas as pd


class PandaSQL(pSQL):
    """A patched subclass of PandaSQL providing parameterized queries against Pandas DataFrames"""

    @staticmethod
    def sql_power(x: float, y: float) -> float | None:
        """UDF to return x to the power of y"""
        try:
            return x**y
        except:
            return None

    def _init_connection(self, conn):
        if self.engine.name == 'sqlite':
            conn.connection.driver_connection.create_function(
                'power', 2, self.sql_power)
        super()._init_connection(conn)

    def __call__(self, query, env=None, params=None):
        """
        Execute the SQL query.
        Automatically creates tables mentioned in the query from dataframes before executing.

        :param query: SQL query string, which can reference pandas dataframes as SQL tables.
        :param env: Variables environment - a dict mapping table names to pandas dataframes.
        If not specified use local and global variables of the caller.
        :return: Pandas dataframe with the result of the SQL query.
        """
        if env is None:
            env = get_outer_frame_variables()

        result = None

        with self.conn as conn:
            for table_name in extract_table_names(query):
                if table_name not in env:
                    # don't raise error because the table may be already in the database
                    continue
                if self.persist and table_name in self.loaded_tables:
                    # table was loaded before using the same instance, don't do it again
                    continue
                if isinstance(env[table_name], pd.DataFrame):
                    self.loaded_tables.add(table_name)
                    write_table(env[table_name], table_name, conn)

            try:
                result = pd.read_sql(query, conn, params=params)
            except DatabaseError as ex:
                raise PandaSQLException(ex) from ex
            except ResourceClosedError:
                # query returns nothing
                result = None

        return result

    @staticmethod
    def uniquify(df_columns) -> list[str]:
        """Make a list of columns unique"""
        seen = set()
        result = []
        for item in df_columns:
            fudge = 1
            newitem = item

            while newitem in seen:
                fudge += 1
                newitem = f"{item}_{fudge}"

            result.append(newitem)
            seen.add(newitem)
        return result


PANDASQL_STR = '''class PandaSQL(pSQL):
    """A patched subclass of PandaSQL providing parameterized queries against Pandas DataFrames"""

    @staticmethod
    def sql_power(x: float, y: float) -> float | None:
        """UDF to return x to the power of y"""
        try:
            return x**y
        except:
            return None

    def _init_connection(self, conn):
        if self.engine.name == 'sqlite':
            conn.connection.driver_connection.create_function(
                'power', 2, self.sql_power)
        super()._init_connection(conn)

    def __call__(self, query, env=None, params=None):
        """
        Execute the SQL query.
        Automatically creates tables mentioned in the query from dataframes before executing.

        :param query: SQL query string, which can reference pandas dataframes as SQL tables.
        :param env: Variables environment - a dict mapping table names to pandas dataframes.
        If not specified use local and global variables of the caller.
        :return: Pandas dataframe with the result of the SQL query.
        """
        if env is None:
            env = get_outer_frame_variables()

        result = None

        with self.conn as conn:
            for table_name in extract_table_names(query):
                if table_name not in env:
                    # don't raise error because the table may be already in the database
                    continue
                if self.persist and table_name in self.loaded_tables:
                    # table was loaded before using the same instance, don't do it again
                    continue
                if isinstance(env[table_name], pd.DataFrame):
                    self.loaded_tables.add(table_name)
                    write_table(env[table_name], table_name, conn)

            try:
                result = pd.read_sql(query, conn, params=params)
            except DatabaseError as ex:
                raise PandaSQLException(ex) from ex
            except ResourceClosedError:
                # query returns nothing
                result = None

        return result

    @staticmethod
    def uniquify(df_columns) -> list[str]:
        """Make a list of columns unique"""
        seen = set()
        result = []
        for item in df_columns:
            fudge = 1
            newitem = item

            while newitem in seen:
                fudge += 1
                newitem = f"{item}_{fudge}"

            result.append(newitem)
            seen.add(newitem)
        return result
'''
