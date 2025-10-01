"""A node running an SQL query against multiple Pandas DataFrames"""  # pylint: disable=invalid-name

import json
from typing import TYPE_CHECKING, Optional, cast
import re
import uuid

from PyFlow.Core import NodeBase, PinBase
from PyFlow.Core.NodeBase import NodePinsSuggestionsHelper
from PyFlow.Core.Common import StructureType, PinOptions

if TYPE_CHECKING:
    from PythonExporter.Exporters.implementation import PythonExporterImpl


from ..constants import PDLIB_HEADER_COLOR  # pylint: disable=wrong-import-position
from .. import pandasql2  # pylint: disable=wrong-import-position


class PandasSQLQuery(NodeBase):
    """A node running an SQL query against multiple Pandas DataFrames"""

    def __init__(self, name, uid=None):
        super().__init__(name, uid)
        self.p_query = cast(PinBase, self.createInputPin(
            pinName='query',
            dataType='ExecPin',
            callback=self.compute
        ))
        self.p_sql = cast(PinBase, self.createInputPin(
            pinName='sql',
            dataType='StringPin',
            defaultValue='select count(*) from tableA',
            callback=None,
            structure=StructureType.Single,
            constraint=None,
            structConstraint=None,
            supportedPinDataTypes=[],
            group=''
        ))
        self.p_sql.setInputWidgetVariant('SQLStringWidget')
        self.p_param_dict = cast(PinBase, self.createInputPin(
            pinName='param_dict',
            dataType='AnyPin',
            callback=None,
            structure=StructureType.Dict,
            constraint=None,
            structConstraint=None,
            supportedPinDataTypes=[],
            group=''
        ))
        self.p_param_dict.enableOptions(PinOptions.AllowAny)
        self.p_completed = cast(PinBase, self.createOutputPin(
            pinName='completed',
            dataType='ExecPin'
        ))
        self.p_result = cast(PinBase, self.createOutputPin(
            pinName='result',
            dataType='DataFramePin',
            defaultValue=None,
            structure=StructureType.Single,
            constraint=None,
            structConstraint=None,
            supportedPinDataTypes=[],
            group=''
        ))
        self.p_result.disableOptions(PinOptions.Storable)

        self.headerColor = PDLIB_HEADER_COLOR


    def addInPin(self, name: str, dataType: str):
        """Helper method to add a dynamic input pin"""
        p = cast(PinBase, self.createInputPin(name, dataType))
        p.enableOptions(PinOptions.RenamingEnabled | PinOptions.Dynamic)
        return p


    def postCreate(self, jsonTemplate: Optional[dict] = None):
        super().postCreate(jsonTemplate)
        # recreate dynamically created pins
        existingPins = self.namePinInputsMap
        if jsonTemplate is not None:
            sortedInputs = sorted(jsonTemplate["inputs"], key=lambda x: x["pinIndex"])
            for inPinJson in sortedInputs:
                if inPinJson['name'] not in existingPins:
                    inDyn = self.addInPin(inPinJson['name'], inPinJson['dataType'])
                    inDyn.uid = uuid.UUID(inPinJson['uuid'])
                    try:
                        val = json.loads(inPinJson['value'], cls=inDyn.jsonDecoderClass())
                        inDyn.setData(val)
                    except Exception:
                        inDyn.setData(inDyn.defaultValue())


    @staticmethod
    def pinTypeHints():
        helper = NodePinsSuggestionsHelper()
        helper.addInputDataType("ExecPin")
        helper.addInputDataType("StringPin")
        helper.addOutputDataType("ExecPin")
        helper.addOutputDataType("DataFramePin")
        helper.addInputStruct(StructureType.Single)
        helper.addOutputStruct(StructureType.Single)
        return helper


    @staticmethod
    def category():  # type: ignore
        return 'DatabaseTools|Pandas'

    @staticmethod
    def keywords():
        return []

    @staticmethod
    def description():  # type: ignore
        return 'A node running an SQL query against multiple Pandas DataFrames'


    def to_python(self,  # pylint: disable=unused-argument
                  exporter: 'PythonExporterImpl',
                  inpnames: list[str],  # pylint: disable=unused-argument
                  *args,
                  **kwargs):
        """Export PandasSQLQueryNode as pure python"""
        # export function definition
        if not exporter.is_node_function_processed(self):
            exporter.add_import("pandas", alias="pd")
            exporter.add_import("re")
            exporter.add_import("pandasql.sqldf", imports=["extract_table_names",
                                                           "write_table",
                                                           "get_outer_frame_variables",
                                                           "PandaSQLException",
                                                           "PandaSQL as pSQL"])
            exporter.add_import("sqlalchemy.exc", imports=["DatabaseError", "ResourceClosedError"])
            exporter.add_setup("pandasql_functions", pandasql2.PANDASQL_STR)

            exporter.add_sys_function(
                """def queryPandas(sql, tables, params):
    psql = PandaSQL(persist=True)

    sqlstatements = [s
                        for s in sql.split(';\\n')
                        if not re.search(r"^[;\\s]*$", s)]
    results = []

    for sqlstatement in sqlstatements:
        res = psql(sqlstatement, tables, params=params)  # type: ignore
        if res is not None:
            res.columns = psql.uniquify(res.columns)
            results.append(res)

    if len(results)==0:
        return None
    if len(results)==1:
        return results[0]
    return results
""")
            exporter.set_node_function_processed(self)
        # export call
        call_str = f"{exporter.get_out_list(self, post=' = ')}queryPandas("
        tables = ""
        params = ""
        idx = 1
        for inpin in self.orderedInputs.values():
            if inpin not in [self.p_query, self.p_sql, self.p_param_dict]:
                curpar = f"{repr(inpin.name)}: {inpnames[idx]},\n{' '*(len(call_str)+8)}"
                if inpin.dataType=='DataFramePin':
                    tables+=curpar
                else:
                    params+=curpar
                idx += 1
        param_dict = cast(dict, self.p_param_dict.currentData())
        for k, v in param_dict.items():
            if k not in self.orderedInputs.keys():
                params += f"{k}: {repr(v)},\n{' '*(len(call_str)+8)}"
        if self.p_sql.hasConnections():
            sql=inpnames[0]
        else:
            # beautify sql parameter
            sqllines = cast(str, self.p_sql.currentData()).replace('\t', ' ').splitlines()
            newline = '\n'+' '*(len(call_str)+3)
            sql='"""'+newline.join(sqllines)+'"""'
        call_str += f"{sql},\n{' '*len(call_str)}tables={{{tables}}},\n{' '*len(call_str)}" + \
                    f"params={{{params}}})\n"
        exporter.add_call(call_str)
        # flag that we are exported
        exporter.set_node_processed(self)
        # call out exec pin
        exporter.call_named_pin(self, 'completed')


    def compute(self, *args, **kwargs):
        # get inputs
        sql = self.getData('sql')
        param_dict = self.getData('param_dict')

        # get tables
        tables = {pin.name: pin.getData()
                  for pin in self.orderedInputs.values()
                  if pin not in [self.p_query, self.p_sql, self.p_param_dict]
                     and pin.dataType=='DataFramePin'
                 }

        # tranform parameters
        parameters = {pin.name: pin.getData()
                      for pin in self.orderedInputs.values()
                      if pin not in [self.p_query,
                                     self.p_sql,
                                     self.p_param_dict]
                         and pin.dataType!='DataFramePin'}

        for k, v in param_dict.items():
            if not k in parameters:
                parameters[k] = v

        # query
        psql = pandasql2.PandaSQL(persist=True)

        sqlstatements = [s
                         for s in sql.split(';\n')
                         if not re.search(r"^[;\s]*$", s)]
        results = []

        for sqlstatement in sqlstatements:
            res = psql(sqlstatement, tables, params=parameters)  # type: ignore
            if res is not None:
                res.columns = psql.uniquify(res.columns)
                results.append(res)

        # set result and continue graph
        if len(results)==0:
            self.setData('result', None)
        elif len(results)==1:
            self.setData('result', results[0])
        else:
            self.setData('result', results)
        self.p_completed.call()
