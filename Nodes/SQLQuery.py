"""A node passing an SQL query to a database connection"""  # pylint: disable=invalid-name

import json
from typing import TYPE_CHECKING, Optional, cast
import re
import uuid
from sqlalchemy import text
import pandas as pd

from PyFlow.Core import NodeBase, PinBase
from PyFlow.Core.NodeBase import NodePinsSuggestionsHelper
from PyFlow.Core.Common import StructureType, PinOptions

if TYPE_CHECKING:
    from PythonExporter.Exporters.implementation import PythonExporterImpl


from ..constants import DB_HEADER_COLOR  # pylint: disable=wrong-import-position


class SQLQuery(NodeBase):
    """A node passing an SQL query to a database connection"""

    def __init__(self, name, uid=None):
        super().__init__(name, uid)
        self.p_query = cast(PinBase, self.createInputPin(
            pinName='query',
            dataType='ExecPin',
            callback=self.compute
        ))
        self.p_conn = cast(PinBase, self.createInputPin(
            pinName='conn',
            dataType='DBEnginePin',
            defaultValue=None,
            callback=None,
            structure=StructureType.Single,
            constraint=None,
            structConstraint=None,
            supportedPinDataTypes=[],
            group=''
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
        self.p_has_result = cast(PinBase, self.createInputPin(
            pinName='has_result',
            dataType='BoolPin',
            defaultValue=True,
            callback=None,
            structure=StructureType.Single,
            constraint=None,
            structConstraint=None,
            supportedPinDataTypes=[],
            group=''
        ))
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
        self.headerColor = DB_HEADER_COLOR

    def addInPin(self, name: str, dataType: str):
        """Helper method to add a dynamic input pin"""
        p = cast(PinBase, self.createInputPin(name, dataType))
        p.enableOptions(PinOptions.RenamingEnabled | PinOptions.Dynamic)
        return p

    def postCreate(self, jsonTemplate: Optional[dict] = None):
        super(SQLQuery, self).postCreate(jsonTemplate)
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
        helper.addInputDataType("DBEnginePin")
        helper.addInputDataType("StringPin")
        helper.addOutputDataType("ExecPin")
        helper.addOutputDataType("DataFramePin")
        helper.addInputStruct(StructureType.Single)
        helper.addInputStruct(StructureType.Single)
        helper.addOutputStruct(StructureType.Single)
        return helper

    @staticmethod
    def category():  # type: ignore
        return 'DatabaseTools|Server'

    @staticmethod
    def keywords():
        return []

    @staticmethod
    def description():  # type: ignore
        return 'A node passing an SQL query to a database connection'


    def to_python(self,  # pylint: disable=unused-argument
                  exporter: 'PythonExporterImpl',
                  inpnames: list[str],  # pylint: disable=unused-argument
                  *args,
                  **kwargs):
        """Export SQLQueryNode as pure python"""
        # export function definition
        if not exporter.is_node_function_processed(self):
            exporter.add_import("pandas", alias="pd")
            exporter.add_import("sqlalchemy", imports=["text"])
            exporter.add_import("re")
            exporter.add_sys_function(
                """def queryDatabase(conn, sql, has_result, param_dict={}, **kwargs):
    # tranform parameters
    parameters = {}
    for k, v in kwargs.items():
        if not k in parameters:
            parameters[k] = v
    for k, v in param_dict.items():
        if not k in parameters:
            parameters[k] = v

    # query
    sqlstatements = [s
                     for s in sql.split(';\\n')
                     if not re.search(r"^[;\\s]*$", s)]
    table = None

    with conn.begin() as active_conn:
        for i, sqlstatement in enumerate(sqlstatements):
            if has_result and i==len(sqlstatements)-1: # only last statement can have result
                table = pd.read_sql_query(text(sqlstatement),
                                            active_conn,
                                            params=parameters)
            else:
                active_conn.execute(text(sqlstatement), parameters)
    
    return table""")
            exporter.set_node_function_processed(self)
        # export call
        inputs = inpnames[2]
        if self.p_param_dict.hasConnections():
            inputs+=f", param_dict={inpnames[3]}"
        i=3
        for inpin in self.orderedInputs.values():
            if inpin not in [self.p_query,
                             self.p_conn,
                             self.p_sql,
                             self.p_has_result,
                             self.p_param_dict]:
                inputs += f", {inpin.name} = {inpnames[i]}"
                i += 1
        prg = f"{exporter.get_out_list(self, post=' = ')}queryDatabase({inpnames[0]}, "
        if self.p_sql.hasConnections():
            prg += inpnames[1]+', '
        else:
            # beautify sql parameter
            sqllines = cast(str, self.p_sql.currentData()).replace('\t', '    ').splitlines()
            newline = '\n'+' '*(len(prg)+3)
            prg += '"""'+newline.join(sqllines)+'"""'
        exporter.add_call(f"{prg}, {inputs})\n")
        exporter.set_node_processed(self)
        exporter.call_named_pin(self, 'completed')


    def compute(self, *args, **kwargs):
        # get inputs
        conn = self.getData('conn')
        sql = self.getData('sql')
        has_result = self.getData('has_result')
        param_dict = self.getData('param_dict')

        # tranform parameters
        parameters = {pin.name: pin.getData()
                      for pin in self.orderedInputs.values()
                      if pin not in [self.p_query,
                                     self.p_conn,
                                     self.p_sql,
                                     self.p_has_result,
                                     self.p_param_dict]}
        for k, v in param_dict.items():
            if not k in parameters:
                parameters[k] = v

        # query
        sqlstatements = [s
                         for s in sql.split(';\n')
                         if not re.search(r"^[;\s]*$", s)]
        table = None

        with conn.begin() as active_conn:
            for i, sqlstatement in enumerate(sqlstatements):
                if has_result and i==len(sqlstatements)-1: # only last statement can have result
                    table = pd.read_sql_query(text(sqlstatement),
                                              active_conn,
                                              params=parameters)
                else:
                    active_conn.execute(text(sqlstatement), parameters)

        # set result and continue graph
        self.setData('result', table)
        self.p_completed.call()
