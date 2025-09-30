"""A node passing an SQL query to a database connection"""  # pylint: disable=invalid-name

import json
from typing import TYPE_CHECKING, Optional, cast
import re
import uuid
from sqlalchemy import text
import pandas as pd

from PyFlow.Core import NodeBase, PinBase
from PyFlow.Core.NodeBase import NodePinsSuggestionsHelper
from PyFlow.Core.Common import StructureType, PinOptions, DEFAULT_IN_EXEC_NAME, push

if TYPE_CHECKING:
    from PythonExporter.Exporters.implementation import PythonExporterImpl


from ..constants import FLOW_CONTROL_COLOR  # pylint: disable=wrong-import-position


class forEachRowDF(NodeBase):
    """A node iterating through rows of a DataFrame"""

    def __init__(self, name, uid=None):
        super().__init__(name, uid)
        
        self.p_in_exec = cast(PinBase, self.createInputPin(
            pinName=DEFAULT_IN_EXEC_NAME,
            dataType='ExecPin',
            callback=self.compute
        ))
        
        self.p_df = cast(PinBase, self.createInputPin(
            pinName='df',
            dataType='DataFramePin',
            defaultValue=None,
            callback=None,
            structure=StructureType.Single,
            constraint="1",
            structConstraint=None,
            supportedPinDataTypes=[],
            group=''
        ))
        self.p_df.enableOptions(PinOptions.AllowAny)

        self.p_loop_body = cast(PinBase, self.createOutputPin(
            pinName='loop_body',
            dataType='ExecPin',
            group=''
        ))

        self.p_idx = cast(PinBase, self.createOutputPin(
            pinName='idx',
            dataType='AnyPin',
            defaultValue=True,
            supportedPinDataTypes=['StringPin', 'IntPin', 'FloatPin'],
            group=''
        ))
        self.p_idx.enableOptions(PinOptions.AllowAny)

        self.p_row = cast(PinBase, self.createOutputPin(
            pinName='row',
            dataType='SeriesPin',
            constraint="1",
            group=''
        ))
        self.p_row.enableOptions(PinOptions.AllowAny)

        self.p_completed = cast(PinBase, self.createOutputPin(
            pinName='completed',
            dataType='ExecPin'
        ))

        self.headerColor = FLOW_CONTROL_COLOR


    @staticmethod
    def pinTypeHints():
        helper = NodePinsSuggestionsHelper()
        helper.addInputDataType("ExecPin")
        helper.addInputDataType("DataFramePin")
        helper.addOutputDataType("ExecPin")
        helper.addOutputDataType("AnyPin")
        helper.addOutputDataType("SeriesPin")
        helper.addOutputDataType("ExecPin")
        helper.addInputStruct(StructureType.Single)
        helper.addInputStruct(StructureType.Single)
        helper.addOutputStruct(StructureType.Single)
        helper.addOutputStruct(StructureType.Single)
        return helper

    @staticmethod
    def category():  # type: ignore
        return 'DatabaseTools|Pandas'

    @staticmethod
    def keywords():
        return ['iter', 'for', 'loop', 'each']

    @staticmethod
    def description():  # type: ignore
        return 'Iterate through rows of a Pandas DataFrame'


    def to_python(self,  # pylint: disable=unused-argument
                  exporter: 'PythonExporterImpl',
                  inpnames: list[str],  # pylint: disable=unused-argument
                  *args,
                  **kwargs):
        """Export forEachRowDF node as pure python"""
        # export function definition
        if not exporter.is_node_function_processed(self):
            exporter.add_import("pandas", alias="pd")
            exporter.set_node_function_processed(self)
        # export call
        exporter.add_call(f"for {exporter.get_out_list(self)} in {inpnames[0]}.iterrows():\n")
        # flag that we are exported
        exporter.set_node_processed(self)
        # convert the loop body
        exporter.increase_indent()
        exporter.call_named_pin(self, 'loop_body')
        exporter.decrease_indent()
        # continue graph
        exporter.call_named_pin(self, 'completed')


    def compute(self, *args, **kwargs):
        # get inputs
        df: pd.DataFrame = self.getData('df')

        if len(df.index) == 0:
            self.p_completed.call(*args, **kwargs)
        else:
            for i, rw in df.iterrows():
                self.p_idx.setData(i)
                self.p_row.setData(rw)
                push(self.p_idx)
                push(self.p_row)
                self.p_loop_body.call(*args, **kwargs)

        # set result and continue graph
        self.p_completed.call(*args, **kwargs)
