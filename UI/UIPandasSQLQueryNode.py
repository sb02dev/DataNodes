"""The UI handling of the SQLQuery node: editing SQL statements in editor
and adding query parameters as pins
"""  # pylint: disable=invalid-name

# pylint: disable=no-name-in-module
from qtpy.QtCore import Signal  # type: ignore
from qtpy.QtWidgets import QInputDialog
# pylint: enable=no-name-in-module

from PyFlow.UI.Canvas.UICommon import Colors, NodeActionButtonInfo
from PyFlow.UI import RESOURCES_DIR

from .UISQLQueryNode import UISQLQueryNode


class UIPandasSQLQueryNode(UISQLQueryNode):
    """UI of the PandasSQLQuery node"""
    pinCreated = Signal(object)

    def __init__(self, raw_node, w=80, color=Colors.NodeBackgrounds, headColorOverride=None):
        super().__init__(raw_node, w, color, headColorOverride)

        self.action_add_table = self._menu.addAction("Add param")
        self.action_add_table.setToolTip("Add input table")
        self.action_add_table.setData(NodeActionButtonInfo(RESOURCES_DIR + "/pin.svg"))
        self.action_add_table.triggered.connect(self.on_add_table_pin)


    def on_add_table_pin(self):
        """Event fired when a new input table pin should be added"""
        name, confirmed = QInputDialog.getText(None, "Rename",
                                               "Enter new pin name")
        if confirmed and name != self.name and name != "":
            name = self._rawNode.getUniqPinName(name)
            raw_pin = self._rawNode.addInPin(name, 'DataFramePin')
            ui_pin = self._createUIPinWrapper(raw_pin)
            self.pinCreated.emit(ui_pin)
            self.updateNodeShape()
