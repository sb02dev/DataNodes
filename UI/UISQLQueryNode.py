"""The UI handling of the SQLQuery node: editing SQL statements in editor
and adding query parameters as pins
"""  # pylint: disable=invalid-name
import subprocess
import os
import logging
from typing import Optional, cast

# pylint: disable=no-name-in-module
from PySide6.QtWidgets import QGraphicsSceneMouseEvent
from qtpy.QtCore import QFileSystemWatcher
from qtpy.QtWidgets import QInputDialog
# pylint: enable=no-name-in-module

from PyFlow.UI.Canvas.UICommon import Colors, NodeActionButtonInfo
from PyFlow.UI.Canvas.UINodeBase import UINodeBase
from PyFlow.UI import RESOURCES_DIR
from PyFlow.UI.Widgets.SelectPinDialog import SelectPinDialog
from PyFlow.ConfigManager import ConfigManager
from PyFlow.Core import NodeBase, PinBase

logger = logging.getLogger('PyFlow')


INITIAL_CODE = """select
  *
from
  tableA
;
"""

class UISQLQueryNode(UINodeBase):
    """UI of the SQLQuery node"""
    watcher: Optional[QFileSystemWatcher] = None

    def __init__(self, raw_node, w=80, color=Colors.NodeBackgrounds, headColorOverride=None):
        self.pin_widget = None

        super().__init__(raw_node, w, color, headColorOverride)

        self.action_edit = self._menu.addAction("Edit")
        self.action_edit.triggered.connect(self.on_edit)

        self._file_path = ""
        self.file_handle = None
        self.current_editor_process = None

        self.action_add_param = self._menu.addAction("Add param")
        self.action_add_param.setToolTip("Add query parameter")
        self.action_add_param.setData(NodeActionButtonInfo(RESOURCES_DIR + "/pin.svg"))
        self.action_add_param.triggered.connect(self.create_pin_dialog)

        self.p_sql = cast(PinBase, cast(NodeBase, self._rawNode).getPinByName('sql'))
        self.p_sql.dataBeenSet.connect(self.on_sql_pin_changed)


    def create_pin_dialog(self):
        """Get the pin data type through the standard PyFlow dialog"""
        d = SelectPinDialog()
        d.exec_()
        data_type = d.getResult()
        if data_type is not None:
            self.on_add_input_pin(data_type)


    def on_add_input_pin(self, data_type):
        """Event fired when a new parameter pin added to node"""
        name, confirmed = QInputDialog.getText(None, "Rename",
                                               "Enter new pin name")
        if confirmed and name != self.name and name != "":
            name = self._rawNode.getUniqPinName(name)
            raw_pin = self._rawNode.addInPin(name, data_type)
            self._createUIPinWrapper(raw_pin)
            self.updateNodeShape()

    def mouseDoubleClickEvent(self,  # pylint: disable=invalid-name
                              event: QGraphicsSceneMouseEvent) -> None:
        """Event fired on double-click"""
        r = super().mouseDoubleClickEvent(event)
        self.on_edit()
        return r


    @property
    def node_sql(self):
        """Property to directly access the sql pin value of the underlying node"""
        return self._rawNode.getData('sql')


    @node_sql.setter
    def node_sql(self, value):
        """Property to directly access the sql pin value of the underlying node"""
        self._rawNode.setData('sql', value)
        if self.pin_widget:
            self.pin_widget.setWidgetValue(value)


    def on_sql_pin_changed(self, value):
        """Event fired when sql pin changed and file needs to be updated"""
        if self.file_handle is not None:
            self.file_handle.seek(0)
            code_string = self.file_handle.read()
            if value.getData() != '' and value.getData() != code_string:
                f = open(self._file_path, "w", encoding='utf8')
                f.write(self.node_sql)
                f.close()


    def on_file_changed(self, path):
        """Event fired when the temporary file on disk is changed"""
        uid_str = str(self.uid).replace('-', '')
        if uid_str not in path:
            return

        if not os.path.exists(path):
            self._file_path = ''
            if self.file_handle is not None:
                self.file_handle.close()
                self.file_handle = None
            return
        else:
            # open file handle if needed
            if self.file_handle is None:
                self.file_handle = open(path, "r", encoding="utf8")

            # read code string
            self.file_handle.seek(0)
            code_string = self.file_handle.read()
            self.try_apply_node_data(code_string)


    def try_apply_node_data(self, data_string):
        """Set the node's sql string and catch&log any exceptions during"""
        if data_string == self.node_sql:
            return
        try:
            self.node_sql = data_string
            self.updateNodeShape()
            self.updateNodeHeaderColor()
        except Exception as e:
            logger.warning(e)


    def shoutDown(self):
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None
        return super().shoutDown()


    def kill(self, *args, **kwargs):
        try:
            if self.file_handle is not None:
                self.file_handle.close()
                self.file_handle = None
            os.remove(self._file_path)
        except Exception:
            pass
        return super().kill(*args, **kwargs)


    def on_edit(self):
        """Double-click and Edit menu handler"""
        edit_cmd = ConfigManager().getPrefsValue('PREFS', 'GeneralPreferences/EditorCmd')
        temp_files_dir = self.canvasRef().getApp().getTempDirectory()  # type: ignore  # pylint: disable=not-callable

        if self._file_path == '':
            # if no file associated - create one
            uid_str = str(self.uid).replace('-', '')
            self._file_path = os.path.join(temp_files_dir, f'{uid_str}.sql')

        # always recreate file, because data could have changed since
        f = open(self._file_path, "w", encoding='utf8')
        if self.node_sql=='':
            f.write(INITIAL_CODE)
        else:
            f.write(self.node_sql)
        f.close()

        file_path_string = f'"{self._file_path}"'
        edit_cmd = edit_cmd.replace("@FILE", file_path_string)

        # create file watcher and attach event
        if UISQLQueryNode.watcher is None:
            UISQLQueryNode.watcher = QFileSystemWatcher()
        if self._file_path not in UISQLQueryNode.watcher.files():
            UISQLQueryNode.watcher.addPath(self._file_path)

        try:
            UISQLQueryNode.watcher.fileChanged.disconnect(self.on_file_changed)
        except Exception:
            pass

        UISQLQueryNode.watcher.fileChanged.connect(self.on_file_changed)
        self.current_editor_process = subprocess.Popen(edit_cmd, shell=True)
        self.file_handle = open(self._file_path, "r", encoding='utf8')


    def createInputWidgets(self, inputsCategory, inGroup=None, pins=True):
        r = super().createInputWidgets(inputsCategory, inGroup, pins)
        self.pin_widget = inputsCategory.getWidgetByName('sql')
        return r
