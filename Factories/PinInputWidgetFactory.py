"""Pin input widgets for the data nodes"""

from PyFlow.Core.Common import DEFAULT_WIDGET_VARIANT
from PyFlow.UI.Widgets.InputWidgets import InputWidgetSingle

from qtpy.QtWidgets import QPlainTextEdit
from qtpy.QtGui import QFontDatabase

from ..SQLHighlighter import SQLHighlighter

class SQLStringWidget(InputWidgetSingle):
    """A multiline edit with SQL Syntax Highlighting"""
    def __init__(self, parent=None, dataSetCallback=None, defaultValue=None, **kwargs):
        super().__init__(parent, dataSetCallback, defaultValue, **kwargs)
        self.editor = QPlainTextEdit()
        self.editor.setFont(QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont))
        self.highlighter = SQLHighlighter(self.editor)
        self.highlighter.setDocument(self.editor.document())
        self.setWidget(self.editor)
        self.editor.document().contentsChanged.connect(self.contentsChanged)

    def blockWidgetSignals(self, bLock=False):
        return self.editor.blockSignals(bLock)

    def setWidgetValue(self, value):
        self.editor.setPlainText(value)

    def contentsChanged(self):  # pylint: disable=invalid-name
        """Pass the changed value to the pin"""
        txt = self.editor.document().toPlainText()
        if callable(self.dataSetCallback):
            self.dataSetCallback(txt)


def getInputWidget(dataType, dataSetter, defaultValue,  # pylint: disable=invalid-name
                   widgetVariant=DEFAULT_WIDGET_VARIANT, **kwargs):  # pylint: disable=invalid-name
    """Returns the input widget variants defined above"""
    if dataType=='StringPin':
        if widgetVariant == 'SQLStringWidget':
            return SQLStringWidget(dataSetCallback=dataSetter,
                                   defaultValue=defaultValue,
                                   **kwargs)
