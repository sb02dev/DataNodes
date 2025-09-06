"""An SQL Syntax Highlighter class"""

from typing import Optional, cast
from qtpy.QtCore import Qt, QObject
from qtpy.QtGui import (QSyntaxHighlighter, QTextCharFormat, QFont)

import pygments.lexers.sql
from pygments.token import Token

class SQLHighlighter(QSyntaxHighlighter):
    """An SQL Syntax Highlighter class for Qt Editors"""
    def __init__(self, parent: Optional[QObject] = None):
        super().__init__(cast(QObject, parent))

        self._mappings = {}

        f_keyword = QTextCharFormat()
        f_keyword.setFontWeight(QFont.Weight.Bold)
        f_keyword.setForeground(Qt.GlobalColor.blue)
        self.add_mapping(Token.Keyword, f_keyword)

        f_operator = QTextCharFormat()
        f_operator.setForeground(Qt.GlobalColor.darkGreen)
        self.add_mapping(Token.Operator, f_operator)

        f_name = QTextCharFormat()
        f_name.setFontWeight(QFont.Weight.Bold)
        f_name.setForeground(Qt.GlobalColor.red)
        self.add_mapping(Token.Name, f_name)

        f_num = QTextCharFormat()
        f_num.setFontWeight(QFont.Weight.Bold)
        f_num.setForeground(Qt.GlobalColor.darkGreen)
        self.add_mapping(Token.Literal.Number.Integer, f_num)

        f_punct = QTextCharFormat()
        f_punct.setFontWeight(QFont.Weight.Bold)
        self.add_mapping(Token.Punctuation, f_punct)

        self._lex = pygments.lexers.sql.SqlLexer()


    def add_mapping(self, token, fmt):
        """Helper to map tokens to formats"""
        self._mappings[token] = fmt


    def highlightBlock(self, text: str) -> None:  # pylint: disable=invalid-name
        """The main highlighter routine. This one uses pygments."""
        start = 0
        for tok, txt in self._lex.get_tokens(text):
            if tok in self._mappings:
                self.setFormat(start, len(txt), self._mappings[tok])
            start+=len(txt)
