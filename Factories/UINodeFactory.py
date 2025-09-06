"""Factory function for provided Node UI classes"""  # pylint: disable=invalid-name
from PyFlow.UI.Canvas.UINodeBase import UINodeBase

from ..UI.UISQLQueryNode import UISQLQueryNode

def createUINode(raw_instance):
    """The factory function for provided Node UI classes"""
    if raw_instance.__class__.__name__=='SQLQuery':
        return UISQLQueryNode(raw_instance)
    return UINodeBase(raw_instance)
