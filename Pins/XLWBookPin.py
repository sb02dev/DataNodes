"""Implementation of a PyFlow Pin to carry XlWings Workbook objects"""  # pylint: disable=invalid-name
import json
import logging
import xlwings as xw
from pythoncom import com_error  # pylint: disable=no-name-in-module

from PyFlow.Core.Common import PinOptions
from PyFlow.Core import PinBase

from ..constants import XLLIB_WORKBOOK_PIN_COLOR  # pylint: disable=relative-beyond-top-level

class XLWBookData:
    """Internal data structure for XLWBookPin"""
    def __init__(self, value = None) -> None:
        super().__init__()
        self.value = value
        self.fullname = value.fullname \
                        if value is not None and hasattr(value, 'fullname') \
                        else None


    def get_value(self) -> xw.Book | None:
        """Get the xw.Book object as a value with some thread-safety checks"""
        if self.value is not None:
            book = self.value
            # must check if it is accessible from this thread and if not we create
            # a new object in this thread (this should not be too expensive or
            # disruptive since the workbook is already open in Excel)
            try:
                _ = book.sheets[0]
                return book
            except com_error as e:
                logging.getLogger('PyFlow.DataNodes').exception(e)
        if self.fullname is not None:
            return xw.Book(self.fullname,
                           update_links=False,
                           read_only=True, # TODO: really read-only???
                           ignore_read_only_recommended=True)
        return None


class XLWBookEncoder(json.JSONEncoder):
    """Encode xw.Book values to json"""
    def default(self, o):
        if isinstance(o, xw.Book):
            try:
                fname = o.fullname
            except com_error as e:
                # COM object for different thread, can't do anything
                logging.getLogger('PyFlow.DataNodes').exception(e)
                fname = None
            return {
                "_type": "XLWBook",
                "filepath": fname
            }
        if isinstance(o, XLWBookData):
            return {
                "_type": "XLWBookData",
                "filepath": o.fullname,
            }
        return super().default(o)


class XLWBookDecoder(json.JSONDecoder):
    """Decode XLWBook values from json"""
    def __init__(self, *args, **kwargs):
        if 'object_hook' in kwargs:
            self._passed_object_hook = kwargs['object_hook']
            del kwargs['object_hook']
        super().__init__(*args, object_hook=self._object_hook, **kwargs)

    def _object_hook(self, o):  # type: ignore
        """Decode XLWBook objects"""
        if '_type' in o:
            if o['_type'] in ['XLWBook', 'XLWBookData']:
                try:
                    if o['filepath']=='' or o['filepath'] is None:
                        return None
                    else:
                        return XLWBookPin.internalDataStructure()(
                            xw.Book(o['filepath'],
                                    update_links=False,
                                    read_only=True, # TODO: really readonly???
                                    ignore_read_only_recommended=True)
                        )
                except Exception as e:
                    logging.getLogger('PyFlow.DataNodes').exception(e)
                    return None
        if callable(self._passed_object_hook):
            return self._passed_object_hook(o)
        return o


class XLWBookPin(PinBase):
    """Pin transferring xw.Book objects between nodes"""

    def __init__(self, name, owningNode, direction, **kwargs):
        super().__init__(name, owningNode, direction, **kwargs)
        self.disableOptions(PinOptions.Storable)
        self.setDefaultValue(None)

    @staticmethod
    def IsValuePin():
        return True

    @staticmethod
    def supportedDataTypes():  # type: ignore
        return ('XLWBookPin', )

    @staticmethod
    def pinDataTypeHint():  # type: ignore
        return 'XLWBookPin', None

    @staticmethod
    def color():  # type: ignore
        return XLLIB_WORKBOOK_PIN_COLOR

    @staticmethod
    def internalDataStructure():
        return XLWBookData

    @staticmethod
    def processData(data):
        if isinstance(data, xw.Book):
            try:
                _ = data.sheets[0]
            except com_error as e:
                logging.getLogger('PyFlow.DataNodes').exception(e)
                return None # FIXME: cannot do anything
        if isinstance(data, XLWBookData):
            return data
        return XLWBookPin.internalDataStructure()(data)

    @staticmethod
    def jsonEncoderClass():
        """Returns json encoder class for this pin"""
        return XLWBookEncoder

    @staticmethod
    def jsonDecoderClass():
        """Returns json decoder class for this pin"""
        return XLWBookDecoder
