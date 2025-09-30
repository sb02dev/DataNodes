"""Implementation of a PyFlow Pin to carry Pandas DataFrames"""  # pylint: disable=invalid-name
import json
import io
import pandas as pd

from PyFlow.Core.Common import PinOptions
from PyFlow.Core import PinBase
from ..constants import PDLIB_SERIES_PIN_COLOR  # pylint: disable=relative-beyond-top-level

class SeriesData:
    """Internal data structure for SeriesPin"""
    def __init__(self, value = None) -> None:
        super().__init__()
        self.value = value


class SeriesEncoder(json.JSONEncoder):
    """Encode Series values to json"""
    def default(self, o):
        if isinstance(o, pd.Series):
            try:
                jsonobj = o.to_json()
            except Exception:
                jsonobj = None
            return {
                "_type": "Series",
                "data": jsonobj
            }
        return super().default(o)

class SeriesDecoder(json.JSONDecoder):
    """Decode Series values from json"""
    def __init__(self, *args, **kwargs):
        if 'object_hook' in kwargs:
            self._passed_object_hook = kwargs['object_hook']
            del kwargs['object_hook']
        super().__init__(*args, object_hook=self._object_hook, **kwargs)

    def _object_hook(self, o):  # type: ignore
        """Decode Series objects"""
        if '_type' in o:
            if o['_type']=='Series':
                try:
                    return pd.read_json(io.StringIO(o['data']), typ='series')
                except Exception:
                    return None
        if callable(self._passed_object_hook):
            return self._passed_object_hook(o)
        return o


class SeriesPin(PinBase):
    """Pin transferring Pandas Series objects between nodes"""

    def __init__(self, name, owningNode, direction, **kwargs):
        super().__init__(name, owningNode, direction, **kwargs)
        self.disableOptions(PinOptions.Storable)
        self.setDefaultValue(None)

    @staticmethod
    def IsValuePin():
        return True

    @staticmethod
    def supportedDataTypes():  # type: ignore
        return ('SeriesPin', )

    @staticmethod
    def pinDataTypeHint():  # type: ignore
        return 'SeriesPin', None

    @staticmethod
    def color():  # type: ignore
        return PDLIB_SERIES_PIN_COLOR

    @staticmethod
    def internalDataStructure():
        return SeriesData

    @staticmethod
    def processData(data):
        return data

    @staticmethod
    def jsonEncoderClass():
        """Returns json encoder class for this pin"""
        return SeriesEncoder

    @staticmethod
    def jsonDecoderClass():
        """Returns json decoder class for this pin"""
        return SeriesDecoder
