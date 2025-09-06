"""Implementation of a PyFlow Pin to carry Pandas DataFrames"""  # pylint: disable=invalid-name
import json
import pandas as pd

from PyFlow.Core.Common import PinOptions
from PyFlow.Core import PinBase
from ..constants import DB_DATAFRAME_PIN_COLOR  # pylint: disable=relative-beyond-top-level

class DataFrameData:
    """Internal data structure for DataFramePin"""
    def __init__(self, value = None) -> None:
        self.value = value

class DataFrameEncoder(json.JSONEncoder):
    """Encode DataFrame values to json"""
    def default(self, o):
        if isinstance(o, pd.DataFrame):
            try:
                jsonobj = o.to_json()
            except Exception:
                jsonobj = None
            return {
                "_type": "DataFrame",
                "data": jsonobj
            }
        return super().default(o)

class DataFrameDecoder(json.JSONDecoder):
    """Decode DataFrame values from json"""
    def __init__(self, *args, **kwargs):
        if 'object_hook' in kwargs:
            self._passed_object_hook = kwargs['object_hook']
            del kwargs['object_hook']
        super().__init__(*args, object_hook=self._object_hook, **kwargs)

    def _object_hook(self, o):  # type: ignore
        """Decode DataFrame objects"""
        if '_type' in o:
            if o['_type']=='DataFrame':
                try:
                    return pd.read_json(o['data'])
                except Exception:
                    return None
        if callable(self._passed_object_hook):
            return self._passed_object_hook(o)
        return o


class DataFramePin(PinBase):
    """Pin transferring Pandas DataFrame objects between nodes"""

    def __init__(self, name, owningNode, direction, **kwargs):
        super().__init__(name, owningNode, direction, **kwargs)
        self.disableOptions(PinOptions.Storable)
        self.setDefaultValue(None)

    @staticmethod
    def IsValuePin():
        return True

    @staticmethod
    def supportedDataTypes():  # type: ignore
        return ('DataFramePin', )

    @staticmethod
    def pinDataTypeHint():  # type: ignore
        return 'DataFramePin', None

    @staticmethod
    def color():  # type: ignore
        return DB_DATAFRAME_PIN_COLOR

    @staticmethod
    def internalDataStructure():
        return DataFrameData

    @staticmethod
    def processData(data):
        return data

    @staticmethod
    def jsonEncoderClass():
        """Returns json encoder class for this pin"""
        return DataFrameEncoder

    @staticmethod
    def jsonDecoderClass():
        """Returns json decoder class for this pin"""
        return DataFrameDecoder
