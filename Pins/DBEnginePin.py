"""Implementation of a PyFlow Pin to carry database connections"""
import json
from sqlalchemy import Engine, create_engine, exc

from PyFlow.Core.Common import PinOptions
from PyFlow.Core import PinBase
from ..constants import DB_ENGINE_PIN_COLOR  # pylint: disable=relative-beyond-top-level

class DBEngineData:
    """Internal data structure for DBEnginePin"""
    def __init__(self, value = None) -> None:
        self.value = value.engine.url if value is not None else None

class DBEngineEncoder(json.JSONEncoder):
    """Encode DBEngine values to json"""
    def default(self, o):
        if isinstance(o, Engine):
            return {
                "_type": "DBEngineData",
                "engineurl": o.engine.url
            }
        return super().default(o)
    
class DBEngineDecoder(json.JSONDecoder):
    """Decode DBEngine values from json"""
    def __init__(self, *args, **kwargs):
        if 'object_hook' in kwargs:
            self._passed_object_hook = kwargs['object_hook']
            del kwargs['object_hook']
        super().__init__(*args, object_hook=self._object_hook, **kwargs)
    
    def _object_hook(self, o):  # type: ignore
        """Decode DBEngineData objects"""
        if '_type' in o:
            if o['_type']=='DBEngineData':
                try:
                    return create_engine(o['engineurl'])
                except exc.DBAPIError:
                    return None
        if callable(self._passed_object_hook):
            return self._passed_object_hook(o)
        return o


class DBEnginePin(PinBase):
    """Pin transferring DB Engine connections between nodes"""

    def __init__(self, name, owningNode, direction, **kwargs):
        super().__init__(name, owningNode, direction, **kwargs)
        self.disableOptions(PinOptions.Storable)
        self.setDefaultValue(None)

    @staticmethod
    def IsValuePin():
        return True

    @staticmethod
    def supportedDataTypes():  # type: ignore
        return ('DBEnginePin', )

    @staticmethod
    def pinDataTypeHint():  # type: ignore
        return 'DBEnginePin', None

    @staticmethod
    def color():  # type: ignore
        return DB_ENGINE_PIN_COLOR

    @staticmethod
    def internalDataStructure():
        return DBEngineData

    @staticmethod
    def processData(data):
        return data

    @staticmethod
    def jsonEncoderClass():
        """Returns json encoder class for this pin"""
        return DBEngineEncoder

    @staticmethod
    def jsonDecoderClass():
        """Returns json decoder class for this pin"""
        return DBEngineDecoder
