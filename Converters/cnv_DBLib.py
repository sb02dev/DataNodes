"""Converters for the DBLib"""  # pylint: disable=invalid-name

try:
    from PyFlow.Packages.PythonExporter.Exporters.converter_base import (  # pylint: disable=import-error, no-name-in-module # type: ignore
        ConverterBase
    )
except ImportError:
    from PythonExporter.Exporters.converter_base import ConverterBase

class PyCnvDBLib(ConverterBase):  # type: ignore
    """A converter class for the DBLib conversion"""
