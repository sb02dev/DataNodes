# DataNodes for PyFlow

## Overview

This is an extension package for https://github.com/pedroCabrera/PyFlow.
PyFlow is a general purpose visual scripting framework for python.

The aim is to extend PyFlow with some data related nodes which can be
used to connect to databases, run sql scripts, download data, transfer
those data to Excel, etc.

It also exposes converters for the Python exporter package
(https://github.com/sb02dev/PythonExporter).

## Status & Contribution

Currently this project is in a *proof-of-concept* state, not ready for
production use.

I accept contributions as issue reports, code contributions through PRs,
even ideas how to go further from here.

## Install

When you have PyFlow set up, you can download this package from github
and put it in a folder which then you set up as *additional packages*
folder in PyFlow preferences.

## Development

Clone the repo and before doing any work, don't forget to change the
path of PyFlow in pytest.ini, .vscode/settings.json, .vscode/launch.json
