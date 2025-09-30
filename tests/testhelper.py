import os
import difflib
from glob import glob

from PyFlow.Core.PyCodeCompiler import Py3CodeCompiler


def get_test_names(category: str):
    """Gets the test names from the graph folders, based on the category prefix"""
    fnames = glob(f"{category}_*.pygraph", root_dir="tests/graphs")
    tnames = [os.path.splitext(fname)[0][(len(category)+1):] for fname in fnames]
    return tnames


def run_export_and_compare(pycnv, testfolder, test_name):
    """Loads the graph, runs an export on it, compares the file to the
    expected, prints the diff and asserts equality.
    """
    fname_graph = os.path.join(testfolder, 'graphs', test_name+'.pygraph')
    res_folder = os.path.join(testfolder, 'conv')
    if not os.path.exists(res_folder):
        os.mkdir(res_folder)
    fname_result = os.path.join(res_folder, test_name+'.py')
    fname_expected = os.path.join(testfolder, 'expectedconv', test_name+'.py')

    pycnv.graphLoader(fname_graph)
    pycnv.exporter(pycnv.app, fname_result)

    with open(fname_result, 'r', encoding='utf8') as f1, \
         open(fname_expected, 'r', encoding='utf8') as f2:
        f1_lines = f1.readlines()
        f2_lines = f2.readlines()

    diff = difflib.unified_diff(f1_lines, f2_lines, 
                                fromfile='result', tofile='expected')
    print(''.join(diff))

    assert len(f1_lines)==len(f2_lines), "Line count difference"
    for i, line1 in enumerate(f1_lines):
        if i!=4: # because that line contains the date
            assert line1==f2_lines[i], \
                   f"Line {i} differs\n--  {line1}\n++  {f2_lines[i]}"


def run_graph_and_return(pycnv, testfolder, test_name, exec_pin_name):
    """Loads a graph, runs it from a specified pin then returns the graphManager
    to get the results"""
    fname_graph = os.path.join(testfolder, 'graphs', test_name+'.pygraph')
    pycnv.graphLoader(fname_graph)
    # run
    gman = pycnv.app.graphManager.get()
    gman.findPinByName(exec_pin_name).call()
    return gman

def run_graph_in_python(pycnv, testfolder, test_name):
    """Loads a graph, exports to python, runs the python and returns the
    results to assert them."""
    fname_graph = os.path.join(testfolder, 'graphs', test_name+'.pygraph')
    res_folder = os.path.join(testfolder, 'conv')
    if not os.path.exists(res_folder):
        os.mkdir(res_folder)
    fname_result = os.path.join(res_folder, test_name+'.py')
    # load graph and export to python
    pycnv.graphLoader(fname_graph)
    pycnv.exporter(pycnv.app, fname_result)
    # run python
    with open(fname_result, 'r', encoding='utf8') as f1:
        pycode = ''.join(f1.readlines())
    cmp = Py3CodeCompiler()
    mem = cmp.compile(pycode, "ModuleToTest")
    # return the results of the run
    return mem
