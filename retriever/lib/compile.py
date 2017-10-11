from __future__ import division
from __future__ import print_function

from future import standard_library

standard_library.install_aliases()
from builtins import zip
from builtins import str
import json
import sys
import pprint
import os
import imp
from collections import OrderedDict

if sys.version_info[0] < 3:
    from codecs import open
from retriever.lib.templates import TEMPLATES
from retriever.lib.models import myTables
from retriever.lib.defaults import SCRIPT_SEARCH_PATHS
from os.path import join, isfile, getmtime, exists

def MODULE_LIST(force_compile=False):
    """Load scripts from scripts directory and return list of modules."""
    modules = []
    loaded_scripts = []
    
    for search_path in [search_path for search_path in SCRIPT_SEARCH_PATHS if exists(search_path)]:
        to_compile = [
            file for file in os.listdir(search_path) if file[-5:] == ".json" and
            file[0] != "_" and (
                (not isfile(join(search_path, file[:-5] + '.py'))) or (
                    isfile(join(search_path, file[:-5] + '.py')) and (
                        getmtime(join(search_path, file[:-5] + '.py')) < getmtime(
                            join(search_path, file)))) or force_compile)]
        for script in to_compile:
            script_name = '.'.join(script.split('.')[:-1])
            if script_name not in loaded_scripts:
                compiled_script = compile_json(join(search_path, script_name))
                setattr(compiled_script, "_file", os.path.join(search_path, script))
                setattr(compiled_script, "_name", script_name)
                modules.append(compiled_script)
                loaded_scripts.append(script_name)

        files = [file for file in os.listdir(search_path)
                 if file[-3:] == ".py" and file[0] != "_" and
                 '#retriever' in ' '.join(open(join(search_path, file), 'r').readlines()[:2]).lower()]


        for script in files:
            script_name = '.'.join(script.split('.')[:-1])
            if script_name not in loaded_scripts:
                loaded_scripts.append(script_name)
                file, pathname, desc = imp.find_module(script_name, [search_path])
                try:
                    new_module = imp.load_module(script_name, file, pathname, desc)
                    if hasattr(new_module, "retriever_minimum_version"):
                        # a script with retriever_minimum_version should be loaded
                        # only if its compliant with the version of the retriever
                        if not parse_version(VERSION) >= parse_version("{}".format(
                                new_module.retriever_minimum_version)):
                            print("{} is supported by Retriever version "
                                  "{}".format(script_name, new_module.retriever_minimum_version))

                            print("Current version is {}".format(VERSION))
                            continue
                    # if the script wasn't found in an early search path
                    # make sure it works and then add it
                    new_module.SCRIPT.download
                    setattr(new_module.SCRIPT, "_file", os.path.join(search_path, script))
                    setattr(new_module.SCRIPT, "_name", script_name)
                    modules.append(new_module.SCRIPT)

def compile_json(json_file, debug=False):
    """
    Function to compile JSON script files to python scripts
    The scripts are created with `retriever new_json <script_name>` using
    command line
    """
    json_object = OrderedDict()
    json_file = str(json_file) + ".json"

    try:
        json_object = json.load(open(json_file, "r"))
    except ValueError:
        pass

    if type(json_object) is dict and "resources" in json_object.keys():

        # Note::formats described by frictionlessdata data may need to change
        tabular_formats = ["csv", "tab"]
        vector_formats = ["shp", "kmz"]
        raster_formats = ["tif","tiff" "bil", ".hdr", "h5","hdf5", "hr", "image"]

        for resource_item in json_object["resources"]:
            if "format" in resource_item:
                if resource_item["format"] in tabular_formats:
                    resource_item["format"] = "tabular"
                elif resource_item["format"] in vector_formats:
                    resource_item["format"] = "vector"
                elif resource_item["format"] in raster_formats:
                    resource_item["format"] = "raster"
            else:
                resource_item["format"] = "tabular"

            # Check for required resource fields
            spec_list = ["name", "url"]

            rspec = set(spec_list)
            if not rspec.intersection(resource_item.keys()) == rspec:
                raise ValueError("Check either {} fields in  Package {}".format(rspec, json_file))

            for spec in spec_list:
                if not resource_item[spec]:
                    raise ValueError("Check either {} for missing values.\n Package {}".format(rspec, json_file))

        json_object["tables"] = {}
        temp_tables = {}
        table_names = [item["name"] for item in json_object["resources"]]
        temp_tables["tables"] = dict(zip(table_names, json_object["resources"]))

        for table_name, table_spec in temp_tables["tables"].items():
            json_object["tables"][table_name] = myTables[temp_tables["tables"][table_name]["format"]](**table_spec)
        json_object.pop("resources", None)

        return TEMPLATES["default"](**json_object)
    return None
