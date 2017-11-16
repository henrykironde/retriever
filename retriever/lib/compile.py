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

def compile_json(json_file, debug=False):
    """
    Function to compile JSON script files to python scripts
    The scripts are created with `retriever new_json <script_name>` using
    command line
    """
    json_object = OrderedDict()
    json_file = str(json_file) + ".json"
    pp = pprint.PrettyPrinter(indent=1)

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

        if debug:

            pprint_objects = json_object

            for item in pprint_objects["tables"]:
                pprint_objects["tables"][item] = json_object["tables"][item].__dict__

            print("Values being passed to template: ")
            pp.pprint(pprint_objects)            


        return TEMPLATES["default"](**json_object)
    raise ValueError("\n\nMissing \"resources\" in Package {}".format(json_file))
    return None
