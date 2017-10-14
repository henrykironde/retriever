from __future__ import division
from __future__ import print_function

from future import standard_library

standard_library.install_aliases()
from builtins import object
from builtins import range
from builtins import input
from builtins import zip
from builtins import next
from builtins import str
import json
import sys
import pprint
from collections import OrderedDict

if sys.version_info[0] < 3:
    from codecs import open
from retriever.lib.templates import TEMPLATES
# from retriever.lib.models import Cleanup, correct_invalid_value
from retriever.lib.models import myTables # TableMain, Table, TableRaster, TableVector


def add_dialect(table_dict, table):
    """
    Reads dialect key of JSON script and extracts key-value pairs to store them
    in python script

    Contains properties such 'missingValues', delimiter', etc
    """
    for (key, val) in table['dialect'].items():
        # dialect related key-value pairs
        # copied as is
        if key == "missingValues":
            table_dict['cleanup'] = "Cleanup(correct_invalid_value, missing_values=val)"

        elif key == "delimiter":
            table_dict[key] = str(val)
        else:
            table_dict[key] = val


def add_schema(table_dict, table):
    """
    Reads schema key of JSON script and extracts values to store them in
    python script

    Contains properties related to table schema, such as 'fields' and cross-tab
    column name ('ct_column').
    """
    for (key, val) in table['schema'].items():
        # schema related key-value pairs

        if key == "fields":
            # fields = columns of the table

            # list of column tuples
            column_list = []
            for obj in val:
                # fields is a collection of JSON objects
                # (similar to a list of dicts in python)

                if "size" in obj:
                    column_list.append((obj["name"],
                                        (obj["type"], obj["size"])))
                else:
                    column_list.append((obj["name"],
                                        (obj["type"],)))

            table_dict["columns"] = column_list

        elif key == "ct_column":
            table_dict[key] = "'" + val + "'"

        else:
            table_dict[key] = val


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
    # print(json_file)
    if type(json_object) is dict and "resources" in json_object.keys():
        if "format" not in json_object:
            json_object["format"] = "tabular"
        resource_dict ={}
        for resource_item in json_object["resources"]:
            # Check for required resource fields
            spec_list = ["name", "url"]

            rspec = set(spec_list)
            if not rspec.intersection(resource_item.keys()) == rspec:
                raise ValueError("Check either {} fields in  Package {}".format(rspec, json_file))

            for spec in spec_list:
                if not resource_item[spec]:
                    raise ValueError("Check either {} for missing values.\n Package {}".format(rspec, json_file))

            # create tables from list of resources
            # for (t_key, t_val) in resource_item.items():

        json_object["tables"] = {}
        temp_tables = {}
        table_names = [item["name"] for item in json_object["resources"]]
        temp_tables["tables"] = dict(zip(table_names, json_object["resources"]))

        for table_name, table_spec in temp_tables["tables"].items():
            # print(type(table_spec))
            json_object["tables"][table_name] = myTables[json_object["format"]](**table_spec)

        # json_object.pop("resources", None)

        return TEMPLATES["default"](**json_object)
    return None

# if __name__ == '__main__':
#     compile_json("C:/Users/Henry/Documents/GitHub/retriever/scripts/croche_vegetation_data")
#     # compile_json("C:/Users/Henry/Documents/GitHub/weav/scripts/cumbria")
#     # compile_json("C:/Users/Henry/Documents/GitHub/retriever/scripts/breast_cancer_wi")