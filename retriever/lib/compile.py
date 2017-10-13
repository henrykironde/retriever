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

    if type(json_object) is not dict:
        raise ValueError("not a json dictionary, Format file correctly")

    if "retriever" not in json_object.keys():
        # Compile only files that have retriever key
        return

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
        for (t_key, t_val) in resource_item.items():

    # table_names = [item["name"] for item in json_object["resources"]]
    # json_object["tables"] = dict(zip(table_names, json_object["resources"]))
    # json_object.pop("resources", None)

        # print(["name", "url"] in resource_item.keys())
        # for key, values in resource_item.items():
        #
        #
        #     if key == "name":
        #
        #     resource_dict["name"] =  resource_item["name"]
        # if "dialect" in resource_item.items():



    # table_names = [item["name"] for item in json_object["resources"]]
    # json_object["tables"] = dict(zip(table_names, json_object["resources"]))
    #
    # json_object["tables"] = dict(zip(table_names,  json_object["resources"]).items())
    # json_object.pop("resources", None)
    #
    # table_obj = {}
    #
    # for table_name, table_schema in json_object["tables"].items():
    #     table_obj[table_name] = myTables[json_object["format"]]
    #
    # c = table_names
    # print(table_obj)


    #  for item in json_object["resources"]:
    #      json_object["tables"][item["name"]]= item

    # json_object["tables"]= json_object["resources"]
    # # for item in json_object["resources"]:
    # #     json_object["tables"][item["name"]]= item

    # print(json_object["tables"])

    exit()



    # exit()
    # values = {'urls': {}}
    # required_fields = {
    #     "name": "name",
    #     "tables": "tables"
    # }
    #
    # for (key, value) in json_object.items():
    #
    #     if key == "title":
    #         values["title"] = str(value)
    #
    #     elif key == "name":
    #         values["name"] = str(value)
    #
    #     elif key == "description":
    #         values["description"] = str(value)
    #
    #     elif key == "addendum":
    #         values["addendum"] = str(value)
    #
    #     elif key == "homepage":
    #         values["ref"] = str(value)
    #
    #     elif key == "citation":
    #         values["citation"] = str(value)
    #
    #     elif key == "licenses":
    #         values["licenses"] = value
    #
    #     elif key == "keywords":
    #         values["keywords"] = value
    #
    #     elif key == "version":
    #         values["version"] = str(value)
    #
    #     elif key == "encoding":
    #         values["encoding"] = str(value)
    #         # Adding the key 'encoding'
    #         source_encoding = str(value)
    #
    #     elif key == "retriever_minimum_version":
    #         values["retriever_minimum_version"] = str(value)
    #
    #     elif key == "message":
    #         values["message"] = str(value)
    #     elif key == "format"
    #         values["vector"] =
    #     elif key == "resources":
    #
    #         # Array of table objects
    #         tables = {}
    #         for table in value:
    #             table_1 = co
    #             tables_1[name]
    #             # Maintain a dict for table keys and values
    #             table_dict = {}
    #
    #             try:
    #                 values['urls'][table['name']] = table['url']
    #             except Exception as e:
    #                 print(e, "\nError in reading table: ")
    #                 pp.pprint(table)
    #                 continue
    #
    #             if table["schema"] == {} and table["dialect"] == {}:
    #                 continue
    #
    #             for (t_key, t_val) in table.items():
    #
    #                 if t_key == "dialect":
    #                     add_dialect(table_dict, table)
    #
    #                 elif t_key == "schema":
    #                     add_schema(table_dict, table)
    #
    #             tables[table["name"]] = table_dict
    #
    #     else:
    #         values[key] = value
    #
    # # Create a Table object string using the tables dict
    # table_obj = {}
    # for (key, value) in tables.items():
    #     # if
    #     table_obj[key] = Table(key, **value)
    #
    # values["tables"] = table_obj
    #
    # if 'template' in values.keys():
    #     template = values["template"]
    # else:
    #     template = "default"
    #
    # check=True
    # fields = []
    # for item in required_fields.keys():
    #     if item not in values:
    #         fields.append(required_fields[item])
    #         check = False
    #
    # if check:
    #     if debug:
    #         print("Values being passed to template: ")
    #         pp.pprint(values)
    #     return TEMPLATES[template](**values)
    # else:
    #     print(json_file + " is missing parameters: \n")
    #     print(fields)
    #     sys.exit()

if __name__ == '__main__':
    compile_json("C:/Users/Henry/Documents/GitHub/retriever/scripts/croche_vegetation_data")
    # compile_json("C:/Users/Henry/Documents/GitHub/weav/scripts/cumbria")
    # compile_json("C:/Users/Henry/Documents/GitHub/retriever/scripts/breast_cancer_wi")