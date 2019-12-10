import csv
path = "/Users/henrykironde/Downloads/buildbps/kk/AK_BOUNDARY.csv"

import re
import os

p = re.compile("(\"\d+\")")


def hexrepl(match):
    "Return the hex string for a decimal number"
    value = match.group()
    l = len(value)
    return "k"*l


newpath = "/Users/henrykironde/Downloads/all_new_fia/fia"

for path, _, files in os.walk("/Users/henrykironde/Downloads/newfia/fia"):
    for file_n in files:

        if file_n.startswith("."):
            continue
        di = os.path.join(newpath, file_n[:2])
        if not os.path.exists(di):
            os.mkdir(di)
        print(file_n[:2])

        pa_in = os.path.join("/Users/henrykironde/Downloads/newfia/fia", file_n[:2], file_n)
        pa_out = os.path.join(newpath, file_n[:2], file_n)

        print(pa_in)
        print(pa_out)

        with open(pa_in, 'r') as csv_file, open(pa_out, 'w') as csvout:
            # csv_file.readlines()
            c = 0
            for i in csv_file.readlines():
                if c == 0:
                    csvout.write(i)

                    # print(i)
                    c += 1
                else:
                    k = p.sub(hexrepl, i)
                    # print(k)
                    csvout.write(k)


# open
#     # csv_reader = csv.reader(csv_file, delimiter=',', )
#     # line_count = 0
#     # for row in csv_reader:
#     #     print(row)
#     #     line_count += 1