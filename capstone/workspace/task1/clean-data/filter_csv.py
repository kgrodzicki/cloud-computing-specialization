#!/usr/bin/python

__author__ = 'Krzysztof Grodzicki <kgrodzicki@gmail.com> 23.01.2016'

import csv
import os
import zipfile
import sys


def build(row, keys):
    keys = map(lambda key: row[key], keys)
    return ",".join(keys) + "\n"


def main(arg):
    if len(arg) < 3:
        raise ValueError("source and/or dest and/or keys missing")
    source_dir = arg[0]
    if os.path.isdir(source_dir) is False:
        raise ValueError("wrong source")
    destination_dir = arg[1]
    if os.path.isdir(destination_dir) is False:
        raise ValueError("wrong dest")

    for root, dirs, files in os.walk(source_dir):
        for f in files:
            if f.endswith(".zip"):
                try:
                    join = os.path.join(root, f)
                    print "Processing: " + join
                    z = zipfile.ZipFile(join)
                    for name in z.namelist():
                        if ".csv" in name:
                            dest_abspath = os.path.abspath(destination_dir)
                            dest_file = os.path.join(dest_abspath, name + ".txt")
                            out = open(dest_file, "w")
                            reader = csv.DictReader(z.open(name))
                            keys = arg[2:]
                            print "using keys: " + ",".join(keys)
                            for row in reader:
                                out.write(build(row, keys))
                            out.close()
                except zipfile.BadZipfile:
                    print "Bad zip file"
                except:
                    print "unknown error"
    print "Done"


if __name__ == "__main__":
    main(sys.argv[1:])
