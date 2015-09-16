#! /usr/bin/env python -u
# coding=utf-8
import os

__author__ = 'Sayed Hadi Hashemi'

import json
from CourseraSubmission import CourseraSubmission


class MP4(CourseraSubmission):
    def __init__(self):
        super(CourseraSubmission, self).__init__()
        dev = False
        self.part_ids = ['mp4-part-a']
        for part_id in self.part_ids:
            if dev:
                part_id += "-dev"

        self.course_id = 'cloudapplications-001'
        self.part_names = ["Create and Populate an HBase Table"]
        PREFIX = os.environ["PREFIX"]

        self.files_results = [os.path.join(PREFIX, "SuperTable.output")]
        self.files_codes = [
            [os.path.join(PREFIX, "SuperTable.java"),
             os.path.join(PREFIX, "SuperTable.log"),
             os.path.join(PREFIX, "SuperTable.hbase"),
             ]
        ]

    @staticmethod
    def get_file_content(file_name):
        with open(file_name, "r") as fp:
            return fp.read()

    def aux(self, part_index):
        ret = {}
        for filename in self.files_codes[part_index]:
            ret[filename] = self.get_file_content(filename)
        return json.dumps(ret)

    def output(self, part_index):
        return self.get_file_content(self.files_results[part_index])

    def is_enabled(self, part_index):
        return True

if __name__ == "__main__":
    mp1 = MP4()
    mp1.make_sumbission()
