#! /usr/bin/env python -u
# coding=utf-8
import os

__author__ = 'Sayed Hadi Hashemi'

import json
from CourseraSubmission import CourseraSubmission


class MP1(CourseraSubmission):
    def __init__(self):
        super(CourseraSubmission, self).__init__()
        dev = False
        self.part_ids = ['mp1-part-a', 'mp1-part-b', 'mp1-part-c', 'mp1-part-d', 'mp1-part-e',
                         'mp1-part-f']
        for part_id in self.part_ids:
            if dev: part_id += "-dev"

        self.course_id = 'cloudapplications-001'
        self.part_names = ["Title Count", "Top Titles", "Top Title Statistics", "Orphan Pages", "Top Popular Links",
                           "Popularity League", ]
        self.part_codes = [part_name.replace(" ", "") for part_name in self.part_names]
        self.results = {}

    @staticmethod
    def get_file_content(file_name):
        with open(file_name, "r") as fp:
            return fp.read()

    def aux(self, part_index):
        part_code = self.part_codes[part_index]
        return self.results[part_code]['aux']

    def output(self, part_index):
        part_code = self.part_codes[part_index]
        return self.results[part_code]['output']

    def run(self):
        PREFIX = os.environ["PREFIX"]
        DATASET_N = os.environ["DATASET_N"]
        DATASET_PATCH = os.environ["DATASET_PATCH"]

        for part_code in self.part_codes:
            self.results[part_code] = {
                'output': self.get_file_content(os.path.join(PREFIX, part_code + ".hash")),
                'aux': json.dumps(dict(
                    source_code=self.get_file_content(os.path.join(PREFIX, part_code + ".java")),
                    response=self.get_file_content(os.path.join(PREFIX, part_code + ".output")),
                    n=DATASET_N,
                    patch=DATASET_PATCH,
                ))
            }


if __name__ == "__main__":
    mp1 = MP1()
    mp1.make_sumbission()
