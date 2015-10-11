#! /usr/bin/env python -u
# coding=utf-8
import os

__author__ = 'Sayed Hadi Hashemi'

import json
from CourseraSubmission import CourseraSubmission


class MP5(CourseraSubmission):
    def __init__(self):
        super(CourseraSubmission, self).__init__()
        dev = False
        self.part_ids = ['mp5-part-a', 'mp5-part-b', 'mp5-part-c', 'mp5-part-d']
        if dev:
            self.part_ids = [part_id + "-dev" for part_id in self.part_ids]

        PREFIX = os.environ["PREFIX"]
        self.course_id = 'cloudapplications-001'
        self.part_names = ["Connected Components of Graph", "KMeans Clustering", "Shortest Paths in Graph", "Random Forest Classifier"]
        self.files_results = [os.path.join(PREFIX,"a.output"), os.path.join(PREFIX,"b.output"), os.path.join(PREFIX,"c.output"), os.path.join(PREFIX,"d.output")]
        self.files_codes = [
            ["./src/ConnectedComponentsComputation.java", os.path.join(PREFIX, "a.log")],
            ["./src/KMeansMP.java", os.path.join(PREFIX, "b.log")],
            ["./src/ShortestPathsComputation.java", os.path.join(PREFIX, "c.log")],
            ["./src/RandomForestMP.java", os.path.join(PREFIX, "d.log")]
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
        ret = True
        ret = ret and os.path.exists(self.files_results[part_index])
        for filename in self.files_codes[part_index]:
            ret = ret and os.path.exists(filename)
        return ret

if __name__ == "__main__":
    mp1 = MP5()
    mp1.make_sumbission()
