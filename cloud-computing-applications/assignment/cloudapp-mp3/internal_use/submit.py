#! /usr/bin/env python -u
# coding=utf-8
import os

__author__ = 'Sayed Hadi Hashemi'

import json
from CourseraSubmission import CourseraSubmission


class MP3(CourseraSubmission):
    def __init__(self):
        super(CourseraSubmission, self).__init__()
        dev = False
        self.part_ids = ['mp3-part-a', 'mp3-part-b', 'mp3-part-c', 'mp3-part-d']
        for part_id in self.part_ids:
            if dev:
                part_id += "-dev"

        self.course_id = 'cloudapplications-001'
        self.part_names = ["Top Word Finder Topology", "File Reader Spout", "Normalizer Bolt", "Top N Finder Bolt"]
        self.files_results = ["output-part-a.txt", "output-part-b.txt", "output-part-c.txt", "output-part-d.txt"]
        self.files_codes = [
            ["src/TopWordFinderTopologyPartA.java"],
            ["src/FileReaderSpout.java", "src/TopWordFinderTopologyPartB.java"],
            ["src/NormalizerBolt.java", "src/TopWordFinderTopologyPartC.java"],
            ["src/TopNFinderBolt.java", "src/TopWordFinderTopologyPartD.java"]
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
        return self.get_file_content(self.files_results[part_index])[:400*1024]

    def is_enabled(self, part_index):
        ret = True
        ret = ret and os.path.exists(self.files_results[part_index])
        for filename in self.files_codes[part_index]:
            ret = ret and os.path.exists(filename)
        return ret

if __name__ == "__main__":
    mp1 = MP3()
    mp1.make_sumbission()
