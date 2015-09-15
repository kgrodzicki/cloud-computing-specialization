#! /usr/bin/env python -u
# coding=utf-8
import os
import random

__author__ = 'xl'


def generate_by_user(user_id):
    r = random.Random(str(user_id))
    n = r.randint(1, 5) * 5
    patch_number = r.randint(1, 5)
    return n, patch_number


def save(user_id, n, patch_number):
    XL_HOME = os.environ["XL_HOME"]
    with open(os.path.join(XL_HOME, "user_setting.sh"), "w") as fp:
        fp.writelines([
            "export USER_ID=%s\n" % user_id,
            "export DATASET_N=%s\n" % n,
            "export DATASET_PATCH=%s\n" % patch_number,
        ])


if __name__ == "__main__":
    coursera_user_id = raw_input('Coursera User ID: ')
    save(coursera_user_id, *generate_by_user(coursera_user_id))
