__author__ = 'grokrz'


def is_less(v0, v1):
    for idx in [0, 1, 2, 3]:
        if not v0[idx] <= v1[idx]:
            return False
    return True


given = [0, 0, 0, 2]
vectors = [
    [1, 0, 0, 0],
    [2, 0, 0, 0],
    [3, 0, 0, 0],
    [4, 3, 2, 1],
    [5, 3, 2, 1],
    [6, 3, 2, 1],
    [0, 1, 2, 1],
    [0, 2, 2, 1],
    [0, 3, 2, 1],
    [0, 4, 2, 2],
    [2, 5, 2, 2],
    [0, 0, 1, 1],
    [0, 0, 2, 1],
    [1, 0, 2, 1],
    [5, 3, 3, 1],
    [5, 3, 4, 1],
    [5, 3, 5, 4],
    [6, 3, 6, 4],
    [0, 0, 0, 1],
    # [0,0,0,2],
    [3, 0, 0, 3],
    [3, 0, 0, 4]
]

concurrent_vectors = 0
for v in vectors:
    if not is_less(given, v) and not is_less(v, given):
        print "vector " + str(v) + " is concurrent to vector " + str(given)
        concurrent_vectors += 1

print concurrent_vectors
