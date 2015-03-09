__author__ = 'grokrz'


def is_less(v0, v1):
    for idx in [0, 1, 2, 3]:
        if not v0[idx] <= v1[idx]:
            return False
    return True


given = [1, 4, 2, 0]
vectors = [
    [1, 0, 0, 0],
    [2, 0, 0, 0],
    [3, 5, 2, 0],
    [4, 5, 2, 0],
    [5, 5, 2, 0],
    [0, 1, 0, 0],
    [0, 2, 0, 0],
    [1, 6, 2, 0],
    [1, 3, 2, 0],
    [1, 5, 2, 0],
    [1, 7, 2, 0],
    [1, 0, 1, 0],
    [1, 0, 2, 0],
    [1, 0, 3, 0],
    [1, 0, 4, 0],
    [5, 5, 5, 0],
    [5, 5, 6, 0],
    [5, 5, 7, 0],
    [5, 8, 6, 5],
    [0, 0, 0, 1],
    [1, 7, 2, 2],
    [1, 7, 3, 3],
    [5, 7, 6, 4],
    [5, 7, 6, 5]
]

concurrent_vectors = 0
for v in vectors:
    if not is_less(given, v) and not is_less(v, given):
        print "vector " + str(v) + " is concurrent to vector " + str(given)
        concurrent_vectors += 1

print concurrent_vectors
