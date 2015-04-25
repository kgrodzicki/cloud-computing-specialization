__author__ = 'grokrz'
#
# P1 = ['P1', 'P2', 'P4', 'P6']
# P2 = ['P1', 'P2', 'P3', 'P4']
# P3 = ['P1', 'P2', 'P3', 'P5']
# P4 = ['P1', 'P2', 'P4', 'P5']
# P5 = ['P3', 'P4', 'P5', 'P6']
# P6 = ['P1', 'P3', 'P5', 'P6']

P1 = ['P1', 'P2']
P2 = ['P2', 'P3']
P3 = ['P3', 'P4']
P4 = ['P4', 'P5']
P5 = ['P5', 'P1']

ALL = [P1, P2, P3, P4, P5]


def check(processes):
    result = []
    for p0 in processes:
        for p1 in processes:
            if p0 != p1:
                print "checking intersection between {} and {}".format(p0, p1)
                intersection = set(p0).intersection(p1)
                print intersection
                result.append(intersection != set())
    return result


is_safe = False in check(ALL)

print "Is safe = {}".format(not is_safe)
