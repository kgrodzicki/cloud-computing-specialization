__author__ = 'grokrz'

m = 32


def hash_function(x, i):
    return ((pow(x, 2) + pow(x, 3)) * i) % m


def show_bits_set_to_1(value):
    for i in range(1, 4):
        print "Bit set to 1: " + str(hash_function(value, i))


values = [2010, 2013, 2007]

for val in values:
    show_bits_set_to_1(val)