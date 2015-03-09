__author__ = 'grokrz'

m = 32


def hash_function(x, i, m):
    return ((pow(x, 2) * pow(x, 3)) * i) % m


def show_bits_set_to_1(val):
    for i in range(1, 4):
        print "Bit set to 1: " + str(hash_function(val, i, m))


show_bits_set_to_1(2013)