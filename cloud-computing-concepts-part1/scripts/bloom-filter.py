__author__ = 'grokrz'

m = 64


def hash_function(x, i):
    return (x * i) % m


def show_bits_set_to_1(value):
    for i in range(1, 3):
        print "Bit set to 1: " + str(hash_function(value, i))


values = [1975, 1985, 1995, 2005]
map(show_bits_set_to_1, values)

show_bits_set_to_1(2015)
