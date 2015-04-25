__author__ = 'grokrz'

h1 = 10.0 / 10.0 * 100
h2 = 15.0 / 15.0 * 100
h3 = 13.25 / 15.0 * 100
h4 = 10.0 / 10.0 * 100
h5 = 9.0 / 10.0 * 100
final_exam = 33.5 / 40 * 100

homework = [h1, h2, h3, h4, h5]
print "Final exam: {}%".format(final_exam)

h_res = (h1 + h2 + h3 + h4 + h5) / 5
print "Homework: {}%".format(h_res)

print "All weekly homeworks is 60%, the final exam is 40%: {}%".format(h_res * 60/100 + final_exam * 40/100)
