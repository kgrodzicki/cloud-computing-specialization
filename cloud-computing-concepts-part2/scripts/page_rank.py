__author__ = 'grokrz'


class Page:
    def __init__(self, page_rank=0, degree=0):
        self.page_rank = page_rank
        self.degree = degree

    def get_page_rank(self):
        return self.page_rank

    def get_degree(self):
        return self.degree


pages_in_neighbour = [Page(2.0, 1.0), Page(1.0, 1.0)]

acc = 0
for q in pages_in_neighbour:
    acc += (q.page_rank / q.degree)

print "PageRank: 0.85 * acc + 0.15 = {}".format(0.85 * acc + 0.15)
