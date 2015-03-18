__author__ = 'grokrz'

log = True


class Node:
    def __init__(self, node_id=None, successor=None):
        self.node_id = node_id
        self.successor = successor
        self.leader = None
        self.__messages = []

    def get_successor_id(self):
        if self.successor:
            return self.successor.node_id
        return None

    def set_successor(self, node):
        self.successor = node

    def send_to_successor(self, message):
        if self.successor:
            self.successor.msg(message)

    def msg(self, message):
        self.__messages.append(message)
        self.log("RECEIVED MESSAGE", message)
        node_attr = message[1]
        if message[0] == 'election':
            if node_attr == self.node_id:
                self.leader = node_attr
                self.send_to_successor(('elected', self.node_id))
            elif node_attr < self.node_id:
                self.send_to_successor(('election', self.node_id))
            else:
                self.send_to_successor(message)
        elif message[0] is 'elected':
            self.leader = node_attr
            if node_attr == self.node_id:
                pass
            else:
                self.leader = node_attr
                self.send_to_successor(message)
        else:
            self.log("UNKNOWN MESSAGE", message)

    def initiate_leader_election(self):
        if self.successor:
            self.send_to_successor(('election', self.node_id))

    def number_of_received_messages(self):
        return len(self.__messages)

    def log(self, text, obj):
        if log:
            print "{} {} {}".format(self.node_id, text, obj)

    def __str__(self):
        return "[{}][{}] -> [{}]".format(self.node_id, self.leader, self.get_successor_id())


def make_ring(ids=[]):
    result = dict()
    for i in ids:
        result[i] = Node(i)

    for k, v in enumerate(ids):
        if k < len(ids) - 1:
            successor = result[ids[k + 1]]
            result[v].set_successor(successor)
    # close the ring
    last_key = ids[len(ids) - 1]
    result[last_key].set_successor(result[ids[0]])
    return result


def print_ring(val):
    for key, value in val.iteritems():
        print value


node_ids = [1, 2, 3, 4, 5]
N = len(node_ids)

ring = make_ring(node_ids)
ring[2].initiate_leader_election()
# print_ring(ring)
nr_of_messages = 0
for k, v in ring.iteritems():
    nr_of_messages += v.number_of_received_messages()
print "Number of messages: {}".format(nr_of_messages)






