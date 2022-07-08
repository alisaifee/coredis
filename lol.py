
def build_list_iter(n, v):
    return [i for i in range(n)]

def build_list(n, v):
    l = [None]*n
    for i in range(n):
        l[i] = i
    return l

def build_list_2(n, v):
    l = []
    for i in range(n):
        l.append(i)
    return l

def build_list_3(n, v):
    for i in range(n):
        v[i] = i

c = [None]*10000
for i in range(1000):
    build_list_3(10000, c)
