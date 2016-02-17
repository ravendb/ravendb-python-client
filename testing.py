import inspect


def smallest_diff_key(A, B):
    """return the smallest key adiff in A such that A[adiff] != B[bdiff]"""
    diff_keys = [k for k in A if A.get(k) != B.get(k)]
    if len(diff_keys) == 0:
        return None
    return min(diff_keys)


def dict_cmp(A, B):
    if len(A) != len(B):
        return cmp(len(A), len(B))
    adiff = smallest_diff_key(A, B)
    bdiff = smallest_diff_key(B, A)
    if adiff != bdiff or bdiff is None or adiff is None:
        return cmp(adiff, bdiff)

    return cmp(A[adiff], B[bdiff])


class Foo(object):
    def __init__(self, number_a=1, number_b=2, number_c=2):
        self.number_a = number_a
        self.number_b = number_b
        self.number_c = number_c

    def plus(self):
        self.number_d = self.number_a + self.number_b
        return self.number_d


if __name__ == "__main__":
    c = Foo(3, 4)
    s = "sdsdasd"
    print type(c)
    g = c.__class__.__name__+"s"
    print(g)
    print c.__class__
