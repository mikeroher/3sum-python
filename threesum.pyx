#cython: language_level=3, boundscheck=False, wraparound=False, infer_types=False

#access to NumPy-Python functions,
import numpy as np
#access to Numpy C API
cimport numpy as np
from itertools import islice

# DTYPE for this, which is assigned to the usual NumPy runtime
# type info object.
# IMPORTANT - MAKE SURE ALL MATHEMATICAL OPERATIONS USE THE SAME DATATYPE. Otherwise, operations
# will introduce different integer types resulting in different hashing values.
cdef DTYPE = np.short

cdef class RowPair:
    """
    Wrapper for two row vectors and calculates the column wise sum

    Keyword arguments:
        rowA, '1xm'-dimensional numpy array
        rowB, '1xm'-dimensional numpy array

    Returns the RowPair wrapper
    """
    cdef public:
        np.ndarray rowA
        np.ndarray rowB
        np.ndarray row_sum

    __slots__ = ['rowA', 'rowB', 'row_sum']
    def __init__(self, np.ndarray rowA, np.ndarray rowB):
        self.rowA = rowA
        self.rowB = rowB
        self.row_sum = np.sum(np.column_stack((rowA, rowB)), axis=1, dtype=DTYPE)

    def __eq__(self, other):
        return np.array_equal(self.rowA, other.rowA) and np.array_equal(self.rowB, other.rowB)

    def __hash__(self):
        return hash(np.column_stack((self.rowA, self.rowB)).data.tobytes())

# Since, numpy's loadtxt and genfromtxt do a lot of guesswork and error checking,
# we're going to create our own read file array.
def iter_loadtxt(str filename, str delimiter):
    cdef:
        int rowlength = -1
        str line = None
        str line_str = None
        np.ndarray data
    def iter_func():
        with open(filename, 'r') as infile:
            for line_str in infile:
                line = line_str.rstrip().split(delimiter)
                for item in line:
                    yield DTYPE(item)
        nonlocal rowlength
        if (rowlength < 0):
            rowlength = len(line)

    data = np.fromiter(iter_func(), dtype=DTYPE)
    # One colum can be returned as -1 ==>
    data = data.reshape((-1, rowlength))
    return data

cpdef chunk_dataframe(np.ndarray df, int n):
    assert n > 0, "# of chunks must be greater than zero"
    #chunk_size = int(df.shape[0] / n)
    # will work even if the length of the dataframe is not evenly divisible by num_processes
    #chunks = [df.ix[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
    cdef list chunks = np.array_split(df, n, axis=0)
    return chunks

def chunk_hashtable(dict ht, short N):
    cdef:
        size_t i
        np.int64_t key
        object _iter = iter(ht)
        int dict_len = len(ht)
    for i in range(0, dict_len, N):
        yield {key: ht[key] for key in islice(_iter, N)}

cpdef sum_each_of_first_two_files(np.ndarray dfA, np.ndarray dfB):
    cdef:
        RowPair rowpair = None
        # Since insert operations are very slow, we're going to do all the
        # insertions at once. This is significantly faster.
        dict _hashtable = dict()

        size_t a, b
        # It is very important to type ALL your variables. You do not get any
        # warnings if not, only much slower code (they are implicitly typed as
        # Python objects).
        np.ndarray rowA, rowB

        # This must be an int64 as the hashed key is too large for a short
        np.int64_t key
        set value

        unsigned long LEN_A = dfA.shape[0]
        unsigned long LEN_B = dfB.shape[0]

    for a in range(LEN_A):
        for b in range(LEN_B):
            rowpair = RowPair(dfA[a], dfB[b])
            # Can't hash an intarray so we have to take the data as bytes (i.e. a string)
            key = hash(rowpair.row_sum.tobytes())
            value = _hashtable.get(key)

            # if rowpair.row_sum[6] == 143 and rowpair.row_sum[8] == 143 and rowpair.row_sum[0] == 143:
            #     print(rowpair.row_sum, hash(rowpair.row_sum.tobytes()))

            if value is None:
                _hashtable[key] = set([rowpair])
                # _hashtable[key] = [rowpair]
            else:
                _hashtable[key].add(rowpair)
                # _hashtable[key].append(rowpair)
    return _hashtable

cpdef find_differences_in_third_file(short [:, :] df, object hashtable, const short LAMBDA):
    cdef:
        np.int64_t key
        set value
        np.ndarray difference
        unsigned long LEN_C = df.shape[0]
        size_t c

        list matches = []
        tuple match = None
        RowPair rowpair = None

    for c in range(LEN_C):
        # The values call is necessary because np.subtract here returns the original
        # data type which is a Pandas series. This way we get an nparray instead where
        # we can call the data.
        difference = np.subtract(LAMBDA, df[c], dtype=DTYPE)
        # if difference[6] == 143 and difference[8] == 143 and difference[0] == 143:
        #     print(difference, hash(difference.tobytes()))
        key = hash(difference.tobytes())
        value = hashtable.get(key)

        if value is not None:
            for rowpair in value:
                match = (tuple(rowpair.rowA), tuple(rowpair.rowB), tuple(df[c]))
                matches.append(match)
    return matches