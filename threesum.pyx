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


cdef dict hashtable = {}
# print("hashtable intialized only once as a global")

cdef class RowPair:
    """
    Wrapper for two row vectors and calculates the column wise sum.
    This class wraps the row vectors and is stored in the dictionary's
    value for the row sum. For example, the dictionary's key would be a
    sum, suppose 50. Then, the value of that dictionary would be a list
    of Rowpair objects, where each row_sum attribute is 50.

    Public variables exist for rowA, rowB and the row sum.

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
        # Align the two rows on top of each other, then sum down (i.e. each column).
        # Thus, the the input is two row vectors, each `1xn`. The output of the
        # column stack is a numpy array 2xn which is passed into np.sum resulting
        # in a 1xn vector holding the sum.
        self.row_sum = np.sum(np.column_stack((rowA, rowB)), axis=1, dtype=DTYPE)

    def __eq__(self, other):
        return np.array_equal(self.rowA, other.rowA) and np.array_equal(self.rowB, other.rowB)

    def __hash__(self):
        return hash(np.column_stack((self.rowA, self.rowB)).data.tobytes())

def iter_loadtxt(str filename, str delimiter):
    """
    Read a delimited `nxm` table into a numpy `nxm` table. Since, numpy's
    loadtxt and genfromtxt do a lot of guesswork and error checking,
    we're going to create our own read file array.

    :param str filename: The filename to read into a numpy array
    :param str delimiter: The delimeter to split each column by (typically a space " ")
    :return np.ndarray:
    """
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
    cdef list chunks = np.array_split(df, n, axis=0)
    return chunks

cpdef sum_each_of_first_two_files(np.ndarray dfA, np.ndarray dfB):
    global hashtable

    cdef:
        RowPair rowpair = None

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
            value = hashtable.get(key)

            # if rowpair.row_sum[6] == 143 and rowpair.row_sum[8] == 143 and rowpair.row_sum[0] == 143:
            #     print(rowpair.row_sum, hash(rowpair.row_sum.tobytes()))

            if value is None:
                hashtable[key] = set([rowpair])
                # _hashtable[key] = [rowpair]
            else:
                hashtable[key].add(rowpair)
                # _hashtable[key].append(rowpair)

cpdef find_differences_in_third_file(short [:, :] df, const short LAMBDA):
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