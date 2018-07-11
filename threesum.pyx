#cython: language_level=3, boundscheck=False, wraparound=False, infer_types=True

#access to NumPy-Python functions,
import numpy as np
#access to Numpy C API
cimport numpy as np
import multiprocessing as mp
from itertools import islice
from os import environ

########################################### CHANGE ME #################################################
# DATA_PATH = CURRENT WORKING DIRECTORY
# Determine if we're on sharcnet or local
cdef DATA_PATH
if environ.get("CLUSTER") is not None:
    DATA_PATH = "/project/rohe8957"
else:
    DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
    # DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"

cdef FILE1_NAME = "A.txt"

cdef FILE2_NAME = "B.txt"

cdef FILE3_NAME = "C.txt"

cdef int LAMBDA = 180

cdef int NUM_OF_COLS = 40
########################################## END OF CHANGE ME ############################################

# Passed into the `pd.read_table` in order to ensure that there are 40 columns provided
cdef list LIST_OF_COLS = list(range(0, NUM_OF_COLS))

# DTYPE for this, which is assigned to the usual NumPy runtime
# type info object.
# IMPORTANT - MAKE SURE ALL MATHEMATICAL OPERATIONS USE THE SAME DATATYPE. Otherwise, operations
# will introduce different integer types resulting in different hashing values.
cdef DTYPE = np.intc

cdef FILE_TEMPLATE = "{}/{}"
# http://gouthamanbalaraman.com/blog/numpy-vs-pandas-comparison.html
cdef np.ndarray A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
cdef np.ndarray B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
cdef np.ndarray C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
# Used for some checks to skip rows
#C_MAX = np.max(C.max(axis=1, numeric_only=True))

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

def chunk_dataframe(np.ndarray df, int n):
    assert n > 0, "# of chunks must be greater than zero"
    #chunk_size = int(df.shape[0] / n)
    # will work even if the length of the dataframe is not evenly divisible by num_processes
    #chunks = [df.ix[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
    cdef list chunks = np.array_split(df, n, axis=0)
    return chunks

def chunk_hashtable(dict ht, int N):
    cdef int i
    cdef np.int64_t key
    cdef _iter = iter(ht)
    cdef int dict_len = len(ht)
    for i in range(0, dict_len, N):
        yield {key: ht[key] for key in islice(_iter, N)}


def sum_each_of_first_two_files(np.ndarray dfA):
    cdef RowPair rowpair = None
    # Since insert operations are very slow, we're going to do all the
    # insertions at once. This is significantly faster.
    cdef dict _hashtable = dict()

    # It is very important to type ALL your variables. You do not get any
    # warnings if not, only much slower code (they are implicitly typed as
    # Python objects).
    cdef np.ndarray rowA, rowB
    cdef np.int64_t key
    cdef set value

    # Name = None => Iterates with normal tuples instead of named tuples.
    # This is important as pickle fails when the tuple is named so that
    # parameter is required.
    for rowA in dfA:
        for rowB in B:
            rowpair = RowPair(rowA, rowB)
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

def find_differences_in_third_file(int [:, :] df, hashtable):
    cdef list matches = []
    cdef tuple match = None
    cdef int[:] rowC
    cdef RowPair rowpair = None
    cdef np.int64_t key
    cdef set value
    cdef np.ndarray difference
    for rowC in df:
        # The values call is necessary because np.subtract here returns the original
        # data type which is a Pandas series. This way we get an nparray instead where
        # we can call the data.
        difference = np.subtract(LAMBDA, rowC, dtype=DTYPE)
        # if difference[6] == 143 and difference[8] == 143 and difference[0] == 143:
        #     print(difference, hash(difference.tobytes()))
        key = hash(difference.tobytes())
        value = hashtable.get(key)

        if value is not None:
            for rowpair in value:
                match = (tuple(rowpair.rowA), tuple(rowpair.rowB), tuple(rowC))
                matches.append(match)
    return matches

#https://stackoverflow.com/questions/40357434/pandas-df-iterrow-parallelization
#https://stackoverflow.com/questions/38393269/fill-up-a-dictionary-in-parallel-with-multiprocessing