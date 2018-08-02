#cython: language_level=3, boundscheck=False, wraparound=False, infer_types=False

#access to NumPy-Python functions,
import numpy as np
#access to Numpy C API
cimport numpy as np
# DTYPE for this, which is assigned to the usual NumPy runtime
# type info object.
# IMPORTANT - MAKE SURE ALL MATHEMATICAL OPERATIONS USE THE SAME DATATYPE. Otherwise, operations
# will introduce different integer types resulting in different hashing values.
cdef DTYPE = np.short

cpdef chunk_dataframe(np.ndarray df, int n):
    assert n > 0, "# of chunks must be greater than zero"
    cdef list chunks = np.array_split(df, n, axis=0)
    return chunks

cpdef find_threeway_match(set differences, const short[:,:] dfA, const short[:,:] dfB):
    cdef:

        size_t a, b = 0
        const short[:] rowA
        # This must be an int64 as the hashed key is too large for a short
        bytes row_sum
        unsigned long LEN_A = dfA.shape[0]
        unsigned long LEN_B = dfB.shape[0]

        list matches = []
        tuple match = None

    a, b = 0, 0

    for a in range(LEN_A):
        rowA = dfA[a]
        for b in range(LEN_B):

            row_sum = np.sum(np.column_stack((rowA, dfB[b])), axis=1, dtype=DTYPE).tobytes()
            if row_sum in differences:
                rowC = np.frombuffer(row_sum, dtype=DTYPE)
                matches.append((np.asarray(rowA), np.asarray(dfB[b]), rowC))
    return matches

cpdef generate_differences_set(const short[:,:] df, const short LAMBDA):
    cdef:
        np.int64_t key
        size_t c
        unsigned long LEN_C = df.shape[0]
        bytes diference
        set differences

    differences = set()

    for c in range(LEN_C):
        difference = np.subtract(LAMBDA, df[c], dtype=DTYPE).tobytes()
        differences.add(difference)

    return differences