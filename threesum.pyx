#cython: language_level=3, boundscheck=False, wraparound=False, infer_types=False

#access to NumPy-Python functions,
import numpy as np
#access to Numpy C API
cimport numpy as np

from cpython cimport bool
# DTYPE for this, which is assigned to the usual NumPy runtime
# type info object.
# IMPORTANT - MAKE SURE ALL MATHEMATICAL OPERATIONS USE THE SAME DATATYPE. Otherwise, operations
# will introduce different integer types resulting in different hashing values.
cdef DTYPE = np.short


cpdef chunk_dataframe(const short[:,:] df, int n):
    """
    Chunk the dataframe into parts
    The result will be an array of memory views. 
    
    :param df: The 2D array to split into
    :param n: The number of chunks to split it into. If the number of chunks does not 
                evenly fit into the number of rows, then the length of the last chunk
                will be less than n. 
    :return: List of memory views.
    """
    assert n > 0, "# of chunks must be greater than zero"
    cdef list chunks = np.array_split(df, n, axis=0)
    return chunks

cpdef find_threeway_match(set differences, const short[:,:] dfA, const short[:,:] dfB,
                          bool exit_after_first_match):
    """
    Loop through the first and second files, search where the sum of the two rows exists
    in the `differences` set. 
    
    :param differences: Set containing byte strings of the elements in the array.
    :param dfA: 2D memoryview/numpy array to search
    :param dfB: 2D memoryview/numpy array to search
    :param exit_after_first_match: bool indicating if we should exit after the first match
            is found.
    :return: a list of matches, stored as tuples
    """
    cdef:
        size_t a, b
        const short[:] rowA
        # Once we find a match, we convert the match's byte string back to a numpy array for
        # printing purposes. This stores that variable
        np.ndarray rowC
        # Stores the sum calculated at each iteration
        bytes row_sum
        # The Cython code is most optimized (read: converts directly to C) when we use a native
        # for loop (i.e. not a `for...in`). Thus, we get the number of rows and store them in
        # variables to be used as the iterator length.
        unsigned long LEN_A = dfA.shape[0]
        unsigned long LEN_B = dfB.shape[0]
        # Stores the list of matches
        list matches = []
    a, b = 0, 0

    for a in range(LEN_A):
        # Since we're using a native for loop, we need to store the rowA row.
        rowA = dfA[a]
        for b in range(LEN_B):
            # This line of numpy takes the two rows, stacks them ontop of each other (i.e.
            # converts the two 1xn rowvectors to a 2xn matrix) then sums the columns. Once,
            # the sum is calculated, it stores it in the row_sum variable as bytes.
            #
            # IMPORTANT - We must specify the DTYPE for calculation, otherwise errors occur
            # where the summed result is a different data type than the input data type which
            # becomes critical later on.
            row_sum = np.sum(np.column_stack((rowA, dfB[b])), axis=1, dtype=DTYPE).tobytes()

            # If the `row_sum` is in the differences set, then we have found a match!!
            if row_sum in differences:
                # To get the last file's row, we have to convert it back from the byte representation.
                rowC = np.frombuffer(row_sum, dtype=DTYPE)
                # Append it to our list of matches as a tuple of numpy arrays
                matches.append((np.asarray(rowA), np.asarray(dfB[b]), rowC))
                # If the user wants to quit after the first match, let's exit and return the matches
                # array. Once we return, we'll handle the MPI interactions from the run.py side.
                if exit_after_first_match:
                    return matches
        # After every 1K rows, print a little checkpoint so we can see how far the code has
        # progressed.
        if a % 1000 == 0:
            print("\t- Checkpoint: Row {}".format(a))
    return matches

cpdef generate_differences_set(const short[:,:] df, const short LAMBDA):
    """
    Loop through each row and subtract it from the LAMBDA value
    
    :param df: 2D memoryview/numpy array
    :param LAMBDA: target value that will be searched for
    :return: a Set of byte strings of the difference between LAMBDA and each row
    """
    cdef:
        size_t c
        # The Cython code is most optimized (read: converts directly to C) when we use a native
        # for loop (i.e. not a `for...in`). Thus, we get the number of rows and store them in
        # variables to be used as the iterator length.
        unsigned long LEN_C = df.shape[0]
        # Stores the byte representation of the difference between LAMBDA and the row
        bytes diference
        # Set of differences.
        set differences = set()

    for c in range(LEN_C):
        # Subtracts each value in df[c] from LAMBDA, storing the result as bytes
        # IMPORTANT - We use bytes instead of storing the arrays themselves or the base 10 string
        # representation for mainly effeciency reasons but also because the set must be
        # Pickable (i.e. serializable) as it's serialized before being passed between processes.
        difference = np.subtract(LAMBDA, df[c], dtype=DTYPE).tobytes()
        # Add it to the array
        differences.add(difference)
    return differences