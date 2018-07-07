import pandas as pd
import numpy as np
import multiprocessing as mp
from collections import ChainMap
import itertools

########################################### CHANGE ME #################################################
# Set width for output
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)

# DATA_PATH = CURRENT WORKING DIRECTORY
# DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"

FILE_NAME = f"{DATA_PATH}/3sum_output.txt"
LAMBDA = 180
NUM_OF_COLS = 40

# Number of processors to use for multiprocessing
NUM_OF_PROCESSES = mp.cpu_count()
########################################## END OF CHANGE ME ############################################

# Passed into the `pd.read_table` in order to ensure that there are 40 columns provided
LIST_OF_COLS = list(range(0, NUM_OF_COLS))

A = pd.read_table(f"{DATA_PATH}/A 2.txt", sep=" ", header=None, usecols=LIST_OF_COLS)
B = pd.read_table(f"{DATA_PATH}/B 2.txt", sep=" ", header=None, usecols=LIST_OF_COLS)
C = pd.read_table(f"{DATA_PATH}/C 2.txt", sep=" ", header=None, usecols=LIST_OF_COLS)

class RowPair(object):
    """
    Wrapper for two row vectors and calculates the column wise sum

    Keyword arguments:
        rowA, '1xm'-dimensional numpy array
        rowB, '1xm'-dimensional numpy array

    Returns the RowPair wrapper
    """
    def __init__(self, rowA, rowB):
        self.rowA = rowA
        self.rowB = rowB
        self.row_sum = np.sum(np.column_stack((rowA, rowB)), axis=1)

def _chunk_dataframe(df:pd.DataFrame, n=NUM_OF_PROCESSES) -> [pd.DataFrame]:

    chunk_size = int(df.shape[0] / n)
    # will work even if the length of the dataframe is not evenly divisible by num_processes
    chunks = [df.ix[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
    return chunks

def sum_each_of_first_two_files(dfA:pd.DataFrame, hashtable):
    rowpair = None
    for idxA, rowA in dfA.iterrows():
        for idxB, rowB in B.iterrows():
            rowpair = RowPair(rowA, rowB)

            # TODO: Insert effeciency checks here (sum < MAX C)

            # Can't hash an intarray so we have to take the data as bytes (i.e. a string)
            key = hash(rowpair.row_sum.data.tobytes())
            value = hashtable.get(key)
            if (value is None):
                hashtable[key] = [rowpair]
            else:
                hashtable[key].append(rowpair)
    return

def find_differences_in_third_file(df:pd.DataFrame):
    matches = []
    append = matches.append
    for idxC, rowC in df.iterrows():
        # The values call is necessary because np.subtract here returns the original
        # data type which is a Pandas series. This way we get an nparray instead where
        # we can call the data.
        difference = np.subtract(LAMBDA, rowC).values
        key = hash(difference.data.tobytes())
        value = hashtable.get(key)
        if (value is not None):
            for v in value:
                # output = f"{tuple(v.rowA)} {tuple(v.rowB)} {tuple(rowC)}"
                # print(output)
                # print(output, file=file)
                append((tuple(v.rowA), tuple(v.rowB), tuple(rowC)))
    return matches

# file.close()

if __name__ == "__main__":
    manager = mp.Manager()

    hashtable = manager.dict()

    pool = mp.Pool(processes=NUM_OF_PROCESSES)
    first_file_chunked = _chunk_dataframe(A)

    # The itertools.repeat isn't actually repeating. It's unpacked by the starmap.
    # The result is stored in the hashtable dict so we don't actually need the result
    # of this call.
    _ = pool.starmap(sum_each_of_first_two_files, zip(first_file_chunked, itertools.repeat(hashtable)))
    pool.close()


    # create our pool with `num_processes` processes
    pool = mp.Pool(processes=NUM_OF_PROCESSES)
    third_file_chunked = _chunk_dataframe(C)
    # apply our function to each chunk in the list
    matches = pool.map(find_differences_in_third_file, third_file_chunked)

    file = open(FILE_NAME, "w+")
    for match_tuple in matches:
        for match in match_tuple:
            print(match, file=file)
            print(match)
    file.close()

#https://stackoverflow.com/questions/40357434/pandas-df-iterrow-parallelization
#https://stackoverflow.com/questions/38393269/fill-up-a-dictionary-in-parallel-with-multiprocessing