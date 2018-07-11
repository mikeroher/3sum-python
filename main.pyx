import pandas as pd
import numpy as np
import multiprocessing as mp
import _timing
from os import environ

########################################### CHANGE ME #################################################
# Set width for output
OUTPUT_WIDTH = 320

# DATA_PATH = CURRENT WORKING DIRECTORY
# Determine if we're on sharcnet or local
if environ.get("CLUSTER") is not None:
    DATA_PATH = "/project/rohe8957"
else:
    DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
    # DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"

FILE1_NAME = "A.txt"

FILE2_NAME = "B.txt"

FILE3_NAME = "C.txt"

OUTPUT_FILENAME = f"{DATA_PATH}/3sum_output.txt"

LAMBDA = 180

NUM_OF_COLS = 40

# Number of processors to use for multiprocessing
NUM_OF_PROCESSES = mp.cpu_count()
########################################## END OF CHANGE ME ############################################

pd.set_option('display.width', OUTPUT_WIDTH)
np.set_printoptions(linewidth=OUTPUT_WIDTH)

# Passed into the `pd.read_table` in order to ensure that there are 40 columns provided
LIST_OF_COLS = list(range(0, NUM_OF_COLS))

FILE_TEMPLATE = "{}/{}"

A = pd.read_table(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), sep=" ", header=None, usecols=LIST_OF_COLS)
B = pd.read_table(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), sep=" ", header=None, usecols=LIST_OF_COLS)
C = pd.read_table(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), sep=" ", header=None, usecols=LIST_OF_COLS)
# Used for some checks to skip rows
C_MAX = np.max(C.max(axis=1, numeric_only=True))

class RowPair:
    """
    Wrapper for two row vectors and calculates the column wise sum

    Keyword arguments:
        rowA, '1xm'-dimensional numpy array
        rowB, '1xm'-dimensional numpy array

    Returns the RowPair wrapper
    """
    __slots__ = ['rowA', 'rowB', 'row_sum']
    def __init__(self, rowA:np.ndarray, rowB:np.ndarray):
        self.rowA = rowA
        self.rowB = rowB
        self.row_sum = np.sum(np.column_stack((rowA, rowB)), axis=1)

def _chunk_dataframe(df:pd.DataFrame, n=NUM_OF_PROCESSES) -> [pd.DataFrame]:

    chunk_size = int(df.shape[0] / n)
    # will work even if the length of the dataframe is not evenly divisible by num_processes
    chunks = [df.ix[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
    return chunks

def sum_each_of_first_two_files(dfA:pd.DataFrame, hashtable) -> None:
    rowpair = None
    # Since insert operations are very slow, we're going to do all the
    # insertions at once. This is significantly faster.
    _hashtable = dict()

    # Instead of looping twice, we vectorize the cross-join then loop once
    # through the new table
    dfA['KEY'] = 0
    B['KEY'] = 0
    merged = pd.merge(dfA, B, on='KEY')
    merged.index = pd.MultiIndex.from_product((dfA.index, B.index))
    dfA.drop('KEY', axis=1, inplace=True)
    B.drop('KEY', axis=1, inplace=True)

    for idx, row in enumerate(merged.itertuples(index=False)):
        rowA = merged.iloc[idx:idx, 0:NUM_OF_COLS]
        rowB = merged.iloc[idx:idx, NUM_OF_COLS:(NUM_OF_COLS*2)]
        rowpair = RowPair(rowA, rowB)
        # Can't hash an intarray so we have to take the data as bytes (i.e. a string)
        key = hash(rowpair.row_sum.data.tobytes())
        value = _hashtable.get(key)
        if value is None:
            _hashtable[key] = [rowpair]
        else:
            _hashtable[key].append(rowpair)
    # # Bulk insert the `_hashtable` into the `hashtable`.
    hashtable.update(_hashtable)

    return

def find_differences_in_third_file(df:pd.DataFrame):
    matches = []
    match = None
    for rowC in df.itertuples(index=False):
        # The values call is necessary because np.subtract here returns the original
        # data type which is a Pandas series. This way we get an nparray instead where
        # we can call the data.
        difference = np.subtract(LAMBDA, rowC)
        key = hash(difference.data.tobytes())
        value = hashtable.get(key)

        if value is not None:
            for v in value:
                match = (tuple(v.rowA), tuple(v.rowB), tuple(rowC))
                matches.append(match)
    return matches

if __name__ == "__main__":

    manager = mp.Manager()

    hashtable = manager.dict()

    sum_each_of_first_two_files(A, hashtable)

    pool = mp.Pool(processes=NUM_OF_PROCESSES)
    # create our pool with `num_processes` processes
    #pool = mp.Pool(processes=NUM_OF_PROCESSES)
    third_file_chunked = _chunk_dataframe(C)
    # apply our function to each chunk in the list
    matches = pool.map(find_differences_in_third_file, third_file_chunked)

    pool.close()
    pool.join()
    file = open(OUTPUT_FILENAME, "w+")
    for match_tuple in matches:
        for match in match_tuple:
            print(match, file=file)
            print(match)
    file.close()

#https://stackoverflow.com/questions/40357434/pandas-df-iterrow-parallelization
#https://stackoverflow.com/questions/38393269/fill-up-a-dictionary-in-parallel-with-multiprocessing
# https://stackoverflow.com/questions/24870953/does-iterrows-have-performance-issues