import pandas as pd
import numpy as np

# Set width for output
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)


#DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"

FILE_NAME = f"{DATA_PATH}/3sum_output.txt"
LAMBDA = 180
NUM_OF_COLS = 40
# Passed into the `pd.read_table` in order to ensure that there are 40 columns provided
LIST_OF_COLS = list(range(0, NUM_OF_COLS))

A = pd.read_table(f"{DATA_PATH}/A.txt", sep=" ", header=None, usecols=LIST_OF_COLS)
B = pd.read_table(f"{DATA_PATH}/B.txt", sep=" ", header=None, usecols=LIST_OF_COLS)
C = pd.read_table(f"{DATA_PATH}/C.txt", sep=" ", header=None, usecols=LIST_OF_COLS)

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

hashtable = {}

rowpair = None
for idxA, rowA in A.iterrows():
    for idxB, rowB in B.iterrows():
        rowpair = RowPair(rowA, rowB)
        # Can't hash an intarray so we have to take the data as bytes (i.e. a string)
        key = hash(rowpair.row_sum.data.tobytes())
        value = hashtable.get(key)
        if (value is None):
            hashtable[key] = [rowpair]
        else:
            hashtable[key].append(rowpair)

print(hashtable)

# Uncomment this for printing the hashtable
# for key, value in hashtable.items():
#     rowsum = value[0].row_sum
#     print("{0}\t{1: >5}".format(rowsum, len(value)))

file = open(FILE_NAME, "w+")

for idxC, rowC in C.iterrows():
    # The values call is necessary because np.subtract here returns the original
    # data type which is a Pandas series. This way we get an nparray instead where
    # we can call the data.
    difference = np.subtract(LAMBDA, rowC).values
    key = hash(difference.data.tobytes())
    value = hashtable.get(key)
    if (value is not None):
        for v in value:
            output = f"{tuple(v.rowA)} {tuple(v.rowB)} {tuple(rowC)}"
            print(output)
            print(output, file=file)

file.close()

#https://stackoverflow.com/questions/40357434/pandas-df-iterrow-parallelization
#https://stackoverflow.com/questions/38393269/fill-up-a-dictionary-in-parallel-with-multiprocessing