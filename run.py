import numpy as np
import multiprocessing as mp
from os import environ
import itertools
import _timing
from mikeroher.mikeroher.threesum import sum_each_of_first_two_files, find_differences_in_third_file, chunk_dataframe
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

# Passed into the `pd.read_table` in order to ensure that there are 40 columns provided
LIST_OF_COLS = list(range(0, NUM_OF_COLS))

FILE_TEMPLATE = "{}/{}"

DTYPE = np.intc

A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)

manager = mp.Manager()

hashtable = manager.dict()

_hashtable = sum_each_of_first_two_files(A)
# Bulk insert the `_hashtable` into the `hashtable`.
hashtable.update(_hashtable)

pool = mp.Pool(processes=NUM_OF_PROCESSES)
# create our pool with `num_processes` processes
#pool = mp.Pool(processes=NUM_OF_PROCESSES)
third_file_chunked = chunk_dataframe(C, NUM_OF_PROCESSES)
# apply our function to each chunk in the list

matches = pool.starmap(find_differences_in_third_file, zip(third_file_chunked, itertools.repeat(hashtable)))
pool.close()

pool.close()
pool.join()
file = open(OUTPUT_FILENAME, "w+")
for match_tuple in matches:
    for match in match_tuple:
        print(match, file=file)
        print(match)
file.close()
