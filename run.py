import numpy as np
import multiprocessing as mp
from os import environ
from itertools import repeat
import _timing

try:
    # On local machine, it compiles to the nested path
    #from mikeroher.mikeroher.threesum import sum_each_of_first_two_files, \
    from mikeroher.mikeroher.threesum import sum_each_of_first_two_files, \
        find_differences_in_third_file, chunk_dataframe, iter_loadtxt
except ImportError:
    # If executing directly in project folder, then this is the correct import.
    try:
        from mikeroher.threesum import sum_each_of_first_two_files, \
            find_differences_in_third_file, chunk_dataframe, iter_loadtxt
    except ImportError:
        # When executing on sharcnet, it compiles directly to this
        from threesum import sum_each_of_first_two_files, find_differences_in_third_file, chunk_dataframe, iter_loadtxt

########################################### CHANGE ME #################################################

# DATA_PATH = CURRENT WORKING DIRECTORY
# Determine if we're on sharcnet or local
if environ.get("CLUSTER") is not None:
    DATA_PATH = "/project/rohe8957"
else:
    # DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
    DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"

FILE1_NAME = "A.txt"

FILE2_NAME = "B.txt"

FILE3_NAME = "C.txt"

OUTPUT_FILENAME = f"{DATA_PATH}/3sum_output.txt"

LAMBDA = 180

# Number of processors to use for multiprocessing
NUM_OF_PROCESSES = mp.cpu_count()

# When updating the dictionary, don't do it all at once.
# Split it into chunks and insert each chunk.
NUMBER_OF_C_CHUNKS = NUM_OF_PROCESSES**2
########################################## END OF CHANGE ME ############################################

FILE_TEMPLATE = "{}/{}"

# Must be short or at least match the DTPE constant in `threesum.pyx`
DTYPE = np.short

# A = iter_loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ")
# B = iter_loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ")
# C = iter_loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ")
# MAX_C = np.amax(C, axis=0)

#print(A)
LIST_OF_COLS = list(range(0,40))
A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)

manager = mp.Manager()

# hashtable = manager.dict()

sum_each_of_first_two_files(A, B)
# for chunk in chunk_hashtable(_hashtable, HASHTABLE_CHUNK_SIZE):
#     hashtable.update(chunk)

pool = mp.Pool(processes=NUM_OF_PROCESSES)
# create our pool with `num_processes` processes
#pool = mp.Pool(processes=NUM_OF_PROCESSES)
third_file_chunked = chunk_dataframe(C, NUMBER_OF_C_CHUNKS)
# apply our function to each chunk in the list

#matches = pool.starmap(find_differences_in_third_file, zip(third_file_chunked, itertools.repeat(hashtable)))
matches = pool.starmap(find_differences_in_third_file, zip(third_file_chunked, repeat(LAMBDA)))

pool.close()
pool.join()

file = open(OUTPUT_FILENAME, "w+")
for match_tuple in matches:
    for match in match_tuple:
        print(match, file=file)
        print(match)
file.close()


# CONVERT TO MPI?
# Change to namedtple?
