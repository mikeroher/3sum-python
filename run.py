import numpy as np
from mpi4py import MPI
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
if __name__ == '__main__':
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
    COMM = MPI.COMM_WORLD

    # When updating the dictionary, don't do it all at once.
    # Split it into chunks and insert each chunk.
    NUMBER_OF_C_CHUNKS = COMM.Get_size()

    ########################################## END OF CHANGE ME ############################################

    FILE_TEMPLATE = "{}/{}"

    # Must be short or at least match the DTPE constant in `threesum.pyx`
    DTYPE = np.short

    # A = iter_loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ")
    # B = iter_loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ")
    # C = iter_loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ")
    # MAX_C = np.amax(C, axis=0)

    #print(A)
    if COMM.rank == 0:
        LIST_OF_COLS = list(range(0,40))
        A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
        B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
        C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)

        hashtable = sum_each_of_first_two_files(A, B)
    else:
        hashtable = None

    hashtable = COMM.bcast(hashtable)

    COMM.Barrier()

    if COMM.rank == 0:
        third_file_chunked = chunk_dataframe(C, hashtable, NUMBER_OF_C_CHUNKS)
    else:
        third_file_chunked = None

    chunk = COMM.scatter(third_file_chunked)

    match = find_differences_in_third_file(chunk, LAMBDA)

    matches = COMM.gather(match)

    if COMM.rank == 0:
        file = open(OUTPUT_FILENAME, "w+")
        for match_tuple in matches:
            for match in match_tuple:
                print(match, file=file)
                print(match)
        file.close()
# https://groups.google.com/forum/#!msg/mpi4py/yT9N4M2kkck/LWEgolYXNdkJ