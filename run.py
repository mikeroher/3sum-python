import numpy as np
from mpi4py import MPI
from os import environ
import pickle
from datetime import datetime
import _timing

try:
    # On local machine, it compiles to the nested path
    #from mikeroher.mikeroher.threesum import sum_each_of_first_two_files, \
    from mikeroher.mikeroher.threesum import sum_each_of_first_two_files, \
        find_differences_in_third_file, chunk_dataframe
except ImportError:
    # If executing directly in project folder, then this is the correct import.
    try:
        from mikeroher.threesum import sum_each_of_first_two_files, \
            find_differences_in_third_file, chunk_dataframe
    except ImportError:
        # When executing on sharcnet, it compiles directly to this
        from threesum import sum_each_of_first_two_files, find_differences_in_third_file, chunk_dataframe

########################################### CHANGE ME #################################################
if __name__ == '__main__':

    IS_ON_SERVER = environ.get("CLUSTER") is not None


    # DATA_PATH = CURRENT WORKING DIRECTORY
    # Determine if we're on sharcnet or local
    if IS_ON_SERVER:
        DATA_PATH = "/project/rohe8957"
        FILE1_NAME = "A_cut.txt"
        FILE2_NAME = "B_cut.txt"
        FILE3_NAME = "C_cut.txt"
        NUMBER_OF_COLUMNS = 37
        LAMBDA = 169
    else:
        # DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
        DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"
        FILE1_NAME = "A.txt"
        FILE2_NAME = "B.txt"
        FILE3_NAME = "C.txt"
        NUMBER_OF_COLUMNS = 40
        LAMBDA = 180

    OUTPUT_FILENAME = f"{DATA_PATH}/3sum_output.txt"

    COMM = MPI.COMM_WORLD

    # When updating the dictionary, don't do it all at once.
    # Split it into chunks and insert each chunk.
    NUMBER_OF_C_CHUNKS = COMM.Get_size()

    ########################################## END OF CHANGE ME ############################################

    FILE_TEMPLATE = "{}/{}"

    # Must be short or at least match the DTPE constant in `threesum.pyx`
    DTYPE = np.short

    if COMM.rank == 0:
        LIST_OF_COLS = list(range(0, NUMBER_OF_COLUMNS))
        A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
        B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
        C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)


        # Loop through each of the rows in the first two files, summing the columns
        # to create a dictionary where the key is the row of summed columns and the
        # value is a list of `Rowpair` objects which is a wrapper around the two rows
        # that were summed.
        hashtable = sum_each_of_first_two_files(A, B)
        # hashtable = pickle.load('2018-07-03 18:39:17_hashtable.pickle')

        # Write the hashtable to a file. If the execution crashes after this runs,
        # you can comment out the `sum_each_of_first_two_files` call and replace
        # it with the `pickle.load`. This will allow us to skip the memory/time
        # consuming process of summing each of the first two files and proceed
        # directly to the finding differences step.

        # Get the datetime as a string for unique filenames
        time_as_str = datetime.now().isoformat(' ', 'seconds')
        hashtable_filename = f"{time_as_str}_hashtable.pickle"
        # Write the hashtable to a file
        with open(hashtable_filename, 'wb') as handle:
            pickle.dump(hashtable, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        hashtable = None

    # Share the hashtable across all processes. This assumes the root process
    # is the first process.
    hashtable = COMM.bcast(hashtable)



    if COMM.rank == 0:
        third_file_chunked = chunk_dataframe(C, NUMBER_OF_C_CHUNKS)
    else:
        third_file_chunked = None

    chunk = COMM.scatter(third_file_chunked)

    match = find_differences_in_third_file(chunk, hashtable, LAMBDA)

    matches = COMM.gather(match)

    if COMM.rank == 0:
        file = open(OUTPUT_FILENAME, "w+")
        for match_tuple in matches:
            for match in match_tuple:
                print(match, file=file)
                print(match)
        file.close()
# https://groups.google.com/forum/#!msg/mpi4py/yT9N4M2kkck/LWEgolYXNdkJ