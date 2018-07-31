import numpy as np
from mpi4py import MPI
from os import environ
import pickle
from collections import defaultdict
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
    NUMBER_OF_CHUNKS = COMM.Get_size()

    ########################################## END OF CHANGE ME ############################################

    FILE_TEMPLATE = "{}/{}"

    # Must be short or at least match the DTPE constant in `threesum.pyx`
    DTYPE = np.short

    LIST_OF_COLS = list(range(0, NUMBER_OF_COLUMNS))

    # This is used by all processes so we need to define it for all.
    B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE,
                   ndmin=2)

    if COMM.rank == 0:
        # These are only used by the root process, so we can define it only for the root process
        A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
        C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)

        first_file_chunked = chunk_dataframe(A, NUMBER_OF_CHUNKS)
    else:
        first_file_chunked = None

    A_chunk = COMM.scatter(first_file_chunked)


    # Loop through each of the rows in the first two files, summing the columns
    # to create a dictionary where the key is the row of summed columns and the
    # value is a list of `Rowpair` objects which is a wrapper around the two rows
    # that were summed.
    hashtable_chunk = sum_each_of_first_two_files(A_chunk, B)

    hashtables = COMM.gather(hashtable_chunk)

    if COMM.rank == 0:
        # MERGE ALL THE HASHTABLES CREATED FROM EACH PROCESS INTO ONE MASTER HASHTABLE
        # ---
        # Use a default dict for the merge. The difference is that a default dict doesn't throw
        # KeyErrror's if the key is not found. So, we can call extend directly on it. Initialize
        # the dict with the contents of the first hashtable (to skip one iteration) then add each
        # of the following hashtables in, checking each time if the key already exists. If it does,
        # append the values to the existing key.

        # Initialize with contents of first hashtable and set the fallback to an empty list.
        hashtable = defaultdict(list, hashtables[0])
        # Loop through remaining hashtables...
        for ht in hashtables[1:]:
            for k, v in ht.items():
                # Remember: defaultdict automatically creates the key if it doesn't exist so we can assume
                # it does. If it doesn't, a list is automatically created and the `v` list is extended to it.
                hashtable[k].extend(v)
        # Since we're done editing it, change it back to a dict as Cython doesn't handle defaultdict's
        # particularly well
        hashtable = dict(hashtable)


        # MAKE A BACKUP OF THE HASHTABLE SO WE CAN RESTORE FROM HERE IF NEEDED
        # ---
        # Write the hashtable to a file. If the execution crashes after this runs,
        # you can comment out the `sum_each_of_first_two_files` call and replace
        # it with the `pickle.load`. This will allow us to skip the memory/time
        # consuming process of summing each of the first two files and proceed
        # directly to the finding differences step.
        # hashtable = pickle.load('2018-07-03 18:39:17_hashtable.pickle')

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

    # Release the memory associated with file A and B as they are not needed anymore
    A = None
    B = None

    if COMM.rank == 0:
        third_file_chunked = chunk_dataframe(C, NUMBER_OF_CHUNKS)
    else:
        third_file_chunked = None

    C_chunk = COMM.scatter(third_file_chunked)

    match = find_differences_in_third_file(C_chunk, hashtable, LAMBDA)

    matches = COMM.gather(match)

    if COMM.rank == 0:
        file = open(OUTPUT_FILENAME, "w+")
        for match_tuple in matches:
            for match in match_tuple:
                print(match, file=file)
                print(match)
        file.close()
# https://groups.google.com/forum/#!msg/mpi4py/yT9N4M2kkck/LWEgolYXNdkJ