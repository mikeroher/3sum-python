import numpy as np
from mpi4py import MPI
from os import environ
import _timing

try:
    # On local machine, it compiles to the nested path
    #from mikeroher.mikeroher.threesum import generate_differences_set, \
    from mikeroher.mikeroher.threesum import generate_differences_set, \
        find_threeway_match, chunk_dataframe
except ImportError:
    # If executing directly in project folder, then this is the correct import.
    try:
        from mikeroher.threesum import generate_differences_set, \
            find_threeway_match, chunk_dataframe
    except ImportError:
        # When executing on sharcnet, it compiles directly to this
        from threesum import generate_differences_set, find_threeway_match, chunk_dataframe

########################################### CHANGE ME #################################################
if __name__ == '__main__':

    IS_ON_SERVER = environ.get("CLUSTER") is not None


    # DATA_PATH = CURRENT WORKING DIRECTORY
    # Determine if we're on sharcnet or local
    if IS_ON_SERVER:
        DATA_PATH = "/project/rohe8957"
        FILE1_NAME = "A1m.txt"
        FILE2_NAME = "B1m.txt"
        FILE3_NAME = "C1m.txt"
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

    if COMM.rank == 0:
        C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
        third_file_chunked = chunk_dataframe(C, NUMBER_OF_CHUNKS)
    else:
        third_file_chunked = None

    C_chunk = COMM.scatter(third_file_chunked)

    difference = generate_differences_set(C_chunk, LAMBDA)

    differences_list = COMM.gather(difference)

    if COMM.rank == 0:
        differences = set(differences_list[0])

        for diff in differences_list[1:]:
            differences.update(diff)
    else:
        differences = None

    differences = COMM.bcast(differences, root=0)

    if COMM.rank == 0:
        A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE,
                       ndmin=2)
        first_file_chunked = chunk_dataframe(A, NUMBER_OF_CHUNKS)
    else:
        first_file_chunked = None

    A_chunk = COMM.scatter(first_file_chunked)

    B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)

    match = find_threeway_match(differences, A_chunk, B)

    matches = COMM.gather(match)

    if COMM.rank == 0:

        file = open(OUTPUT_FILENAME, "w+")
        for match_tuple in matches:
            for match in match_tuple:
                print(match, file=file)
                print(match)
        file.close()
# https://groups.google.com/forum/#!msg/mpi4py/yT9N4M2kkck/LWEgolYXNdkJ