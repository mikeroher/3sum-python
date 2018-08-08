import numpy as np
from mpi4py import MPI
from os import environ
import _timing
import pickle
from time import time, strftime, localtime

# Depending on where the user imports the library from, it will have a different
# compiled path. To make it work across all platforms, we will try several different
# ways.
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
    # This line checks if we're on Sharcnet or our local machine. Sharcnet defines an environment
    # variable named cluster.
    IS_ON_SERVER = environ.get("CLUSTER") is not None
    # Variable referencing the MPI communicator world
    COMM = MPI.COMM_WORLD
    # This is a commonly used comparison that checks if we're the root process or a worker process
    # The root process has a rank of 0, all workers are > 0.
    IS_ROOT_PROC = COMM.rank == 0


    if IS_ON_SERVER:
        # Working directory - where the input files should be read from
        DATA_PATH = "/project/rohe8957"
        FILE1_NAME = "A1m.txt"
        FILE2_NAME = "B1m.txt"
        FILE3_NAME = "C1m.txt"
        # Number of columns in each data file. This prevents OOBOE where the delimeter is
        # at the end of the file. It will also throw an error if the # of cols doesn't match.
        NUMBER_OF_COLUMNS = 37
        # This is the target value we are searching for.
        LAMBDA = 169
    else: # We are on our local machine
        DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
        # DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"
        FILE1_NAME = "A.txt"
        FILE2_NAME = "B.txt"
        FILE3_NAME = "C.txt"
        NUMBER_OF_COLUMNS = 40
        LAMBDA = 180
    # This is the filepath to where the output (i.e. matches) should be stored.
    OUTPUT_FILENAME = f"{DATA_PATH}/3sum_output.txt"

    # MPI_Scatter/Gather requires that the array have N items where N is the number of processes.
    # This stores the number of processes.
    NUMBER_OF_CHUNKS = COMM.Get_size()

    ########################################## END OF CHANGE ME ############################################

    # This is the template string we use to merge the working directory with the filename.
    FILE_TEMPLATE = "{}/{}"

    # The DTYPE is the datatype to read in. I'd advise against changing this.
    # IMPORTANT: Must be short or if changed, match the DTPE constant in `threesum.pyx`
    DTYPE = np.short

    # The np.loadtxt file expects a list of column indices to use. We'll use this feature to verify the
    # number of columns if the number of columns > the variable, an error will not be thrown (intended).
    # If the variable > number of columns, an error will be thrown.
    LIST_OF_COLS = list(range(0, NUMBER_OF_COLUMNS))

    # The logging function used throughout the code. This timestamps each message and prints to stdout.
    def LOG(msg):
        # Only print the log message once if called by all processes
        if IS_ROOT_PROC:
            print(strftime("%Y-%m-%d %H:%M:%S", localtime()), "-", msg)

    if IS_ROOT_PROC:
        # Read in the third file and split it into several chunks
        C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)
        third_file_chunked = chunk_dataframe(C, NUMBER_OF_CHUNKS)
        LOG("Third file loaded and chunked")
    else:
        third_file_chunked = None

    # Note: This will be a common pattern used throughout the file (define a variable in the root process (if),
    #   initialize it equal to None in the workers (else), then call an MPI operation on the variable. This
    #   is how mpi4py scatters/gatters across all processes that use this file. For example, once we call scatter,
    #   the None variable will be replaced by its respective chunk.
    #   See here for more info: https://pythonprogramming.net/scatter-gather-mpi-mpi4py-tutorial/

    # Scatter the chunks to give each process its chunk
    C_chunk = COMM.scatter(third_file_chunked, root=0)

    LOG("Third file scattered.")
    LOG("Started generating differences for third file")

    # Create the Python set with the difference between the LAMBDA and each row of the C_chunk
    difference = generate_differences_set(C_chunk, LAMBDA)
    LOG("Finished generating differences for third file")

    # Gather the result from each process and send it back to the root process.
    # The result is a list of sets: [set(), set(), ...]
    differences_list = COMM.gather(difference, root=0)
    LOG("Merging results of generating differences")

    if IS_ROOT_PROC:
        # Now we need to merge the list of sets into one set.
        # Initialize a set with the contents of the first set.
        differences = set(differences_list[0])

        # For the remaining sets, iterate through them and add them to the one set.
        for diff in differences_list[1:]:
            differences.update(diff)

        # As a checkpoint/restore, we're going to save the set to a file that we can
        # restore to if the next half of the code fails. To do that, comment out all
        # code related to the third file and generating differences, and replace the
        # differences set with the one from the file:
        #          differences = pickle.load('xxxx_differences_set.p')
        LOG("Saving differences set to file")
        pickle.dump(differences, open("{}_differences_set.p".format(
            strftime("%Y-%m-%d %H:%M:%S", localtime())
        ), "wb"))
    else:
        differences = None

    LOG("Broadcasting difference chunk")

    # Share the differences set with each process.
    differences = COMM.bcast(differences, root=0)

    if IS_ROOT_PROC:
        # Read and chunk the first file
        A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE,
                       ndmin=2)
        first_file_chunked = chunk_dataframe(A, NUMBER_OF_CHUNKS)
    else:
        first_file_chunked = None

    LOG("Scattering chunks of first file")
    # Scatter the first file across all processes
    A_chunk = COMM.scatter(first_file_chunked)

    LOG("Loading second file")

    # Read the second file
    B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE, ndmin=2)

    LOG("Started looking for threeway match")
    # Loop through the chunked first file and the second file, search for where the sum of the
    # row from the chunk and the second file is equal to the value in the dictionary.
    match = find_threeway_match(differences, A_chunk, B)
    LOG("Finished looking for threeway match")

    # Gather the results into a nested array
    matches = COMM.gather(match)

    if IS_ROOT_PROC:
        LOG("Printing output")
        file = open(OUTPUT_FILENAME, "w+")
        for match_tuple in matches:
            for match in match_tuple:
                print(match, file=file)
                print(match)
        file.close()