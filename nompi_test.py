import numpy as np
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
    DATA_PATH = "/Users/mikeroher/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/Laurier (2017-2018)/Research/3sum.nosync/src/data"
    # DATA_PATH = "/Users/mikeroher/Desktop/3sum/mikeroher"
    FILE1_NAME = "A.txt"
    FILE2_NAME = "B.txt"
    FILE3_NAME = "C.txt"
    NUMBER_OF_COLUMNS = 40
    LAMBDA = 180
    FILE_TEMPLATE = "{}/{}"
    OUTPUT_FILENAME = f"{DATA_PATH}/3sum_output.txt"
    LIST_OF_COLS = list(range(0, NUMBER_OF_COLUMNS))
    DTYPE = np.short
    A = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE1_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE,
                   ndmin=2)
    B = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE2_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE,
                   ndmin=2)
    C = np.loadtxt(FILE_TEMPLATE.format(DATA_PATH, FILE3_NAME), delimiter=" ", usecols=LIST_OF_COLS, dtype=DTYPE,
               ndmin=2)

    differences = generate_differences_set(C, LAMBDA)
    matches = find_threeway_match(differences, A, B, False)
    for match in matches:
        print(np.asarray(match[0]), np.asarray(match[1]), match[2])