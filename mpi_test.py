from mpi4py.futures import *
if __name__ == '__main__':
    executor = MPIPoolExecutor(max_workers=2)
    for result in executor.map(pow, [2]*32, range(32)):
        print(result)