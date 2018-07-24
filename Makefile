PYTHON?=python
MPI_NUM_PROCESSES=2

all:    local

local:
	${PYTHON} setup.py build_ext --inplace

run:
	${PYTHON} run.py

annotate:
	cythonize -a threesum.pyx
	open threesum.html

mpi_run:
	mpiexec -n ${MPI_NUM_PROCESSES} python -m mpi4py.futures run.py

clean:
	@echo Cleaning Prevoius Builds
	@rm -rf ./mikeroher/
	@rm -rf ./build/