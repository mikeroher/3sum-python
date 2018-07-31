#!/bin/bash
#SBATCH --account=def-ikotsire
#SBATCH --nodes=4                # number of MPI processes
#SBATCH --mem-per-cpu=12G       # memory; default unit is megabytes
#SBATCH --time=0-03:00           # time (DD-HH:MM)
#SBATCH --mail-user=rohe8957@mylaurier.ca
#SBATCH --mail-type=ALL
#SBATCH --output=%x-%j.out
mpiexec -n 4 python3 run.py