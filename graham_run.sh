#!/bin/bash
#SBATCH --account=def-ikotsire
#SBATCH -p mpi
#SBATCH --ntasks-per-node=64
#SBATCH --mem-per-cpu=2G       # memory; default unit is megabytes
#SBATCH --time=00-72:00           # time (DD-HH:MM)
#SBATCH --mail-user=rohe8957@mylaurier.ca
#SBATCH --mail-type=ALL
#SBATCH --output=%x-%j.out
mpiexec -np 64 python3 run.py