#!/bin/bash
#SBATCH --account=def-ikotsire
#SBATCH --ntasks-per-node=24
#SBATCH --mem-per-cpu=8G       # memory; default unit is megabytes
#SBATCH --time=00-72:00           # time (DD-HH:MM)
#SBATCH --mail-user=rohe8957@mylaurier.ca
#SBATCH --mail-type=ALL
#SBATCH --output=%x-%j.out
srun python3 run.py