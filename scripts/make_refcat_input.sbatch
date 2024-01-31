#!/bin/bash

#SBATCH --partition=roma,milano
#SBATCH --account=rubin:commissioning
#SBATCH --time=10:00:00

##SBATCH --nodes=1
##SBATCH --exclusive

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32

source /sdf/data/rubin/user/jchiang/ops_rehearsal_3/setup.sh
cd /sdf/data/rubin/user/jchiang/ops_rehearsal_3/refcats

python make_refcat_input.py
