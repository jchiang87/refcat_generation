#!/bin/bash

#SBATCH --partition=roma,milano
#SBATCH --account=rubin:commissioning
#SBATCH --time=10:00:00

##SBATCH --nodes=1
##SBATCH --exclusive

#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16

source /sdf/data/rubin/user/jchiang/ops_rehearsal_3/setup.sh
cd /sdf/data/rubin/user/jchiang/ops_rehearsal_3/calibs

#echo "*** BIAS JOBS ***"
#python run_imsim_jobs.py BIAS
#
#echo "*** DARK JOBS ***"
#python run_imsim_jobs.py DARK
#
#echo "*** FLAT JOBS ***"
#python run_imsim_jobs.py FLAT
#
echo "*** PTC JOBS ***"
python run_imsim_jobs.py PTC
