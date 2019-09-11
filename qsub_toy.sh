# QSUB script to run QBO extracter on Mazama
# - Note: the Nio (NCL/NetCDF classes) appear to be pretty twitchy about IO,
#  and the files are really big, so until we can sort out parallel IO, we need
#  to do this serially.
#!/bin/bash
#
#  Basics: Number of nodes, processors per node (ppn), and walltime (hhh:mm:ss)
#PBS -l walltime=0:2:00
#PBS -l mem=8gb
#PBS -l procs=1
#PBS -N yoder_toy_test
#PBS -q default
#PBS -V
#PBS -m e
#
#  File names for stdout and stderr.  If not set here, the defaults
# are <JOBNAME>.o<JOBNUM> and <JOBNAME>.e<JOBNUM>
#PBS -o /scratch/yoder/toy_job.out
#PBS -e /scratch/yoder/toy_job.err
#
# NOTE: we don't seem to be able to assign values to variables inside the #PBS header block, so any PBS parameters
#  that we want to set dynamically should be passed as parameters (aka, qsub my_script.sh -o my_output.out ... )
#  it might also be interesting to see about piping scripts from a Python subprocess. The main bit for this is the
#  "ssh and execute" command, like:
# $ ssh cees-mazama 'qsub ~/Codes/workspace/qsub_toy.sh'
#  of course, without ssh certificate authentication, we can't fully automate this step, but...
#
export FOUT="/scratch/yoder/toy_job.out"
export FERR="/scratch/yoder/toy_job.err"
#
echo "Fout: " $FOUT
echo "FERR: " $FERR
echo "PBS_O_OUTPUT_PATH: " $PBS_O_OUTPUT_PATH
echo "PBS_OUTPUT_PATH: " $PBS_OUTPUT_PATH

#
echo "PBS variables set."
#rm /scratch/yoder/toy_job.out
#rm /scratch/yoder/toy_job.err
#
cd $HOME/Codes/workspace
pwd
#
# init/enable conda environments (and stuff):
. /usr/local/anaconda3/etc/profile.d/conda.sh
#
# activate the working environment
conda activate ncl_stable
#
echo "conda ncl_stable should be activated..."
#
# quick python test:
python -c "print('Test command line execution from Python...')"
echo "try to import Nio"
python -c "import Nio"
echo "Nio import worked?"
#
echo "toy script finished."
#
# and execute this job:
#python $HOME/Codes/workspace/QBO_U_parser.py 7 --src_pathname=/scratch/yoder/U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc --dest_path=/scratch/yoder --batch_size=2500 --verbose=1 --n_cpu=1 --n_tries=10 --io_safer
