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
#FOUT = /scratch/yoder/toy_job.out
#FERR = /scratch/yoder/toy_job.err
#
#  File names for stdout and stderr.  If not set here, the defaults
# are <JOBNAME>.o<JOBNUM> and <JOBNAME>.e<JOBNUM>

#PBS -o /scratch/yoder/toy_job.out
#PBS -e /scratch/yoder/toy_job.err
#
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
# and execute this job:
#python $HOME/Codes/workspace/QBO_U_parser.py 7 --src_pathname=/scratch/yoder/U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc --dest_path=/scratch/yoder --batch_size=2500 --verbose=1 --n_cpu=1 --n_tries=10 --io_safer
