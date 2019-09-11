# QSUB script to run QBO extracter on Mazama
# - Note: the Nio (NCL/NetCDF classes) appear to be pretty twitchy about IO,
#  and the files are really big, so until we can sort out parallel IO, we need
#  to do this serially.
#!/bin/bash
#
#  Basics: Number of nodes, processors per node (ppn), and walltime (hhh:mm:ss)
###xxPBS -l nodes=5:ppn=8
###PxxBS -l walltime=2:00:00
#PBS -l mem=8gb
#PBS -l procs=1
#PBS -N yoder_QBO_test
#PBS -q default
#PBS -V
#PBS -m e
#
#
#rm /scratch/yoder/regi_05_08_job.out
#rm /scratch/yoder/regi_05_08_job.err
#
#  File names for stdout and stderr.  If not set here, the defaults
# are <JOBNAME>.o<JOBNUM> and <JOBNAME>.e<JOBNUM>
#PBS -o /scratch/yoder/job9-11.out
#PBS -e /scratch/yoder/job9-11.err
#
#if [ -f /scratch/yoder/job9-11.out ]; then
#    rm /scratch/yoder/job9-11.out
#    echo "removing old .out file"
#fi
#if [ -f /scratch/yoder/job9-11.err ]; then
#    rm /scratch/yoder/job9-11.err
#    echo "removing old .err file"
#fi
#
echo "PBS variables set."
#
# init/enable conda environments (and stuff):
. /usr/local/anaconda3/etc/profile.d/conda.sh

# activate the working environment
conda activate ncl_stable
#cd $HOME/Codes/workspace
#
echo "conda ncl_stable should be activated..."
#
# quick python test:
# (this is a diagnostic step. can we execute a python command? Can we import
#  the Nio library?
python -c "print('Test command line execution from Python...')"
python -c "import Nio"
#
# and execute this job:
python ~/Codes/workspace/QBO_U_parser.py 9 10 11 --src_pathname=/scratch/yoder/U_V_T_Z3_plWACCMSC_CTL_122.cam.h2.0001-0202.nc --dest_path=/scratch/yoder --batch_size=2500 --verbose=1 --n_cpu=1 --n_tries=10 --io_safer

#
echo "job complete."

