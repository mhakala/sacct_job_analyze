# sacct_job_analyze
Slurm batch scheduler accounting data analysis tools using python3 pandas framework.

For understanding how users use the resources we can analyze the job statistics generated 
by Slurm to accounting DB. In some cases the resources asked by users is far from optimal. 
Here the best approach we have found is to identify these users and provide them detailed 
instructions how to enhance their job submission process.

Common issues include
..* Asking too much memory for the job
..* Asking (almost) too little time for the job leading to issues with Timeout 
..* Many jobs very short (<15min, resources wasted due to sheduling overhead)
..* CPU utilization is poor. Usually there is some application that has done the task and has been left in the background idling and waiting for the timeout

### Basic usage

First generate statistics with wanted interval
```
sacct --units=M -p --delimiter=";" -s cd,f --allusers --starttime=$start --endtime=$end --fields=JobID,AveCPU,CPUTimeRAW,NCPUS,MaxRSS,ReqMem,Elapsed,Account,User,Submit,Start,End,Timelimit,State,Partition > out.csv
```
Second run the analysis tool
```
python3 sacct_job_analyze.py
```
and visualization is possible with included html-file.
