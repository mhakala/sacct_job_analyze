#!/bin/bash

# take location simply as param
cd $1

# get data from 7 days ago to today
start=`date --date='7 days ago' +%Y-%m-%d`
end=`date +%Y-%m-%d`

date
sacct --units=M -p --delimiter=";" -s cd,f --allusers --starttime=$start --endtime=$end --fields=JobID,AveCPU,CPUTimeRAW,NCPUS,MaxRSS,ReqMem,Elapsed,Account,User,Submit,Start,End,Timelimit,State,Partition > out.csv
date	

# parse the data
python3 sacct_job_analyze.py
