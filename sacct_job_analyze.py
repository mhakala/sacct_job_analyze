#!/usr/bin/python3


# Functions to handle Input
#############################################################################################
def read_csv():
  # simple function to read data from a file
  data = pd.read_csv('out.csv', sep=';')
  return data

def read_sacct():
  # function to read the data directly from sacct
  print('not implemented yet')


# Functions to handle string/value conversion
#############################################################################################
# function converts format (DD-)HH:MM:SS to seconds
def ave2sec(x):
  if ( '-' in x ):
    vals = x.split('-')
    times = vals[1].split(':')
    sec = 24*3600*int(vals[0])+3600*int(times[0])+60*int(times[1])+int(times[2])
  else:
    times = x.split(':')
    sec = 3600*int(times[0])+60*int(times[1])+int(times[2])
  return (sec)

def scalingref(x):
  # returns reference scaling factor for MPI jobs based on 1.5 factor:
  # doubling cores should make parformance x1.5 (or better)
  if int(x) == 1:
    ref = 1
  else:
    ref = np.power(1/1.5,np.log2(int(x)))

  return ref

def rss2g(x):
  return int(float(x[:-1]))/1024

def reqmem2g(x):
  return int(float(x[:-2]))/1024


# Functions to handle DataFrames
#############################################################################################
def parse_df(data):
  # convert multi-line DataFrame to more compact form for analysis
  from datetime import datetime

  data[['id','subid']] = data.JobID.str.split('_',1, expand=True)
  data.drop(['subid'],axis=1, inplace=True)

  df=pd.DataFrame()
  df=data[~data['JobID'].str.contains("\.")]
  df.rename(columns={'State': 'Parentstate'}, inplace=True)

  data2=data.shift(-1).dropna(subset=['JobID'])
  df2=data2[data2['JobID'].str.contains("\.batch")]

  data2=data.shift(-2).dropna(subset=['JobID'])
  df3=data2[data2['JobID'].str.contains("\.0")]

  df.update(df2.MaxRSS)
  df.update(df3.MaxRSS)
  df.update(df2.AveCPU)
  df.update(df3.AveCPU)
  df=df.join(df2[['State']])
  df.update(df3.State)

  # drop columns that all all nan
  df.dropna(axis=1, inplace=True, how='all')

  # drop rows where any element is nan (errors in the data)
  df.dropna(axis=0, inplace=True, how='any')
  df.reset_index(inplace=True)

  df.loc[:,'State']=df.State.apply(str)

  # add extra columns to df for analysis purposes
  df.insert(len(df.columns),'runtime',(pd.to_datetime(df['End'])-pd.to_datetime(df['Start']))/np.timedelta64(1,'s'))
  df.insert(len(df.columns),'waittime',(pd.to_datetime(df['Start'])-pd.to_datetime(df['Submit']))/np.timedelta64(1,'s'))

  # NOTE: this value ok only if State=Completed, for State=timeout values close to RawCPUTime
  df.insert(len(df.columns),'cpuavesec',df['AveCPU'].apply(ave2sec) )

  # scaling reference number for MPI jobs
  df.insert(len(df.columns),'cpufactor',df['cpuavesec']/df['runtime'] )

  df.loc[:,'MaxRSS']=df['MaxRSS'].apply(rss2g)
  df.insert(len(df.columns),'MemReqTotal', df['ReqMem'].apply(reqmem2g)*df['NCPUS'] )
  df.insert(len(df.columns),'MemRatio', df['MaxRSS']/df['MemReqTotal'] )
  df.insert(len(df.columns),'TimeRatio', df['runtime']/df['Timelimit'].apply(ave2sec)  )


  df.drop(['Elapsed','Start','End','Submit','AveCPU','CPUTimeRAW','JobID'], axis=1, inplace=True)

  return df

def analyze_df(df):
  # real work happens here. Based on proper conditions mark flags for jobs that need furher investigation
  # - cpu-load below treshold
  # - memory usage below treshold or very close to 100% (nearly killed due to memory exceeded)
  # - huge percentage of jobs killed with TIMEOUT
  # - runtimes very close to TIMEOUT value OR runtimes very short
  usrdf = pd.DataFrame(columns=['User','NJobs','UniqueJobs','%failed','%timeout','%poor_cpu_util','%low_mem_util_warn','%high_mem_util_warn','%close2timeout','%jobs_below_15min'])

  df.insert(len(df.columns),'warning', '0')
  for user in df.User.unique():
    df2 = df[ df['User'] == user]
    account = df2.Account.iloc[0]
    jobs = len(df2)
    unique_jobs = len(df2.id.unique())
    okjobs = len(df2 [ df2['State'] == 'COMPLETED'] )

    # Failed jobs
    failed = len (df2[ df2['State'] == 'FAILED'] )
    pfailed = 0
    if failed > 0:
       pfailed = failed/jobs

    # timeout jobs
    timeout = len(df2[ (df2['State'] == 'FAILED' ) & (df2['Parentstate'] == 'TIMEOUT') ])
    ptimeout = 0
    if timeout > 0:
        ptimeout = timeout/failed

    # runtime check, % of jobs where runtime is <15min (=900s)
    short_runtime = len(df2[ (df2['State'] == 'COMPLETED' ) & (df2['runtime'] < 900 ) ])
    pshort_runtime = 0
    if short_runtime > 0:
      pshort_runtime = short_runtime/okjobs

    # poor cpu utlization means that cpu-load is below 50% of the peak for whole run.
    # for multi-cpu, use scaling factor (assume 1.5 MPI scaling to be sufficient).
    poor_utilization = len(df2[ (df2['State'] == 'COMPLETED' ) & (df2['cpufactor'] < 0.5 ) & (~df2['Partition'].str.contains("gpu") ) ])
    putilization = 0
    if poor_utilization > 0:
      putilization = poor_utilization/okjobs

    # memory usage
    low_memory = len(df2[ (df2['State'] == 'COMPLETED' ) & (df2['MemRatio'] < 0.15 ) ])
    high_memory = len(df2[ (df2['State'] == 'COMPLETED' ) & (df2['MemRatio'] > 0.9 ) ])
    plowmem = 0
    phighmem = 0
    if low_memory > 0:
      plowmem = low_memory/okjobs
    if high_memory > 0:
      phighmem = high_memory/okjobs

    # runtime check
    rtime = len(df2[ (df2['State'] == 'COMPLETED' ) & (df2['TimeRatio'] > 0.9 ) ])
    prtime = 0
    if rtime > 0:
      prtime = rtime/okjobs

    user2=user+'('+account+')'
    usrdf.loc[len(usrdf)]=[user2,jobs,unique_jobs,pfailed*100,ptimeout*100,putilization*100,plowmem*100,phighmem*100,prtime*100,pshort_runtime]

  return usrdf


# Functions to handle Output summary
#############################################################################################
def print_data(data):
  print('============================')
  print(data)

def writecsv(data):
  data.to_csv('result.csv', sep=';', index=False)
  
def help():
  # print useage information
  msg = """
  Usage:
  -h           : this help message
  -csv <file>  : read the data from a file instead of calling sacct directly
  """
  print(msg)


if __name__ == '__main__':
    from argparse import ArgumentParser
    import sys
    import pandas as pd
    import numpy as np
    import time

    times=[]
    comment=[]

    times.append([time.process_time(),'start'])


    data=read_csv()
    times.append([time.process_time(),'read csv'])
    data=parse_df(data)
    times.append([time.process_time(),'parse_df'])
    usrdf=analyze_df(data)
    #times.append([time.process_time(),'analyze_df'])

    #print_data(usrdf)
    writecsv(usrdf)
    #times.append([time.process_time(),'print data'])

    prev=times[0][0]
    #print(times)
    #for i in times:
    #    print("%s : %.2f seconds" % (i[1], i[0]-prev) )
    #    prev=i[0]
