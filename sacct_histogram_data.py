#!/usr/bin/python3


# Functions to handle Input
#############################################################################################
def read_csv(file):
  # simple function to read data from a file
  data = pd.read_csv(file, sep=';')
  return data

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
  import datetime
  from dateutil import parser

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

  # reduce data point, 5min accuracy instead of seconds just fine
  df['Submit']=pd.to_datetime(df['Submit']).dt.round('5min')
  df['Start']=pd.to_datetime(df['Start']).dt.round('5min')
  df['End']=pd.to_datetime(df['End']).dt.round('5min')

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
  df.drop(['AveCPU','CPUTimeRAW','JobID','Elapsed','Account','ReqMem'], axis=1, inplace=True)

  # pick up only gpu and gpushort partitions and release memory and jobs > 5min
  df2=df[(df['Partition'].str.contains("gpu")) & (df['runtime'] > 300) ]
  del df
  df2.to_csv('df2.csv', sep=';', index=True)

  return df2

def times_df(df):
  start = df.Submit.min().floor('1H')
  end = df.End.max().floor('1H')+np.timedelta64(1,'h')
  steps = int((end-start).total_seconds()/3600)
  df2 = pd.DataFrame(index=pd.date_range(start,end,freq='1H'), columns=['Running'])
  df2.Running = 0

  for i in range(0,steps):
    s1=start+i*np.timedelta64(1,'h')
    e1=s1+np.timedelta64(1,'h')
    dfr=df[((df['Start'] >= s1) & (df['Start'] <= e1)) | ((df['End'] >= s1) & (df['End'] <= e1)) ]
    dfq=df[((df['Submit'] >= s1) & (df['Submit'] <= e1)) | ((df['Start'] >= s1) & (df['Start'] <= e1)) ]
    df2.loc[s1,'Running']=int(len(dfr))
    df2.loc[s1,'Queue']=int(len(dfq))

  return df2

# Functions to handle Output summary
#############################################################################################
def print_data(data):
  print('============================')
  print(data)

def writecsv(data):
  data.to_csv('result.csv', sep=';', index=True)

if __name__ == '__main__':
    from argparse import ArgumentParser
    import sys
    import pandas as pd
    import numpy as np
    import time

    parser = ArgumentParser(description='Create histogram stats from sacct data')
    parser.add_argument("file", help="input csv-file")
    args = parser.parse_args()

    times=[]
    comment=[]

    times.append([time.process_time(),'start'])
    data=read_csv(args.file)
    times.append([time.process_time(),'read csv'])
    data=parse_df(data)
    times.append([time.process_time(),'parse_df'])
    dfh=times_df(data)
    times.append([time.process_time(),'times_df'])


    #print_data(dfh)
    writecsv(dfh)

    prev=times[0][0]
    #print(times)
    for i in times:
        print("%s : %.2f seconds" % (i[1], i[0]-prev) )
        prev=i[0]

