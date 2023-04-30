from django.shortcuts import render
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time


def index(request):
    if request.method=='POST':
        csvfile1=request.FILES['csvfile']
        df=pd.read_csv(csvfile1,encoding='latin1')
        table = pa.Table.from_pandas(df)
        file_name = f"file_{int(time.time())}.parquet"
        pq.write_table(table,file_name)
        
    return render(request,'index.html')

