import pandas as pd
import os
import redshift_connector as rc
import sys


conn = rc.connect(
    host=os.environ['redshift_host_nprod_finance']
    ,database='gehc_data'
    ,user=os.environ['redshift_user']
    ,password=os.environ['redshift_pass_nprod_finance']
)



out_file_dir = r'c:\temp\\'
