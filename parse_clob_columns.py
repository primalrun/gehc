import pandas as pd
import os

in_file = r'c:\temp\clob_columns.txt'
out_file = r'c:\temp\clob_columns_parse.xlsx'

df_in = pd.read_csv(filepath_or_buffer=in_file, header=0, sep='\t')
df_in['Columns'] = df_in['Columns'].str.split(', ')
df_in = df_in.explode('Columns')
df_in.to_excel(excel_writer=out_file, index=False)
os.startfile(out_file)
print('success')

