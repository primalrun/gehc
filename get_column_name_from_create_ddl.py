import os
import pandas as pd

in_file = r'c:\temp\t1.txt'
column_row_identifier_prefix = '\t'
out_file = r'c:\temp\column_attribute.xlsx'

with open(file=in_file, mode='r') as fr:
    data = fr.readlines()
    column_name_rows = [r for r in data if r.startswith(column_row_identifier_prefix)]
    column_name = [s.split(sep=' ')[0].replace(',', '').strip() for s in column_name_rows]
    column_attribute = [s[s.index(' ') + 1:] for s in column_name_rows]
    data_type = [s.upper().split('ENCODE')[0].strip() for s in column_attribute]
    encode_type = ['ENCODE ' + s.upper().split('ENCODE')[1].strip() for s in column_attribute]

data = list(zip(column_name, data_type, encode_type))
column = ['column_name', 'data_type', 'encode_type']
df = pd.DataFrame(data=data, columns=column)
df.to_excel(excel_writer=out_file, index=False)

os.startfile(out_file)

print('success')
