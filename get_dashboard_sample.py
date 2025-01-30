import os
import pandas as pd

in_dir = r'c:\temp\dashboard_sample'
out_file = r'c:\temp\dashboard_sample.csv'

sample_data = []

for file in os.listdir(in_dir):
    rpt_name = file.split(sep='.')[0]
    file_iter = os.path.join(in_dir, file)
    df_iter = pd.read_csv(file_iter, nrows=1, dtype='str')
    col = df_iter.columns.tolist()
    data = df_iter.values.tolist()[0]
    for i in range(0, len(col)):
        sample_data.append([rpt_name, col[i], data[i]])

df_out = pd.DataFrame(columns=['report', 'field', 'value'], data=sample_data)
df_out.to_csv(path_or_buf=out_file, index=False)
print('success')
