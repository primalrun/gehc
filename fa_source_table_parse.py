import pandas as pd
import os

in_file = r'c:\temp\fa_table.xlsx'
out_file = r'c:\temp\fa_source_table.xlsx'

df = pd.read_excel(io=in_file)
in_data = df.values.tolist()
d1 = [[r[1], r[2]] for r in in_data]
d2 = []

for r in d1:
    source = r[0]
    tables = str(r[1])

    if ',' in tables:
        table_split_list = tables.split(', ')

        for i in range(0, len(table_split_list)):
            d2.append([source, table_split_list[i]])
    else:
        d2.append([source, tables])

df_source = pd.DataFrame(data=d2, columns=['Source Name', 'Source Table'])
df_unique_data_source = df_source.drop_duplicates().copy()

df_unique_data_source.to_excel(excel_writer=out_file, index=False)


os.startfile(out_file)
print('success')