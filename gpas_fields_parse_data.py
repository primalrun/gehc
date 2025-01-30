import csv
import pandas as pd
import os

in_files = [
    r'C:\Users\223107692\Box\C&B\GPAS_OAS_Self_Service_Dashboard_Fields\OAS Field Names - Apurva.txt'
    , r'C:\Users\223107692\Box\C&B\GPAS_OAS_Self_Service_Dashboard_Fields\OAS Field Names Arpit.txt'
]

out_file = r'c:\temp\oas_fields.xlsx'

gpas_fields = []

for file in in_files:
    with open(file) as f:
        data = f.readlines()
        x = data[2][3:7]
        field_data = [line.strip() for line in data if line[3:7] == '"X01']
        field_data_split = [line.split('.') for line in field_data]

        field_clean_1 = []
        for r in field_data_split:
            field = r[2]
            last_quote = field.rfind('"')
            # all characters up to last quote inclusive
            clean_1 = field[:last_quote + 1]
            field_clean_1.append(
                [r[0].replace('"', '')
                    , r[1].replace('"', '')
                    , clean_1.replace('"', '')]
            )

        if len(gpas_fields) == 0:
            gpas_fields = field_clean_1
        else:
            gpas_fields = gpas_fields + field_clean_1

df_column = ['Subject', 'Field Group', 'Field Name']
df = pd.DataFrame(data=gpas_fields, columns=df_column)
df.to_excel(excel_writer=out_file, index=False)
os.startfile(out_file)
print('success')
