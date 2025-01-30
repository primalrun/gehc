import pandas as pd
import os
import sys

file_control = r'C:\temp\read_fdl_car_glprod_control.xlsx'
file_in = r'C:\temp\FDL_Capital_Assets_Register_GLPROD.xlsx'
file_out = r'C:\temp\1.xlsx'
file_sample = r'C:\temp\read_fdl_car_glprod_sample.xlsx'
file_missing_column = r'C:\temp\read_fdl_car_glprod_missing_column.xlsx'

if os.path.exists(file_control) is True:
    bool_control_file_exists = True
    df_control_file = pd.read_excel(io=file_control, header=0)
    dict_control = df_control_file.to_dict(orient='list')
else:
    df_in = pd.read_excel(io=file_in, sheet_name='Results', header=0, nrows=1)
    column_in = df_in.columns.tolist()
    value_in = df_in.values.tolist()[0]
    fdl_data = []
    fdl_data.append(value_in)

    dict_sample = {}

    #Book Type Code, Asset Number, Deprn Expense Cost Center
    #column index (1, 0, 84)
    missing_data = []
    for i in range(0, len(column_in)):
        #check for null
        if value_in[i] != value_in[i]:
            missing_data.append(column_in[i])
        #populate dict if value is not null
        else:
            # dict_sample[column_in[i]] = [(value_in[1], value_in[0], value_in[84]), (value_in[i])]
            dict_sample[column_in[i]] = f"{value_in[1]}^{value_in[0]}^{value_in[84]}^{value_in[i]}"

    df_sample = pd.DataFrame.from_dict(data=[dict_sample])
    df_sample.to_excel(excel_writer=file_sample, index=False)
    df_missing_column = pd.DataFrame(data=missing_data, columns=['missing_column'])
    df_missing_column.to_excel(excel_writer=file_missing_column, index=False)

    control_column = ['sample_data', 'missing_data_columns']
    control_data = ['complete', 'complete']
    dict_control_data = {'sample_data': 'complete'
                         ,'missing_data_columns': missing_data}
    df_control_data = pd.DataFrame(data=[dict_control_data])

    if os.path.exists(file_control) is True:
        os.remove(file_control)

    df_control_data.to_excel(excel_writer=file_control, index=False)


#loop through columns with no value to attempt to retrieve a sample
# for md in missing_data:
#     df_in = pd.read_excel(io=file_in, sheet_name='Results', header=0,
#                           usecols=['Book Type Code', 'Asset Number', 'Deprn Expense Cost Center', md])


print('success')




