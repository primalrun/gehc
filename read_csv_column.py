import pandas as pd
import os

in_file = r'C:\temp\GPAS_LITE_MST_DETAILS_JE_20240319_113000.csv'
out_file = r'C:\temp\out.xlsx'

def set_exponent_flag(p_debit, p_credit):
    if 'E' in p_debit or 'E' in p_credit:
        return 1
    else:
        return 0

df = pd.read_csv(filepath_or_buffer=in_file, dtype=str)
debit_credit = df[['accounted_debit', 'accounted_credit']].copy()
debit_credit['exponent_flag'] = debit_credit.apply(
    lambda x: set_exponent_flag(x['accounted_debit'], x['accounted_credit']) , axis=1)

debit_credit.to_excel(excel_writer=out_file, index=False)
os.startfile(out_file)
print('success')

