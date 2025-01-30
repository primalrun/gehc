import pyperclip
import os

out_file = r'c:\temp\sql_select_to_column_name.txt'

def column_verify(str_iter):
    if ' as ' in str_iter:
        str_new = r[r.rindex(' as ') + 4:]
        return str_new
    if ' ' not in str_iter:
        if '.' in str_iter:
            str_new = r.split('.')[1]
            return str_new
        else:
            return None
    else:
        return None


text = pyperclip.paste()
data_1 = text.splitlines()
data_2 = [s.rstrip() for s in data_1]

data_3 = []

for r in data_2:
    column_verify_result = column_verify(r)
    if column_verify_result is not None:
        data_3.append(column_verify_result)

with open(out_file, 'w') as fw:
    for c in data_3:
        fw.write(c + '\n')

os.startfile(out_file)
print('success')