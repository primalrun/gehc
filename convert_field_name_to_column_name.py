import pyperclip
import pandas as pd
import os

out_file = r'c:\temp\convert_field_name_to_column_name.xlsx'


def fx_field_to_column(p_field):
    if p_field is None:
        return p_field
    if len(p_field) == 0:
        return p_field
    return p_field.replace(' ', '_').lower()

clip_text = pyperclip.paste().splitlines()
df = pd.DataFrame(data=clip_text, columns=['field_name'], dtype='str')
df['column_name'] = df.apply(lambda x: fx_field_to_column(x['field_name']), axis=1)
df.to_excel(excel_writer=out_file, index=False)

os.startfile(out_file)
print('success')

