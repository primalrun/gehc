import pyperclip
import os

out_file = r'c:\temp\convert_comma_delited_to_new_line_separated.txt'
text = pyperclip.paste()

with open(out_file, 'w') as fw:
    for s in text.split(', '):
        fw.write(s + '\n')

os.startfile(out_file)
print('success')

