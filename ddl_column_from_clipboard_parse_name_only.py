import pyperclip
import os

out_file = r'c:\temp\out1.txt'

clip_data = pyperclip.paste()
d1 = clip_data.splitlines()
d2 = []

for r in d1:
    s1 = r.replace('\t', '')
    s2 = s1.replace(',', '')
    space_1 = s2.find(' ')
    s3 = s2[0:space_1]
    if len(d2) > 0:
        d2.append(',' + s3)
    else:
        d2.append(s3)

with open(out_file, 'w') as fw:
    for r in d2:
        fw.write(r + '\n')

os.startfile(out_file)
print('success')
