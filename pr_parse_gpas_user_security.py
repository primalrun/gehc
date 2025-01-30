import pandas as pd
import os

in_file = r'c:\temp\GPAS User list - HC Oct 4th.xlsx'
out_file = r'c:\temp\pr_user_security.xlsx'

# username, additional_security
df_in = pd.read_excel(io=in_file, usecols='B, J')
df_in['additional_security'] = df_in['additional_security'].fillna('NPII')
sso_security = df_in['username'].astype(str) + '_' + df_in['additional_security']
sso_security = list(sso_security.unique())
sso_unique = set([str(s).split(sep='_')[0] for s in sso_security])
sso_pii = set([str(s).split(sep='_')[0] for s in sso_security if str(s).split(sep='_')[1] == 'PII'])
sso_security = []

for s in sso_unique:
    if s in sso_pii:
        sso_security.append([s, 'PII'])
    else:
        sso_security.append([s, 'NPII'])

df_out = pd.DataFrame(data=sso_security, columns=['sso', 'security'])
df_out.to_excel(excel_writer=out_file, index=False)
os.startfile(out_file)

print('success')

