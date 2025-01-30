import pandas as pd
import numpy as np
import sys
import os

in_file = r'c:\temp\US182028 - Access list.xlsx'
sheet_prod = 'Finance PROD'
sheet_dev = 'Finance DEV'
out_file = r'c:\temp\odp_schema_security.xlsx'

schema_env = {
    'hdl_prl_etl_stage_sensitive': 'Fin Dev'
    ,'hdl_prl_sem_data_sensitive': 'Fin Dev'
    ,'hdl_prl_sem_view_sensitive': 'Fin Dev'
    ,'hdl_prl_etl_stage_uat_sensitive': 'Fin Dev UAT'
    ,'hdl_prl_sem_data_uat_sensitive': 'Fin Dev UAT'
    ,'hdl_prl_sem_view_uat_sensitive': 'Fin Dev UAT'
    ,'hdl_prl_etl_stage_sensitive': 'Fin Prod'
    ,'hdl_prl_sem_data_sensitive': 'Fin Prod'
    ,'hdl_prl_sem_view_sensitive': 'Fin Prod'
}


user_detail = {
     '212718864': ['Apurva Khandelwal', 'GEHC DA']
    , '223107692': ['Jason Walker', 'GEHC DA']
    , '223115412': ['Kuldip Singh', 'GEHC DA']
    , '503370023': ['Suni Thomas', 'Vendor DA']
    , '503383638': ['Ajit Gouda', 'Vendor DE']
    , '504009762': ['FSSO', 'GEHC FSSO ETL']
    , '504011355': ['CICD FSSO', 'GEHC FSSO CICD']
    , '504011431': ['Reporting FSSO', 'GEHC FSSO Reporting']
    , '503395391': ['Shriman Koli', 'Vendor VE']
    , '503389725': ['Madhusudan Sahoo', 'Vendor DevOps']
    , '223117412': ['Balaji Arisetti', 'GEHC PM']
}

data_out = []

#dev------------------------------------------------------------------------------------------------------------------
df = pd.read_excel(io=in_file, sheet_name=sheet_dev, dtype=str)

df['null_row'] = df.isna().all(axis=1)
df = df.replace(to_replace=np.NaN, value='missing')

d1 = df.values.tolist()
#remove rows with all null
d2 = [r for r in d1 if r[4] is False]

schema = ''
role = ''
user = ''
for r in range(0, len(d2)):
    schema_iter = d2[r][0]
    role_iter = d2[r][2]
    user_iter = d2[r][3]
    if schema_iter != 'missing':
        schema = schema_iter
    if role_iter != 'missing':
        role = role_iter
    if user_iter != 'missing':
        user = user_iter

    if 'uat' in schema:
        env = 'Fin Dev UAT'
    else:
        env = 'Fin Dev'

    #check if user mapping exists
    if user not in user_detail:
        print('process cancelled, user mapping missing for ' + user)
        sys.exit()
    user_name = user_detail[user][0]
    user_role = user_detail[user][1]
    data_out.append([env, schema, role, user, user_name, user_role])


#prod------------------------------------------------------------------------------------------------------------------
df = pd.read_excel(io=in_file, sheet_name=sheet_prod, dtype=str)

df['null_row'] = df.isna().all(axis=1)
df = df.replace(to_replace=np.NaN, value='missing')

d1 = df.values.tolist()
#remove rows with all null
d2 = [r for r in d1 if r[4] is False]

schema = ''
role = ''
user = ''
for r in range(0, len(d2)):
    schema_iter = d2[r][0]
    role_iter = d2[r][2]
    user_iter = d2[r][3]
    if schema_iter != 'missing':
        schema = schema_iter
    if role_iter != 'missing':
        role = role_iter
    if user_iter != 'missing':
        user = user_iter

    env = 'Fin Prod'

    #check if user mapping exists
    if user not in user_detail:
        print('process cancelled, user mapping missing for ' + user)
        sys.exit()
    user_name = user_detail[user][0]
    user_role = user_detail[user][1]
    data_out.append([env, schema, role, user, user_name, user_role])

df_col = ['Environment', 'Schema', 'Role', 'User', 'User Name', 'User Role']
df_out = pd.DataFrame(data=data_out, columns=df_col)
df_out.to_excel(excel_writer=out_file, index=False)
os.startfile(out_file)
print('success')
