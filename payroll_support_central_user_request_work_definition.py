import pandas as pd
import os
import sys
import sqlalchemy as sa
from urllib.parse import quote_plus

#this script is designed for 1 sso_id and 1 pii_enabled value (either 'Y' or 'N')
#variables needing updates before script is run
    #sso_id
    #pii_enabled
    #data_change_ticket
#the result will be a text file with the insert script

#variables
in_file = r'c:\temp\gpas_user_request.txt'
sso_id = '302014338'
pii_enabled = 'Y'
posting_agent = '504009762'
data_change_ticket = 'RITM0050313'
endpoint = 'odp-fin-prod-etl-redshift.cil9qhmk4spr.us-east-1.redshift.amazonaws.com'
user = os.environ['rs_user']
pword = quote_plus(os.environ['rs_finance_prod_sso_pword'])
db_name = 'gehc_data'
port = '5439'
out_file_dir = r'c:\temp'


conn_string = f"postgresql+psycopg2://{user}:{pword}@{endpoint}:{port}/{db_name}"
engine = sa.create_engine(conn_string)


def parse_company_code(p_company_code_desc):
    if '-GPAS' in p_company_code_desc:
        return p_company_code_desc[p_company_code_desc.index('-GPAS') - 4:p_company_code_desc.index('-GPAS')]
    if p_company_code_desc[-5] == '-' and p_company_code_desc[-4:] != 'GPAS':
        return p_company_code_desc[-4:]
    else:
        return 'error'

data_origin = f'Manual - {data_change_ticket}'
out_file_name = f'GPAS_{data_change_ticket}.txt'
out_file = os.path.join(out_file_dir, out_file_name)

df_request = pd.read_csv(filepath_or_buffer=in_file, delimiter='\r\n', names=['company_code_desc'])
df_request['sso_id'] = sso_id
df_request['company_code'] = df_request.apply(lambda x: parse_company_code(x['company_code_desc']), axis=1)
df_request['pii_enabled'] = pii_enabled
df_error = df_request[df_request['company_code'] == 'error']

if len(df_error) > 0:
    print('error converting company code, details below, process cancelled')
    print(df_error)
    sys.exit()

sql_current_privilege = f"""
select sso_id::varchar as sso_id 
	,company_code 
	,pii_enabled 
from hdl_prl_sem_data_sensitive.gpas_security_rule_d
where sso_id = '{sso_id}'
"""
df_current_privilege = pd.read_sql(sql=sql_current_privilege, con=engine)

#check if current user security has any duplicates for (sso_id, company_code)
df_duplicate_mapping = df_current_privilege[df_current_privilege.duplicated(subset=['sso_id', 'company_code'], keep=False)]

if len(df_duplicate_mapping) > 0:
    print('duplicate user security records, details below, process cancelled')
    print(df_duplicate_mapping)
    sys.exit()

df_comparison = pd.merge(df_request, df_current_privilege, on=['sso_id', 'company_code'], how='left', indicator='exists')
df_insert = df_comparison[['sso_id', 'company_code', 'pii_enabled_x', 'exists']]
df_insert = df_insert[df_insert['exists'] == 'left_only']
df_insert = df_insert.drop('exists', axis=1)
df_insert.rename(columns={'pii_enabled_x': 'pii_enabled'}, inplace=True)
insert_list = df_insert.values.tolist()

insert_string = ''

for i in insert_list:
    sso_id = i[0]
    pii_enabled = i[2]
    company_code = i[1]

    insert_iter_string = f"""
    INSERT INTO hdl_prl_sem_data_sensitive.gpas_security_rule_d 
    (sso_id, pii_enabled, company_code, full_access, load_dtm, update_dtm, posting_agent, data_origin)
    VALUES ('{sso_id}','{pii_enabled}','{company_code}','',GETDATE(),GETDATE(),'{posting_agent}','{data_origin}');            
    """

    insert_string = insert_string + '\n' + insert_iter_string

with open(out_file, 'w') as fw:
    fw.write(insert_string)

os.startfile(out_file)

company_codes = [i[1] for i in insert_list]
company_code_str = ', '.join(f"'{c}'" for c in company_codes)

sql_validation = f"""
select * 
from hdl_prl_sem_data_sensitive.gpas_security_rule_d
where sso_id = {insert_list[0][0]}
and pii_enabled = '{insert_list[0][2]}'
and company_code in ({company_code_str})
"""

print('success')
print('\n')
print('validation sql')
print('\n')
print(sql_validation)

