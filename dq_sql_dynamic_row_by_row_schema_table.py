import os
import pandas as pd
import sqlalchemy as sa
import sys

out_file = r'c:\temp\sql_iter.txt'
out_error_file = r'c:\temp\error.xlsx'

schema_table = [
['gears_fr', 'appropriation_requests']
,['gears_fr', 'approval_business_configuration']
,['gears_fr', 'approval_configuration']
,['gears_fr', 'approval_rule_group_approvers']
,['gears_fr', 'approval_statuses']
,['gears_fr', 'approval_tasks']
,['gears_fr', 'approval_thresholds']
,['gears_fr', 'approver_requirements']
,['gears_fr', 'ar_amount_types']
,['gears_fr', 'ar_amounts']
,['gears_fr', 'ar_attachments']
,['gears_fr', 'ar_business_specific_values']
,['gears_fr', 'asset_categories']
,['gears_fr', 'asset_investment_categories']
,['gears_fr', 'asset_investment_types']
,['gears_fr', 'budget_attachments']
,['gears_fr', 'budget_benefits_new']
,['gears_fr', 'budget_status_types']
,['gears_fr', 'budget_statuses']
,['gears_fr', 'business_role_grants']
,['gears_fr', 'business_roles']
,['gears_fr', 'businesses']
,['gears_fr', 'cip_acf_form']
,['gears_fr', 'cip_acf_sourceline']
,['gears_fr', 'cip_form']
,['gears_fr', 'cip_form_asset_owner']
,['gears_fr', 'cip_source_line']
,['gears_fr', 'cip_source_line_ori']
,['gears_fr', 'comment_ar_subar']
,['gears_fr', 'comments']
,['gears_fr', 'company_code_roles']
,['gears_fr', 'company_codes']
,['gears_fr', 'corporate_bands']
,['gears_fr', 'cost_center_owners']
,['gears_fr', 'cost_centers']
,['gears_fr', 'currencies']
,['gears_fr', 'functions']
,['gears_fr', 'gap_codes']
,['gears_fr', 'ibioprod_audit']
,['gears_fr', 'ibioprod_fa']
,['gears_fr', 'ibioprod_gl_sourceline']
,['gears_fr', 'les']
,['gears_fr', 'logs_gears']
,['gears_fr', 'plan_comments']
,['gears_fr', 'power_asset_types']
,['gears_fr', 'product_lines']
,['gears_fr', 'reference_ids']
,['gears_fr', 'role_types']
,['gears_fr', 'sap_far']
,['gears_fr', 'sap_gl_sourceline']
,['gears_fr', 'sap_locations']
,['gears_fr', 'ssp_inbound']
,['gears_fr', 'statuses']
,['gears_fr', 'sub_ar_business_specific_values']
,['gears_fr', 'sub_ar_business_specifics']
,['gears_fr', 'sub_ar_requestors']
,['gears_fr', 'sub_ar_statuses']
,['gears_fr', 'sub_ar_transaction_types']
,['gears_fr', 'sub_businesses']
,['gears_fr', 'tr_disposal_scrap_detail']
,['gears_fr', 'tr_far']
,['gears_fr', 'tr_pl_function']
,['gears_fr', 'tr_request']
,['gears_fr', 'tr_request_commit']
,['gears_fr', 'tr_transaction_subtype']
,['gears_fr', 'tr_transaction_type']
,['gears_fr', 'user_role_members']
,['gears_fr', 'workflowdelegatetask']
,['gears_fr', 'workflowhistory']
]

data_type_lookup = {
    'bigint': 'decimal'
    ,'character': 'character same size'
    ,'character varying': 'character same size'
    ,'date': 'to_char(date)'
    ,'double precision': 'decimal'
    ,'integer': 'decimal'
    ,'numeric': 'decimal'
    ,'smallint': 'decimal'
    ,'timestamp without time zone': 'to_char(date)'
    ,'boolean': 'decimal'
}

endpoint = 'odp-fin-dev-etl-redshift.odp.health.ge.com'
user = os.environ['rs_user']
pword = os.environ['rs_finance_nprod_sso_pword']
db_name = 'gehc_data'
port = '5439'
conn_string = f"postgresql+psycopg2://{user}:{pword}@{endpoint}:{port}/{db_name}"
engine = sa.create_engine(conn_string)

def change_data_type(p_column, p_old_type):
    if p_old_type in data_type_lookup:
        new_type = data_type_lookup[p_old_type]

        if new_type == 'decimal':
            if p_old_type == 'boolean':
                return f"cast(coalesce({p_column}::int,0) as decimal(38,10)) as {p_column}"
            else:
                return f"cast(coalesce({p_column},0) as decimal(38,10)) as {p_column}"
        if new_type == 'character same size':
            return f"coalesce({p_column}, '<NULL>') as {p_column}"
        if new_type == 'to_char(date)':
            return f"coalesce(to_char({p_column},'yyyy-MM-dd hh24:mi:ss'),'0001-01-01') as {p_column}"
    else:
        return None

sql_list = []

for t in schema_table:
    schema, table = t

    sql_column = f"""
    select column_name
        ,data_type
    from svv_columns
    where 1 = 1
        and table_schema = '{schema}'
        and table_name = '{table}'
    order by ordinal_position
    """

    df_column = pd.read_sql(sql=sql_column, con=engine)
    df_column['converted_column'] = df_column.apply(lambda x: change_data_type(x['column_name']
                                                    , x['data_type'])
                                                    , axis=1)

    null_mask = df_column.isnull().any(axis=1)
    df_null_rows = df_column[null_mask]

    if len(df_null_rows) > 0:
        df_null_rows.to_excel(excel_writer=out_error_file, index=False)
        os.startfile(out_error_file)
        print('error table: ' + table)
        sys.exit()

    select_new = df_column['converted_column'].values.tolist()
    column_select = 'select ' + '\n' + ',\n'.join(select_new)

    sql_out = f"""{column_select}
    from {schema}.{table}
    where 1 = 1
    """

    sql_out = sql_out.replace('\n', ' ')
    sql_out = sql_out.replace('\t', ' ')
    sql_list.append([sql_out])

with open(out_file, 'w') as fw:
    for s in sql_list:
        fw.write(s[0] + '\n')

os.startfile(out_file)
print('success')
