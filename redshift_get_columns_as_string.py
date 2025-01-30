import pandas as pd
import os
import redshift_connector as rc

conn = rc.connect(
    host='odp-fin-dev-etl-redshift.odp.health.ge.com'
    , database='gehc_data'
    , user=os.environ['rs_user']
    , password=os.environ['rs_finance_nprod_sso_pword']
)

out_file = r'c:\temp\column_string.txt'

schema_table = [
    ['gears_fr', 'appropriation_requests']
    , ['gears_fr', 'approval_business_configuration']
    , ['gears_fr', 'approval_configuration']
    , ['gears_fr', 'approval_rule_group_approvers']
    , ['gears_fr', 'approval_statuses']
    , ['gears_fr', 'approval_tasks']
    , ['gears_fr', 'approval_thresholds']
    , ['gears_fr', 'approver_requirements']
    , ['gears_fr', 'ar_amount_types']
    , ['gears_fr', 'ar_amounts']
    , ['gears_fr', 'ar_attachments']
    , ['gears_fr', 'ar_business_specific_values']
    , ['gears_fr', 'asset_categories']
    , ['gears_fr', 'asset_investment_categories']
    , ['gears_fr', 'asset_investment_types']
    , ['gears_fr', 'budget_attachments']
    , ['gears_fr', 'budget_benefits_new']
    , ['gears_fr', 'budget_status_types']
    , ['gears_fr', 'budget_statuses']
    , ['gears_fr', 'business_role_grants']
    , ['gears_fr', 'business_roles']
    , ['gears_fr', 'businesses']
    , ['gears_fr', 'cip_acf_form']
    , ['gears_fr', 'cip_acf_sourceline']
    , ['gears_fr', 'cip_form']
    , ['gears_fr', 'cip_form_asset_owner']
    , ['gears_fr', 'cip_source_line']
    , ['gears_fr', 'cip_source_line_ori']
    , ['gears_fr', 'comment_ar_subar']
    , ['gears_fr', 'comments']
    , ['gears_fr', 'company_code_roles']
    , ['gears_fr', 'company_codes']
    , ['gears_fr', 'corporate_bands']
    , ['gears_fr', 'cost_center_owners']
    , ['gears_fr', 'cost_centers']
    , ['gears_fr', 'currencies']
    , ['gears_fr', 'functions']
    , ['gears_fr', 'gap_codes']
    , ['gears_fr', 'ibioprod_audit']
    , ['gears_fr', 'ibioprod_fa']
    , ['gears_fr', 'ibioprod_gl_sourceline']
    , ['gears_fr', 'les']
    , ['gears_fr', 'logs_gears']
    , ['gears_fr', 'plan_comments']
    , ['gears_fr', 'power_asset_types']
    , ['gears_fr', 'product_lines']
    , ['gears_fr', 'reference_ids']
    , ['gears_fr', 'role_types']
    , ['gears_fr', 'sap_far']
    , ['gears_fr', 'sap_gl_sourceline']
    , ['gears_fr', 'sap_locations']
    , ['gears_fr', 'ssp_inbound']
    , ['gears_fr', 'statuses']
    , ['gears_fr', 'sub_ar_business_specific_values']
    , ['gears_fr', 'sub_ar_business_specifics']
    , ['gears_fr', 'sub_ar_requestors']
    , ['gears_fr', 'sub_ar_statuses']
    , ['gears_fr', 'sub_ar_transaction_types']
    , ['gears_fr', 'sub_businesses']
    , ['gears_fr', 'tr_disposal_scrap_detail']
    , ['gears_fr', 'tr_far']
    , ['gears_fr', 'tr_pl_function']
    , ['gears_fr', 'tr_request']
    , ['gears_fr', 'tr_request_commit']
    , ['gears_fr', 'tr_transaction_subtype']
    , ['gears_fr', 'tr_transaction_type']
    , ['gears_fr', 'user_role_members']
    , ['gears_fr', 'workflowdelegatetask']
    , ['gears_fr', 'workflowhistory']
]

column_list = []
cursor = conn.cursor()

for t in schema_table:
    schema, table = t

    sql_select = f"""
    select column_name
    from svv_columns
    where 1 = 1
        and table_schema='{schema}'
        and table_name  ='{table}'
    order by ordinal_position
    """

    cursor.execute(sql_select)
    df = cursor.fetch_dataframe()
    column_string = ','.join(df['column_name'].tolist())
    column_list.append([column_string])

with open(out_file, 'w') as fw:
    for r in column_list:
        fw.write(r[0] + '\n')

os.startfile(out_file)
print('success')
