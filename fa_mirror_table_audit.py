import pandas as pd
import os
import redshift_connector as rc

print(os.environ['redshift_pass_prod_finance'])

conn = rc.connect(
    host='odp-fin-prod-etl-redshift.cil9qhmk4spr.us-east-1.redshift.amazonaws.com'
    ,database='gehc_data'
    ,user=os.environ['redshift_user']
    ,password=os.environ['redshift_pass_prod_finance']
)



out_file_dir = r'c:\temp\\'

schema_table = [
    'glprod_fr.fa_additions_b'
    # , 'glprod_fr.fa_additions_tl'
    # , 'glprod_fr.fa_adjustments'
    # , 'glprod_fr.fa_asset_history'
    # , 'glprod_fr.fa_asset_invoices'
    # , 'glprod_fr.fa_asset_keywords'
    # , 'glprod_fr.fa_book_controls'
    # , 'glprod_fr.fa_books'
    # , 'glprod_fr.fa_categories'
    # , 'glprod_fr.fa_categories_b'
    # , 'glprod_fr.fa_categories_tl'
    # , 'glprod_fr.fa_category_books'
    # , 'glprod_fr.fa_deprn_detail'
    # , 'glprod_fr.fa_deprn_periods'
    # , 'glprod_fr.fa_deprn_summary'
    # , 'glprod_fr.fa_distribution_history'
    # , 'glprod_fr.fa_locations'
    # , 'glprod_fr.fa_lookups_tl'
    # , 'glprod_fr.fa_methods'
    # , 'glprod_fr.fa_retirements'
    # , 'glprod_fr.fa_transaction_headers'
    # , 'glprod_fr_alpha_ext.fnd_flex_values'
    # , 'glprod_fr_alpha_ext.gl_code_combinations'
    # , 'glprod_fr_alpha_ext.per_all_people_f'

]

cursor = conn.cursor()
sql_row_count = ''

for st in schema_table:
    sql_row_count = sql_row_count + f"select '{st}', count(*) from {st} union all "

sql_row_count = sql_row_count[:-11]
cursor.execute(sql_row_count)
row_count = cursor.fetch_dataframe().values.tolist()
small_count = []

for r in row_count:
    if r[1] < 10:
        small_count.append([r[0], r[1]])

small_count_dict = {}
for r in small_count:
    small_count_dict[r[0]] = r[1]


for st in schema_table:
    schema_name = st.split(sep='.')[0]
    table_name = st.split(sep='.')[1]

    sql_data_type = f"""
    select table_schema,
        table_name,
        ordinal_position as position,
        column_name,
        data_type,
        case when character_maximum_length is not null
            then character_maximum_length
            else numeric_precision end as max_length,
        is_nullable,
        column_default as default_value
    from information_schema.columns
    where 1 = 1
        and table_schema = '{schema_name}'
        and table_name = '{table_name}'
    order by ordinal_position;
    """

    df_columns = [
        'table_schema'
        ,'table_name'
        ,'ordinal_position'
        ,'column_name'
        ,'data_type'
        ,'max_length'
        ,'is_nullable'
        ,'default_value'
    ]

    cursor.execute(sql_data_type)
    df_data_type = cursor.fetch_dataframe()
    df_data_type.columns = df_columns


    sql_select = f"""
    select *
    from {schema_name}.{table_name}
    limit 10
    """


    cursor.execute(sql_select)
    df_select = cursor.fetch_dataframe().transpose()

    if st in small_count_dict:
        column_count = small_count_dict[st]
        placeholder_count = 10 - column_count
        columns = []
        for i in range(0, column_count):
            columns.append('sample_' + str(i + 1))
        df_select.columns = columns

        for i in range(column_count, column_count + placeholder_count):
            column_iter = 'sample_' + str(i + 1)
            df_select[column_iter] = None
    else:
        columns = []
        for i in range(0, 10):
            columns.append('sample_' + str(i + 1))
        df_select.columns = columns


    for i in range(0, 10):
        column_iter = 'sample_' + str(i + 1)
        df_data_type[column_iter] = df_select[column_iter].to_numpy()

    df_data_type.to_excel(out_file_dir + 'out_' + st + '.xlsx', index=False)

print('success')



