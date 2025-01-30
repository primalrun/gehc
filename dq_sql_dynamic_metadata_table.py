import os

out_file = r'c:\temp\sql_iter.txt'

table = [
    'fa_additions_b'
    , 'fa_additions_tl'
    , 'fa_adjustments'
    , 'fa_asset_history'
    , 'fa_asset_invoices'
    , 'fa_asset_keywords'
    , 'fa_book_controls'
    , 'fa_books'
    , 'fa_categories'
    , 'fa_categories_b'
    , 'fa_categories_tl'
    , 'fa_category_books'
    , 'fa_deprn_detail'
    , 'fa_deprn_periods'
    , 'fa_deprn_summary'
    , 'fa_distribution_history'
    , 'fa_locations'
    , 'fa_lookups_tl'
    , 'fa_methods'
    , 'fa_retirements'
    , 'fa_transaction_headers'
    , 'fnd_flex_values'
    , 'gl_code_combinations'
    , 'per_all_people_f'
]

sql_list = []

for t in table:
    table = t

    sql = f"""
    SELECT
        lower(column_name) AS column_name ,
        lower(case when	data_type in (
            'VARCHAR2',
            'NVARCHAR2',
            'CHAR'     ,
            'NCHAR'
            )
            then 'character varying(' || coalesce(DATA_LENGTH,0) || ')'
            when data_type = 'NUMBER'
                then case
                    when coalesce(data_precision,0)=0
                    and coalesce(data_scale,0)=0
                        THEN 'numeric'					
                    when coalesce(data_precision,0)!=0
                    and coalesce(data_scale,0)=0
                        THEN 'numeric(' || data_precision || ')'
                    else 'numeric(' || data_precision || ',' || data_scale || ')'
                end
            when data_type LIKE 'TIMESTAMP%'
            AND instr(data_type,' ') > 0
                then 'timestamp '|| SUBSTR(data_type,instr(data_type,' ')+1)
            when data_type LIKE 'TIMESTAMP%'
            AND instr(data_type,' ') = 0
                then 'timestamp'
            else lower(data_type)
            END) AS data_type
    FROM all_tab_columns
    WHERE 1 = 1
        AND owner ='GL'
        and LOWER(table_name) ='{table}'
    """

    sql = sql.replace('\n', ' ')
    sql = sql.replace('\t', ' ')
    sql_list.append([sql])

with open(out_file, 'w') as fw:
    for s in sql_list:
        fw.write(s[0] + '\n')

os.startfile(out_file)
print('success')
