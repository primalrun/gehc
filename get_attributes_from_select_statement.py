import pandas as pd
import os

file_in = r'c:\temp\sap_asset_books_select.sql'
file_out = r'c:\temp\get_attributes_from_select_statement.xlsx'

with open(file_in, 'r') as fr:
    lines_raw = fr.readlines()

sql_keywords = [
    'select'
    , 'from'
    , 'join'
    , 'where'
    , 'group by'
    , 'union'
    , 'union all'
]

sql_attributes = []
sql_keyword_iter = ''

for i, x in enumerate(lines_raw):
    row_index = i

    if any(s in x for s in sql_keywords) is True:
        keyword_count_iter = 0
        sql_keyword_iter = ''

        for k in sql_keywords:
            if k in x:
                keyword_count_iter += 1
                if len(sql_keyword_iter) == 0:
                    sql_keyword_iter = k
                else:
                    sql_keyword_iter = sql_keyword_iter + ', ' + k
        sql_keyword_count = keyword_count_iter
    else:
        sql_keyword_count = 0

    # row number, keyword count, keyword(s) string, data
    sql_attributes.append([i + 1, sql_keyword_count, sql_keyword_iter, x])

df_sql_attributes = pd.DataFrame(data=sql_attributes, columns=['row_number', 'keyword_count', 'keyword(s)', 'data'])
df_sql_attributes.to_excel(excel_writer=file_out, index=False)




os.startfile(file_out)

