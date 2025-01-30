import pandas as pd
import os
import sqlalchemy as sa
from urllib.parse import quote_plus
import sys

out_file = r'c:\temp\out.xlsx'

endpoint = 'odp-fin-dev-etl-redshift.odp.health.ge.com'
user = os.environ['rs_user']
pword = quote_plus(os.environ['rs_finance_nprod_sso_pword'])
db_name = 'gehc_data'
port = '5439'
conn_string = f"postgresql+psycopg2://{user}:{pword}@{endpoint}:{port}/{db_name}"
engine = sa.create_engine(conn_string)

sql = f"""
select column_name
    ,data_type
from svv_columns
where 1 = 1
    and table_schema = 'glprod_fr'
    and table_name = 'fa_additions_b'
order by ordinal_position
"""

df = pd.read_sql(sql=sql, con=engine)
df.to_excel(excel_writer=out_file, index=False)
os.startfile(out_file)
