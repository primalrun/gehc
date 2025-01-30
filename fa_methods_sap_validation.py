import os
import pandas
import pandas as pd
import sqlalchemy as sa
import sys

fdl_in_file = r'C:\temp\fa_validation\xx_fa_methods.csv'
out_test = r'C:\temp\out_test.xlsx'
endpoint = 'odp-fin-dev-etl-redshift.odp.health.ge.com'
user = os.environ['rs_user']
pword = os.environ['rs_finance_nprod_sso_pword']
db_name = 'gehc_data'
port = '5439'

col_to_compare = [
    'method_code'
    , 'stl_method_flag'
    , 'deprn_basis_rule'
    , 'name'
    , 'exclude_salvage_value_flag'
    , 'attribute1'
    , 'attribute2'
    , 'attribute3'
    , 'attribute4'
    , 'attribute5'
    , 'attribute6'
    , 'attribute7'
    , 'chart_of_depreciation'
    , 'chart_of_depreciation_desc'
]
df_fdl_in = pd.read_csv(filepath_or_buffer=fdl_in_file, header=0, usecols=col_to_compare, dtype=str)
# fdl: remove chart_of_depreciation like "0*"
df_fdl_in['chart_of_depreciation_prefix'] = df_fdl_in['chart_of_depreciation'].str[0]
df_fdl_in = df_fdl_in.query("chart_of_depreciation_prefix != '0'").copy()
df_fdl_in.drop(['chart_of_depreciation_prefix'], axis=1, inplace=True)

rename_column = [c + '_fdl' for c in col_to_compare]
column_new = {k: v for (k, v) in zip(df_fdl_in.columns, rename_column)}
df_fdl_in.rename(columns=column_new, inplace=True)

conn_string = f"postgresql+psycopg2://{user}:{pword}@{endpoint}:{port}/{db_name}"
engine = sa.create_engine(conn_string)

sql_odp = """
select dk.afasl as method_code /*depreciation key*/
	,case
		when dkma.afacla = '1' then 'Y'
		else 'N'
		end as stl_method_flag
	,mm.deprn_basis_rule	
	,ndk.afatxt as name
	,case 
		when dkma.schrott = '1'
			then 'Y'
		else 'N'
		end as exclude_salvage_value_flag	
	,dkma.afarsl as attribute1
	,dkma.metdeg as attribute2
	,dkma.metper as attribute3
	,dkma.metstu as attribute4
	,dkma.methbt as attribute5
	,dkma.umstm as attribute6
	,dkma.umrprz as attribute7
	,dk.afapl as chart_of_depreciation
	,cdt.ktext as chart_of_depreciation_desc
from biosapeu_fr.t090na dk /*depreciation keys*/
inner join biosapeu_fr.t090naz dkma /*depreciation keys - method assignment*/
	on dk.mandt = dkma.mandt 
	and dk.afapl = dkma.afapl
	and dk.afasl = dkma.afasl 
inner join (
select distinct mandt
	,afapl
	,metstu
	,case 
		when bezwkz in ('01', '02', '10', '11', '12', '40') then 'COST'
		when bezwkz in ('20', '21', '22', '23', '24', '25', '26', '27') then 'NBV'
		else null
		end as deprn_basis_rule	
from biosapeu_fr.t090ns /*multilevel method*/
where 1 = 1
	and mandt = '100'
	and left(afapl, 1) <> '0'
) mm /*multilevel method*/
	on dkma.mandt = mm.mandt 
	and dkma.afapl = mm.afapl 
	and dkma.metstu = mm.metstu 
inner join biosapeu_fr.t090nat ndk /*names of depreciation keys*/
	on dk.mandt = ndk.mandt 
	and ndk.spras = 'E'
	and dk.afapl = ndk.afapl 
	and dk.afasl = ndk.afasl
inner join biosapeu_fr.t096t cdt /*chart of depreciation texts*/
	on dk.mandt = cdt.mandt 
	and cdt.spras = 'E'
	and dk.afapl = cdt.afapl 
where 1 = 1
	and dk.mandt = '100'
	and left(dk.afapl, 1) <> '0'
"""

df_odp_in = pd.read_sql(sql=sql_odp, con=engine)
df_odp_in['attribute7'] = df_odp_in['attribute7'].astype('float')
df_odp_in['attribute7'] = df_odp_in['attribute7'].map('{:.1f}'.format)
df_odp_in['attribute7'] = df_odp_in['attribute7'].astype('str')

rename_column = [c + '_odp' for c in col_to_compare]
column_new = {k: v for (k, v) in zip(df_odp_in.columns, rename_column)}
df_odp_in.rename(columns=column_new, inplace=True)

# df_fdl_in.to_excel(out_test, index=False)
# df_odp_in.to_excel(out_test, index=False)
# os.startfile(out_test)

join_columns = [
    'chart_of_depreciation'
    , 'method_code'
    , 'deprn_basis_rule'
    , 'attribute1'
    , 'attribute2'
    , 'attribute3'
    , 'attribute4'
    , 'attribute5'
    , 'attribute6'
    , 'attribute7'
]

join_columns_fdl = [c + '_fdl' for c in join_columns]
join_columns_odp = [c + '_odp' for c in join_columns]
df_combo = pd.merge(df_fdl_in, df_odp_in, left_on=join_columns_fdl, right_on=join_columns_odp, how='left')

df_combo['method_code_compare'] = df_combo['method_code_fdl'].equals(df_combo['method_code_odp'])
df_combo['stl_method_flag_compare'] = df_combo['stl_method_flag_fdl'].equals(df_combo['stl_method_flag_odp'])
df_combo['deprn_basis_rule_compare'] = df_combo['deprn_basis_rule_fdl'].equals(df_combo['deprn_basis_rule_odp'])
df_combo['name_compare'] = df_combo['name_fdl'].equals(df_combo['name_odp'])
df_combo['exclude_salvage_value_flag_compare'] = df_combo['exclude_salvage_value_flag_fdl'].equals(
    df_combo['exclude_salvage_value_flag_odp'])
df_combo['attribute1_compare'] = df_combo['attribute1_fdl'].equals(df_combo['attribute1_odp'])
df_combo['attribute2_compare'] = df_combo['attribute2_fdl'].equals(df_combo['attribute2_odp'])
df_combo['attribute3_compare'] = df_combo['attribute3_fdl'].equals(df_combo['attribute3_odp'])
df_combo['attribute4_compare'] = df_combo['attribute4_fdl'].equals(df_combo['attribute4_odp'])
df_combo['attribute5_compare'] = df_combo['attribute5_fdl'].equals(df_combo['attribute5_odp'])
df_combo['attribute6_compare'] = df_combo['attribute6_fdl'].equals(df_combo['attribute6_odp'])
df_combo['attribute7_compare'] = df_combo['attribute7_fdl'].equals(df_combo['attribute7_odp'])
df_combo['chart_of_depreciation_compare'] = df_combo['chart_of_depreciation_fdl'].equals(
    df_combo['chart_of_depreciation_odp'])
df_combo['chart_of_depreciation_desc_compare'] = df_combo['chart_of_depreciation_desc_fdl'].equals(
    df_combo['chart_of_depreciation_desc_odp'])

# comparison results
compare_columns = [c for c in df_combo.columns if c[-8:] == '_compare']
results = {}

for c in compare_columns:
    column_iter = c
    total_count = df_combo[column_iter].count()
    true_count = df_combo[column_iter].sum()
    false_count = (~df_combo[column_iter]).sum()
    results[c] = [total_count, true_count, false_count]

df_results = pd.DataFrame.from_dict(data=results, orient='index', columns=['total_count', 'match_count', 'mismatch_count'])

with pd.ExcelWriter(out_test) as writer:
    df_combo.to_excel(writer, index=False, sheet_name='comparison')
    df_results.to_excel(writer, index=False, sheet_name='results')

os.startfile(out_test)
