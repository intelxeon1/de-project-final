from enum import Enum

SQL_CREATE_TABLE:str = "CREATE TABLE {new_table} like {from_table} including projections"
SQL_DROP_TABLE:str = "DROP TABLE IF EXISTS {table_name}"
SQL_COPY:str =  "copy {table} {column_format} from local '{data_directory_path}/{file_path}' DELIMITER ',' {skip_records}"
SQL_SWAP_PARTITION:str = "select swap_partitions_between_tables('{from_table}','{start_partition}','{end_partition}','{target_table}')"
SQL_COPY_PARTITION:str= "select COPY_PARTITIONS_TO_TABLE('{from_table}','{start_partition}','{end_partition}','{target_table}')"

SQL_TRUNCATE_METRICS = "truncate table INTELXEONYANDEXRU__DWH.global_metrics"
SQL_UPDATE_METRICS = """

insert into INTELXEONYANDEXRU__DWH.global_metrics
with filtered as
(
	select *,transaction_dt::date as date_update from INTELXEONYANDEXRU__DWH.transactions where account_number_from >0 and account_number_to > 0 	
),
amount as (
	select filtered.date_update, filtered.currency_code as currency_from, ROUND(100 * sum(abs(amount) * cur.currency_with_div),0) as amount_total from filtered join INTELXEONYANDEXRU__DWH.currencies cur on
		filtered.date_update::date >= cur.date_update::date AND 
		filtered.currency_code = cur.currency_code and cur.currency_code_with = '420'
	group by filtered.date_update, filtered.currency_code
)
,
cnt_tr as (
	select date_update, currency_code as currency_from, count(distinct operation_id) as cnt_transactions from filtered group by date_update, currency_code
),
avg_tr as (
	select date_update, currency_code as currency_from, round(count(distinct operation_id ) / count(distinct account_number_from),3)    as avg_transactions_per_account
	from filtered
	group by date_update, currency_code 
),
acc as (
	select date_update, currency_code as currency_from, count(distinct account_number_from)  as cnt_account_make_transactions from filtered 
	where status = 'done'
	group by date_update, currency_code

),
results as (
select amount.date_update,amount.currency_from, amount_total, cnt_transactions, avg_transactions_per_account, COALESCE(cnt_account_make_transactions,0) as cnt_account_make_transactions
from amount
join cnt_tr on 
	amount.date_update = cnt_tr.date_update and 
	amount.currency_from = cnt_tr.currency_from
join avg_tr on
	amount.date_update = avg_tr.date_update and 
	amount.currency_from = avg_tr.currency_from
left outer join acc on
	amount.date_update = acc.date_update and 
	amount.currency_from = acc.currency_from
	order by date_update,currency_from
)
select * from results
 
"""


CURRENCIES_COLUMN_FORMAT = "(currency_code,currency_code_with,date_update FORMAT 'YYYY-MM-DD',currency_with_div)"
SKIP_HEADER = "SKIP 1"


class TABLES(Enum):
	CURRENCIES_MASK = "currencies"
	CURRENCIES_STG = "INTELXEONYANDEXRU__STAGING.currencies"
	TRANSACTION_STG = "INTELXEONYANDEXRU__STAGING.transactions"
	CURRENCIES_DWH = "INTELXEONYANDEXRU__DWH.currencies"
	TRANSACTION_DWH = "INTELXEONYANDEXRU__DWH.transactions"
	TRANSACTION_MASK = 'transactions'
	METRICS = 'INTELXEONYANDEXRU__DWH.global_metrics'
