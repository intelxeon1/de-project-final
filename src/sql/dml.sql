
set SEARCH_path  to 'INTELXEONYANDEXRU__STAGING';
DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
	operation_id varchar(100),
	account_number_from int,
	account_number_to int,
	currency_code numeric(3),
	country varchar(100),
	status varchar(10),
	transaction_type varchar(100),
	amount bigint,
	transaction_dt timestamp(3)
)
ORDER BY status,transaction_type, transaction_dt
segmented BY hash(operation_id,transaction_dt) all nodes
PARTITION BY transaction_dt::date



DROP TABLE IF EXISTS currencies;
CREATE TABLE currencies
(
    currency_code int,
    currency_code_with int,
    date_update timestamp,
    currency_with_div float
)
ORDER BY date_update,
          currency_code,
          currency_code_with
UNSEGMENTED ALL NODES
PARTITION BY date_update::date



DROP TABLE IF EXISTS INTELXEONYANDEXRU__DWH.currencies;
CREATE TABLE INTELXEONYANDEXRU__DWH.currencies
(
    currency_code int,
    currency_code_with int,
    date_update timestamp,
    currency_with_div float
)
ORDER BY date_update,
          currency_code,
          currency_code_with
UNSEGMENTED ALL NODES
PARTITION BY date_update::date


DROP TABLE IF EXISTS INTELXEONYANDEXRU__DWH.transactions;
CREATE TABLE INTELXEONYANDEXRU__DWH.transactions (
	operation_id varchar(100),
	account_number_from int,
	account_number_to int,
	currency_code numeric(3),
	country varchar(100),
	status varchar(10),
	transaction_type varchar(100),
	amount bigint,
	transaction_dt timestamp(3)
)
ORDER BY status,transaction_type, transaction_dt
segmented BY hash(operation_id,transaction_dt) all nodes
PARTITION BY transaction_dt::date


drop table if exists INTELXEONYANDEXRU__DWH.global_metrics;
CREATE TABLE INTELXEONYANDEXRU__DWH.global_metrics
(
    date_update date,
    currency_from int,
    amount_total numeric(17),
    cnt_transactions int,
    avg_transactions_per_account numeric(17,3),
    cnt_account_make_transactions int,
    CONSTRAINT pk PRIMARY KEY (date_update,currency_from)
)
SEGMENTED BY hash(date_update, currency_from) all nodes
PARTITION BY date_update::date