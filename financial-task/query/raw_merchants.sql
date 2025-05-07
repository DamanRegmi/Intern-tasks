select distinct
t.merchant_id,
t.merchant_city,
t.merchant_state,
cast(cast(t.zip as int) as string) zip,
m.category 
from  transactions_data t 
left join mcc_codes m
on t.mcc=m.mcc