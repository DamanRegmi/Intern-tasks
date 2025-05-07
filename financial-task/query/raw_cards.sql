select
id,
client_id,
card_brand,
card_type,
cast(regexp_replace(credit_limit,'[$]','') as decimal(18,3)) as credit_limit                  
from cards_data
