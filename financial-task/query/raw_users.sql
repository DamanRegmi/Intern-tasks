select 
id,
gender,
to_date(concat(birth_year||"/"||birth_month||"/1"),'yyyy/M/d') as birth_date,
address,
cast(regexp_replace(yearly_income,'[$,]','') as decimal(18,3)) as yearly_income,
cast(regexp_replace(total_debt,'[$,]','') as decimal(18,3))total_debt,
cast(credit_score as int),
case
                  when credit_score between 800 and 850 then 5
                  when credit_score between 740 and 799 then 4
                  when credit_score between 670 and 739 then 3
                  when credit_score between 580 and 669 then 2
                  when credit_score between 300 and 579 then 1
                  else null
end as credit_rating,
cast(num_credit_cards as int) ,
cast(retirement_age as int),
cast(retirement_age - (2025-birth_year) as int) as years_for_retirement
from users_data