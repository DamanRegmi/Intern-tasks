select 
          id,
          date,
          client_id,
          card_id,
          merchant_id,
          cast(regexp_replace(amount,'[$]','') as decimal(18,3)) as amount,
          use_chip
          from transactions_data