
  create or replace   view DEMO_DB.ANALYTICS.fct_orders
  
  
  
  
  as (
    with stg as (
    select * from DEMO_DB.ANALYTICS.stg_orders
)

select
    customer,
    sum(amount) as total_amount,
    count(order_id) as total_orders
from stg
group by customer
  );

