with stg as (
    select * from {{ ref('stg_orders') }}
)

select
    customer,
    sum(amount) as total_amount,
    count(order_id) as total_orders
from stg
group by customer
