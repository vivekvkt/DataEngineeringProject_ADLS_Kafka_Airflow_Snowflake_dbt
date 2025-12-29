with ranked_orders as (

    select
        order_id,
        customer,
        amount,
        event_time,

        row_number() over (
            partition by order_id
            order by event_time desc
        ) as rn

    from {{ source('demo_db', 'raw_orders') }}

)

select
    order_id,
    customer,
    amount,
    event_time
from ranked_orders
where rn = 1
