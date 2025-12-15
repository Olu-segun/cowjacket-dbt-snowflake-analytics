select
  order_id,
  customer_id,
  order_date,
  total_quantity,
  total_amount
from {{ ref("int_orders") }}
