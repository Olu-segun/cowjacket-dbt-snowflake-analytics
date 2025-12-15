select
  order_id,
  customer_id,
  order_date,
  quantity_order,
  total_amount
from {{ ref('int_customer_orders') }}
