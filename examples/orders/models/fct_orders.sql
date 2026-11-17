{% set as_of = var('as_of', '2026-03-15T00:00:00') %}
{% set day = as_of[:10] %}
{% set mode = var('mode', 'broken_merge') %}
{% set predicates = ["DBT_INTERNAL_DEST.order_date >= date '" ~ day ~ "' - interval 7 day"]
   if mode == 'broken_merge' else [] %}

{{ config(materialized='incremental', incremental_strategy='merge',
          unique_key='order_id', incremental_predicates=predicates) }}

select order_id, order_date, updated_at,
       {% if mode == 'shared_amount' %}amount_cents + 1 as amount_cents
       {% else %}amount_cents{% endif %}
from {{ source('raw', 'orders') }} as src
where updated_at <= timestamp '{{ as_of }}'
{% if is_incremental() %}
  {% if mode == 'strict_watermark' %}
    and updated_at > (select coalesce(max(updated_at), timestamp '1970-01-01') from {{ this }})
  {% elif mode == 'skip_current_batch' %}
    and updated_at < timestamp '{{ as_of }}'
  {% else %}
    and not exists (
              select 1 from {{ this }} as previous
              where previous.order_id = src.order_id
                and previous.order_date is not distinct from src.order_date
                and previous.updated_at is not distinct from src.updated_at
                and previous.amount_cents is not distinct from src.amount_cents
            )
  {% endif %}
{% endif %}
