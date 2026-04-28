with source as (
    select * from {{ source('bronze', 'bronze_finnhub_financials') }}
),

deduped as (
    select *,
        row_number() over (
            partition by ticker
            order by _ingested_at desc
        ) as _row_num
    from source
)

select
    ticker,
    metric,
    _ingested_at
from deduped
where _row_num = 1
