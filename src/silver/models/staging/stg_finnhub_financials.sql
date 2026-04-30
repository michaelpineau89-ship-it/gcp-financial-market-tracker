with source as (
    select * from {{ source('bronze', 'bronze_finnhub_financials') }}
),

deduped as (
    select *,
        row_number() over (
            partition by symbol
            order by _ingested_at desc
        ) as _row_num
    from source
)

select
    symbol as ticker,
    metric,
    series,
    _ingested_at
from deduped
where _row_num = 1
