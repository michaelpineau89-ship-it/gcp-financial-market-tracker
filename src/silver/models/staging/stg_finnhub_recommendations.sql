with source as (
    select * from {{ source('bronze', 'bronze_finnhub_recommendations') }}
),

deduped as (
    select *,
        row_number() over (
            partition by ticker, period
            order by _ingested_at desc
        ) as _row_num
    from source
    where period is not null
)

select
    ticker,
    cast(period as DATE)        as period,
    cast(strongBuy as INT64)    as strong_buy,
    cast(buy as INT64)          as buy,
    cast(hold as INT64)         as hold,
    cast(sell as INT64)         as sell,
    cast(strongSell as INT64)   as strong_sell,
    (
        cast(strongBuy as INT64) + cast(buy as INT64)
    )                           as total_bullish,
    (
        cast(sell as INT64) + cast(strongSell as INT64)
    )                           as total_bearish,
    _ingested_at
from deduped
where _row_num = 1
