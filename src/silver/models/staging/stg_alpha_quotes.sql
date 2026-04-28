with source as (
    select * from {{ source('bronze', 'bronze_alpha_quotes') }}
),

deduped as (
    select *,
        row_number() over (
            partition by symbol, latest_trading_day
            order by _ingested_at desc
        ) as _row_num
    from source
)

select
    symbol,
    cast(open as FLOAT64)            as open,
    cast(high as FLOAT64)            as high,
    cast(low as FLOAT64)             as low,
    cast(price as FLOAT64)           as price,
    cast(volume as INT64)            as volume,
    cast(latest_trading_day as DATE) as trading_date,
    cast(previous_close as FLOAT64)  as previous_close,
    cast(change as FLOAT64)          as price_change,
    replace(change_percent, '%', '') as change_percent_str,
    cast(
        replace(change_percent, '%', '') as FLOAT64
    )                                as change_percent,
    _ingested_at
from deduped
where _row_num = 1
