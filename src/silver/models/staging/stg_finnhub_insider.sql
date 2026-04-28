with source as (
    select * from {{ source('bronze', 'bronze_finnhub_insider') }}
),

deduped as (
    select *,
        row_number() over (
            partition by ticker, year, month
            order by _ingested_at desc
        ) as _row_num
    from source
    where year is not null and month is not null
)

select
    ticker,
    symbol,
    cast(year as INT64)   as year,
    cast(month as INT64)  as month,
    date(cast(year as INT64), cast(month as INT64), 1) as period_start,
    cast(change as FLOAT64) as net_purchase_shares,
    cast(mspr as FLOAT64)   as monthly_share_purchase_ratio,
    _ingested_at
from deduped
where _row_num = 1
