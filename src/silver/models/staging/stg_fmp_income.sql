with source as (
    select * from {{ source('bronze', 'bronze_fmp_income') }}
),

deduped as (
    select *,
        row_number() over (
            partition by symbol, date, period
            order by _ingested_at desc
        ) as _row_num
    from source
    where symbol is not null and date is not null
)

select
    symbol                                as ticker,
    cast(date as DATE)                    as period_end_date,
    period,
    cast(revenue as INT64)                as revenue,
    cast(grossProfit as INT64)            as gross_profit,
    cast(operatingIncome as INT64)        as operating_income,
    cast(netIncome as INT64)              as net_income,
    cast(eps as FLOAT64)                  as eps,
    safe_divide(
        cast(grossProfit as INT64),
        cast(revenue as INT64)
    )                                     as gross_margin,
    safe_divide(
        cast(netIncome as INT64),
        cast(revenue as INT64)
    )                                     as net_margin,
    _ingested_at
from deduped
where _row_num = 1
