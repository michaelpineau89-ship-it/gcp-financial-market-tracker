with source as (
    select * from {{ source('bronze', 'bronze_fmp_balance') }}
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
    symbol                                        as ticker,
    cast(date as DATE)                            as period_end_date,
    period,
    cast(totalAssets as INT64)                    as total_assets,
    cast(totalLiabilities as INT64)               as total_liabilities,
    cast(totalEquity as INT64)                    as total_equity,
    cast(cashAndCashEquivalents as INT64)         as cash_and_equivalents,
    cast(totalDebt as INT64)                      as total_debt,
    safe_divide(
        cast(totalDebt as INT64),
        cast(totalEquity as INT64)
    )                                             as debt_to_equity,
    safe_divide(
        cast(totalLiabilities as INT64),
        cast(totalAssets as INT64)
    )                                             as liability_ratio,
    _ingested_at
from deduped
where _row_num = 1
