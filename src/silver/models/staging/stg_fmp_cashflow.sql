with source as (
    select * from {{ source('bronze', 'bronze_fmp_cashflow') }}
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
    symbol                                                  as ticker,
    cast(date as DATE)                                      as period_end_date,
    reportedCurrency                                        as currency,
    period,
    filingDate                                              as filing_date,
    acceptedDate                                            as accepted_date,
    fiscalYear                                              as fiscal_year,
    cast(netIncome as INT64)                                as net_income,
    cast(depreciationAndAmortization as INT64)              as depreciation_and_amortization,
    cast(deferredIncomeTax as INT64)                        as deferred_income_tax,
    cast(stockBasedCompensation as INT64)                   as stock_based_compensation,
    cast(changeInWorkingCapital as INT64)                   as change_in_working_capital,
    cast(accountsReceivables as INT64)                      as accounts_receivable,
    cast(inventory as INT64)                                as inventory,
    cast(accountsPayables as INT64)                         as accounts_payable,
    cast(otherWorkingCapital as INT64)                      as other_working_capital,
    cast(otherNonCashItems as INT64)                        as other_non_cash_items,
    cast(netCashProvidedByOperatingActivities as INT64)     as net_cash_provided_by_operating_activities,
    cast(netDebtIssuance as INT64)                          as net_debt_issuance,
    cast(netCommonStockIssuance as INT64)                   as net_common_stock_issuance,
    cast(commonStockIssuance as INT64)                      as common_stock_issuance,
    cast(commonStockRepurchased as INT64)                    as common_stock_repurchase,
    cast(netPreferredStockIssuance as INT64)                as net_preferred_stock_issuance,
    cast(netDividendsPaid as INT64)                         as net_dividends_paid,
    cast(preferredDividendsPaid as INT64)                   as preferred_dividends_paid,
    cast(otherFinancingActivities as INT64)                 as other_financing_activities,
    cast(netChangeInCash as INT64)                          as net_change_in_cash,
    cast(cashAtEndOfPeriod as INT64)                        as cash_at_end_of_period,
    cast(cashAtBeginningOfPeriod as INT64)                  as cash_at_beginning_of_period,
    cast(operatingCashFlow as INT64)                        as operating_cash_flow,
    cast(capitalExpenditure as INT64)                       as capital_expenditure,
    cast(freeCashFlow as INT64)                             as free_cash_flow,
    cast(incomeTaxesPaid as INT64)                          as income_tax_paid,
    cast(interestPaid as INT64)                             as interest_paid,
    _ingested_at
from deduped
where _row_num = 1
