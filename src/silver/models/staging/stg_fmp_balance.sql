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
    symbol                                                          as ticker,
    cast(date as DATE)                                              as period_end_date,
    reportedCurrency                                                as currency,
    cik,
    filingDate as filing_date,
    acceptedDate as accepted_date,
    fiscalYear as fiscal_year,
    period,
    cast(cashAndCashEquivalents as INT64)                           as cash_and_equivalents,
    cast(shortTermInvestments as INT64)                             as short_term_investments,
    cast(cashAndShortTermInvestments as INT64)                      as cash_and_short_term_investments,
    cast(netReceivables as INT64)                                   as net_receivables,
    cast(accountsReceivables as INT64)                               as accounts_receivable,
    cast(otherReceivables as INT64)                                 as other_receivables,
    cast(inventory as INT64)                                        as inventory,
    cast(prepaids as INT64)                                         as prepaids,
    cast(otherCurrentAssets as INT64)                               as other_current_assets,
    cast(totalCurrentAssets as INT64)                               as total_current_assets,
    cast(propertyPlantEquipmentNet as INT64)                        as property_plant_equipment_net,
    cast(goodwill as INT64)                                         as goodwill,
    cast(intangibleAssets as INT64)                                 as intangible_assets,
    cast(goodwillAndIntangibleAssets as INT64)                      as goodwill_and_intangible_assets,
    cast(longTermInvestments as INT64)                              as long_term_investments,
    cast(taxAssets as INT64)                                        as tax_assets,
    cast(otherNonCurrentAssets as INT64)                            as other_non_current_assets,
    cast(totalNonCurrentAssets as INT64)                            as total_non_current_assets,
    cast(otherAssets as INT64)                                      as other_assets,
    cast(totalAssets as INT64)                                      as total_assets,
    cast(totalPayables as INT64)                                    as total_payables,
    cast(accountPayables as INT64)                                  as accounts_payable,
    cast(otherPayables as INT64)                                    as other_payables,
    cast(accruedExpenses as INT64)                                  as accrued_expenses,
    cast(shortTermDebt as INT64)                                    as short_term_debt,
    cast(capitalLeaseObligationsCurrent as INT64)                   as capital_lease_obligations_current,
    cast(taxPayables as INT64)                                      as tax_payables,
    cast(deferredRevenue as INT64)                                  as deferred_revenue,
    cast(otherCurrentLiabilities as INT64)                          as other_current_liabilities,
    cast(totalCurrentLiabilities as INT64)                          as total_current_liabilities,
    cast(longTermDebt as INT64)                                     as long_term_debt,
    cast(capitalLeaseObligationsNonCurrent as INT64)                as capital_lease_obligations_non_current,
    cast(deferredTaxLiabilitiesNonCurrent as INT64)                 as deferred_tax_liabilities_non_current,
    cast(otherNonCurrentLiabilities as INT64)                       as other_non_current_liabilities,
    cast(totalNonCurrentLiabilities as INT64)                       as total_non_current_liabilities,
    cast(otherLiabilities as INT64)                                 as other_liabilities,
    cast(totalLiabilities as INT64)                                 as total_liabilities,
    cast(treasuryStock as INT64)                                    as treasury_stock,
    cast(preferredStock as INT64)                                   as preferred_stock,
    cast(commonStock as INT64)                                      as common_stock,
    cast(retainedEarnings as INT64)                                 as retained_earnings,
    cast(accumulatedOtherComprehensiveIncomeLoss as INT64)          as accumulated_other_comprehensive_income_loss,
    cast(otherTotalStockholdersEquity as INT64)                     as other_stockholders_equity,
    cast(totalStockholdersEquity as INT64)                          as total_stockholders_equity,
    cast(totalLiabilitiesAndTotalEquity as INT64)            as total_liabilities_and_stockholders_equity,
    cast(totalInvestments as INT64)                                 as total_investments,
    cast(totalDebt as INT64)                                        as total_debt,
    cast(totalEquity as INT64)                                      as total_equity,
    safe_divide(
        cast(totalDebt as INT64),
        cast(totalEquity as INT64)
    )                                                               as debt_to_equity,
    safe_divide(
        cast(totalLiabilities as INT64),
        cast(totalAssets as INT64)
    )                                                               as liability_ratio,
    _ingested_at
from deduped
where _row_num = 1
