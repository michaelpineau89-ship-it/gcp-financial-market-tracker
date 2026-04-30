with source as (
    select * from {{ source('bronze', 'bronze_edgar_submissions') }}
),

deduped as (
    select *,
        row_number() over (
            partition by cik
            order by _ingested_at desc
        ) as _row_num
    from source
    where cik is not null
)

select
    cast(cik as STRING)         as cik,
    entity_name as entity_name,
    form                        as form_type,
    fileNumber as file_number,
    primaryDocDescription       as primary_doc_description,
    accessionNumber             as accession_number,
    _ingested_at
from deduped
where _row_num = 1
