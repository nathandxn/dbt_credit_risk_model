with base as (

    select * from {{ ref('source_base__cases') }}

)

select * from base