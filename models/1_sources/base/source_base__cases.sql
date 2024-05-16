with stg as (

    select * from {{ ref('stg_source_base__train_base') }}

)

select * from stg
