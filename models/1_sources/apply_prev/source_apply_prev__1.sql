{{ 
    config(
          materialized='table'
        ) 
    }}

with stg as (

    select * from {{ ref('stg_source_apply_prev__1_0') }}

)

select * from stg