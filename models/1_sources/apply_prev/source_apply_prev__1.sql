{{ 
    config(
          materialized='table'
        ) 
    }}

with stg_0 as (

    select * from {{ ref('stg_source_apply_prev__1_0') }}

)

, stg_1 as (

    select * from {{ ref('stg_source_apply_prev__1_1') }}

)

select * from stg_0
union all
select * from stg_1