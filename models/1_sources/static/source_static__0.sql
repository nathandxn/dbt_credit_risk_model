{{ 
    config(
          materialized='table',
          cluster_by=['model_group', 'case_id']
        ) 
    }}

with stg_0 as (

    select * from {{ ref('stg_source_static__0_0') }}

)

, stg_1 as (

    select * from {{ ref('stg_source_static__0_1') }}

)

select * from stg_0
union all
select * from stg_1
