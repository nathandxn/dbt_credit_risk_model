{{ 
    config(
          materialized='table',
          cluster_by=['model_group', 'case_id']
        ) 
    }}

with stg_0 as (

    select * from {{ ref('stg_source_static__0_0') }}

)

select * from stg_0
