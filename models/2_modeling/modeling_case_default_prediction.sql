{{ 
    config(
          materialized='table',
          cluster_by=['model_group', 'case_id']
        ) 
    }}


with base as (

    select * from {{ ref('source_base__cases') }}

)

, feat_static as (

    select * from {{ ref('source_static__0') }}

)

, modeling as (

    select
          b.case_model_group_key
        , b.case_id
        , b.model_group
        , b.is_default
        , b.decision_date
        , b.decision_month
        , b.week_number
        -- indicates if features were found for that case id
        , nvl2(fs.case_model_group_key, 1, 0)::boolean as has_static_features

        -- features
        , fs.actual_days_past_due_with_tolerance as static_actual_days_past_due_with_tolerance




    from base as b
    left join feat_static as fs
        on b.case_model_group_key = fs.case_model_group_key

)

select * from modeling
