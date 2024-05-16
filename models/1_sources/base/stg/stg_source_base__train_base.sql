with source as (

    select * from {{ source('base', 'train_base') }}

)

, stg as (

    select
          s.case_id
        , 'train' as model_group
        , s.date_decision as decision_date
        , s.month as decision_month
        , s.week_num as week_number
        , s.target as is_default

    from source as s

)

select
      {{ dbt_utils.generate_surrogate_key(['s.case_id', 's.model_group']) }} as case_model_group_key
    , s.*

from stg as s
