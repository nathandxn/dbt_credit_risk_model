with source as (

    select * from {{ source('static', 'train_static_0_0') }}

)

, stg as (

    select
          s.case_id
        , 'train' as model_group
        , s.actualdpdtolerance_344P as actual_days_past_due_with_tolerance
        , s.amtinstpaidbefduel24m_4187115A as count_instal_paid_before_due_last_24_months
        , s.annuity_780A as monthly_annuity_amount
        , s.annuitynextmonth_57A as next_month_annuity_amount
        , s.applicationcnt_361L as count_apply_same_email
        , s.applications30d_658L as count_apply_last_30_days
        , s.applicationscnt_1086L as count_apply_same_phone_number
        , s.applicationscnt_464L as count_apply_same_employer
        , s.applicationscnt_629L as count_apply_same_employer_last_7_days
        , s.applicationscnt_867L as count_apply_same_mobile_phone_number
        , s.avgdbddpdlast24m_3658932P as avg_days_past_or_before_due_of_paymt_last_24_months
        , s.avgdbddpdlast3m_4187120P as avg_days_past_or_before_due_of_paymt_last_3_months

    from source as s

)

select
      s.*

from stg as s
