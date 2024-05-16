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
        , s.avgdbdtollast24m_4525197P as avg_days_past_or_before_due_of_paymt_last_24_months_with_tolerance
        , s.avgdpdtolclosure24_3658938P as avg_days_past_due_of_paymt_last_24_months_with_tolerance_from_max_close_date
        , s.avginstallast24m_3658937A as avg_instals_paid_last_24_months
        , s.avglnamtstart24m_4525187A as avg_loan_amt_last_24_months
        , s.avgmaxdpdlast9m_3716943P as avg_days_past_due_last_9_months
        , s.avgoutstandbalancel6m_4187114A as avg_outstanding_bal_last_6_months
        , s.avgpmtlast12m_4525200A as avg_of_pays_made_last_12_months
        , s.bankacctype_710L as type_of_bank_account
        , s.cardtype_51L as type_of_credit_card
        , s.clientscnt12m_3712952L as count_clients_have_used_same_mobile_number_last_12_months
        , s.clientscnt3m_3712950L as count_clients_have_used_same_mobile_number_last_3_months
        , s.clientscnt6m_3712949L as count_clients_have_used_same_mobile_number_last_6_months
        , s.clientscnt_100L as count_apply_match_employers_phone_and_clients
        , s.clientscnt_1022L as count_clients_share_same_mobile_phone
        , s.clientscnt_1071L as count_apply_match_client_alt_phone
        , s.clientscnt_1130L as count_apply_client_match_alt_phone

    from source as s

)

select
      {{ dbt_utils.generate_surrogate_key(['s.case_id', 's.model_group']) }} as case_model_group_key
    , s.*

from stg as s
