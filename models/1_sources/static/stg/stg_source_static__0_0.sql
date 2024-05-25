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
        , s.clientscnt_136L as count_apply_match_client_email
        , s.clientscnt_157L as count_clients_same_phone_number_as_client
        , s.clientscnt_257L as count_clients_share_alt_phone_number
        , s.clientscnt_304L as count_clients_same_phone_number
        , s.clientscnt_360L as count_clients_same_alt_and_employer_phone_number
        , s.clientscnt_493L as count_clients_match_phone_number_and_employer_phone_number
        , s.clientscnt_533L as count_clients_same_phone_number_and_alt_phone_number
        , s.clientscnt_887L as count_clients_same_employers_phone_number
        , s.clientscnt_946L as count_clients_match_mobile_and_employers_phone_number
        , s.cntincpaycont9m_3716944L as count_incoming_payments_last_9_months
        , s.cntpmts24_3658933L as count_months_any_incoming_payments_last_24_months
        , s.commnoinclast6m_3546845L as count_comms_indicating_low_income_last_6_months
        , s.credamount_770A as loan_amount_or_card_limit
        , s.credtype_322L as credit_type
        , s.currdebt_22A as current_client_debt_amount
        , s.currdebtcredtyperange_828A as current_debt_of_applicant
        , s.datefirstoffer_1144D
        , s.datelastinstal40dpd_247D
        , s.datelastunpaid_3546854D
        , s.daysoverduetolerancedd_3976961L as count_days_past_due_with_tolerance
        , s.deferredmnthsnum_166L as count_deferred_months
        , s.disbursedcredamount_1113A as disbursed_credit_amount_after_consolidation
        , s.disbursementtype_67L as disbursement_type
        , s.downpmt_116A as amount_downpayment

    from source as s

)

select
      {{ dbt_utils.generate_surrogate_key(['s.case_id', 's.model_group']) }} as case_model_group_key
    , s.*

from stg as s
