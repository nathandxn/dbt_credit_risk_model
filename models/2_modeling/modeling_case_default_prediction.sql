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
        , fs.monthly_annuity_amount as static_monthly_annuity_amount
        , fs.next_month_annuity_amount as static_next_month_annuity_amount
        , fs.count_apply_same_email as static_count_apply_same_email
        , fs.count_apply_last_30_days as static_count_apply_last_30_days
        , fs.count_apply_same_phone_number as static_count_apply_same_phone_number
        , fs.count_apply_same_employer as static_count_apply_same_employer
        , fs.count_apply_same_employer_last_7_days as static_count_apply_same_employer_last_7_days
        , fs.count_apply_same_mobile_phone_number as static_count_apply_same_mobile_phone_number
        , fs.avg_days_past_or_before_due_of_paymt_last_24_months as static_avg_days_past_or_before_due_of_paymt_last_24_months
        , fs.avg_days_past_or_before_due_of_paymt_last_3_months as static_avg_days_past_or_before_due_of_paymt_last_3_months
        , fs.avg_days_past_or_before_due_of_paymt_last_24_months_with_tolerance as static_avg_days_past_or_before_due_of_paymt_last_24_months_with_tolerance
        , fs.avg_days_past_due_of_paymt_last_24_months_with_tolerance_from_max_close_date as static_avg_days_past_due_of_paymt_last_24_months_with_tolerance_from_max_close_date
        , fs.avg_instals_paid_last_24_months as static_avg_instals_paid_last_24_months
        , fs.avg_loan_amt_last_24_months as avg_loan_amt_last_24_months
        , fs.avg_days_past_due_last_9_months as avg_days_past_due_last_9_months
        , fs.avg_outstanding_bal_last_6_months as static_avg_outstanding_bal_last_6_months
        , fs.avg_of_pays_made_last_12_months as static_avg_of_pays_made_last_12_months
        , fs.type_of_bank_account as static_type_of_bank_account
        , fs.type_of_credit_card as static_type_of_credit_card
        , {{ prefix_column_name(column_name = 'count_clients_have_used_same_mobile_number_last_12_months', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_have_used_same_mobile_number_last_3_months', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_have_used_same_mobile_number_last_6_months', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_apply_match_employers_phone_and_clients', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_share_same_mobile_phone', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_apply_match_client_alt_phone', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_apply_client_match_alt_phone', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_apply_match_client_email', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_same_phone_number_as_client', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_share_alt_phone_number', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_same_phone_number', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_same_alt_and_employer_phone_number', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_match_phone_number_and_employer_phone_number', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_same_phone_number_and_alt_phone_number', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_same_employers_phone_number', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_clients_match_mobile_and_employers_phone_number', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_incoming_payments_last_9_months', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_months_any_incoming_payments_last_24_months', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_comms_indicating_low_income_last_6_months', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'loan_amount_or_card_limit', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'credit_type', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'current_client_debt_amount', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'current_debt_of_applicant', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_days_past_due_with_tolerance', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_deferred_months', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'disbursed_credit_amount_after_consolidation', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'disbursement_type', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'amount_downpayment', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'interest_rate', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'has_sudden_changes_in_social_demographic_data', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'has_sudden_change_in_employment_length', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'count_home_phone_numbers', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'amount_initial_transaction_of_apply', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'type_initial_transaction_of_apply', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'interest_rate_of_active_credit_contract', prefix = 'static', alias = 'fs') }}
        , {{ prefix_column_name(column_name = 'interest_rate_during_grace_period', prefix = 'static', alias = 'fs') }}

    from base as b
    left join feat_static as fs
        on b.case_model_group_key = fs.case_model_group_key

)

select * from modeling
