with source as (

    select * from {{ source('apply_prev', 'train_applprev_1_1') }}

)

, stg as (

    select
          s.case_id
        , 'train' as model_group
        , s.num_group1 as num_group_1
        , s.approvaldate_319D as approval_date
        , s.creationdate_885D as created_date
        , s.dateactivated_425D as contract_activation_date
        , s.firstnonzeroinstldate_307D as first_installment_date
        , s.dtlastpmt_581D as last_payment_date
        , s.dtlastpmtallstes_3545839D as last_payment_date_all
        , s.employedfrom_700D as employed_from_date
        , s.status_219L as prev_apply_status
        , s.credtype_587L as credit_type
        , s.credacc_status_367L as prev_credit_account_status
        , s.rejectreason_755M as reject_reason
        , s.rejectreasonclient_4145042M as reject_reason_client
        , s.cancelreason_3545846M as applicant_cancel_reason
        , s.inittransactioncode_279L as initial_transaction_code
        , s.district_544M as district_of_applicant_address
        , s.education_1138M as applicant_education_level
        , s.profession_152M as applicant_profession
        , s.familystate_726L as applicant_family_state
        , s.postype_4733339M as type_of_point_of_sale

        , s.pmtnum_8L as count_payments_made
        , s.tenor_203L as count_installments_in_prev_application
        , s.maxdpdtolerance_577P as max_days_past_due
        , s.outstandingdebt_522A as amount_outstanding_debt
        , s.credamount_590A as loan_amount_or_card_limit
        , s.credacc_transactions_402L as count_transactions_prev_credit_account
        , s.credacc_actualbalance_314A as actual_balance_on_credit_account
        , s.credacc_credlmt_575A as credit_card_limit_prev_application
        , s.credacc_maxhisbal_375A as max_historical_balance
        , s.credacc_minhisbal_90A as min_historical_balance
        , s.revolvingaccount_394A as applicant_revolving_account

        , s.mainoccupationinc_437A as client_main_income_amount
        , s.byoccupationinc_3656910L as applicant_occupation_income
        , s.actualdpd_943P as actual_days_past_due_of_prev_contract
        , s.annuity_853A as monthly_annuity
        , s.currdebt_94A as current_debt_amount
        , s.downpmt_134A as down_payment_amount

        , s.childnum_21L as applicant_children_count

        , s.isbidproduct_390L as is_cross_sell_product
        , s.isdebitcard_527L as is_debit_card

        , nvl2(s.approvaldate_319D, 1, 0)::boolean as was_approved
        , nvl2(s.dateactivated_425D, 1, 0)::boolean as was_contract_activated

    from source as s

)

select
      {{ dbt_utils.generate_surrogate_key(['s.case_id', 's.model_group', 's.num_group_1']) }} as case_group_apply_prev_1_key
    , {{ dbt_utils.generate_surrogate_key(['s.case_id', 's.model_group']) }} as case_model_group_key
    , s.*

from stg as s