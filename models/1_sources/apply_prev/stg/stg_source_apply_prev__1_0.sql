with source as (

    select * from {{ source('apply_prev', 'train_applprev_1_0') }}

)

, stg as (

    select
          {{ dbt_utils.generate_surrogate_key(['s.case_id', 's.num_group1']) }} as case_group_apply_prev_1_key
        , s.case_id
        , s.num_group1 as num_group_1
        , s.approvaldate_319D as approval_date
        , s.creationdate_885D as created_date
        , s.dateactivated_425D as contract_activation_date
        , s.dtlastpmt_581D as last_payment_date
        , s.dtlastpmtallstes_3545839D as last_payment_date_all
        , s.employedfrom_700D as employed_from_date
        , s.status_219L as prev_apply_status
        , s.cancelreason_3545846M as applicant_cancel_reason
        
        , s.credacc_actualbalance_314A as actual_balance_on_credit_account
        , s.credacc_credlmt_575A as credit_card_limit_prev_application
        , s.credacc_maxhisbal_375A as max_historical_balance
        , s.credacc_minhisbal_90A as min_historical_balance

        , s.byoccupationinc_3656910L as applicant_occupation_income
        , s.actualdpd_943P as actual_days_past_due_of_prev_contract
        , s.annuity_853A as monthly_annuity
        , s.currdebt_94A as current_debt_amount
        , s.downpmt_134A as down_payment_amount

        , s.childnum_21L as applicant_children_count

        , nvl2(s.approvaldate_319D, 1, 0)::boolean as was_approved
        , nvl2(s.dateactivated_425D, 1, 0)::boolean as was_contract_activated

/*


    , credacc_status_367L VARCHAR(36)
    , credacc_transactions_402L	NUMBER(38, 2)
    , credamount_590A NUMBER(38, 2)
    , credtype_587L VARCHAR(36)

    , district_544M VARCHAR(36)

    , education_1138M VARCHAR(36)

    , familystate_726L VARCHAR(36)
    , firstnonzeroinstldate_307D DATE
    , inittransactioncode_279L VARCHAR(36)
    , isbidproduct_390L	BOOLEAN
    , isdebitcard_527L BOOLEAN
    , mainoccupationinc_437A NUMBER(38, 2)
    , maxdpdtolerance_577P NUMBER(38, 2)
    , outstandingdebt_522A NUMBER(38, 2)
    , pmtnum_8L NUMBER(38, 2)
    , postype_4733339M VARCHAR(36)
    , profession_152M VARCHAR(36)	
    , rejectreason_755M	VARCHAR(36)
    , rejectreasonclient_4145042M VARCHAR(36)
    , revolvingaccount_394A	NUMBER(38, 2)

    , tenor_203L NUMBER(38, 2)

*/

    from source as s

)

select * from stg