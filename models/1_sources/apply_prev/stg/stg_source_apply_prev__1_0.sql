with source as (

    select * from {{ source('apply_prev', 'train_applprev_1_0') }}

)

, stg as (

    select
          {{ dbt_utils.generate_surrogate_key(['s.case_id', 's.num_group1']) }} as case_group_apply_prev_1_key
        , s.case_id
        , s.num_group1 as num_group_1
        , s.approvaldate_319D as approval_date
        , s.status_219L as prev_apply_status
        , s.actualdpd_943P as actual_days_past_due_of_prev_contract
        , s.annuity_853A as monthly_annuity

        , nvl2(s.approvaldate_319D, 1, 0)::boolean as was_approved

/*
    , byoccupationinc_3656910L NUMBER(38, 2)
    , cancelreason_3545846M VARCHAR(36)
    , childnum_21L NUMBER(38, 2)
    , creationdate_885D DATE
    , credacc_actualbalance_314A NUMBER(38, 2)
    , credacc_credlmt_575A NUMBER(38, 2)
    , credacc_maxhisbal_375A NUMBER(38, 2)
    , credacc_minhisbal_90A	NUMBER(38, 2)
    , credacc_status_367L VARCHAR(36)
    , credacc_transactions_402L	NUMBER(38, 2)
    , credamount_590A NUMBER(38, 2)
    , credtype_587L VARCHAR(36)
    , currdebt_94A NUMBER(38, 2)
    , dateactivated_425D DATE
    , district_544M VARCHAR(36)
    , downpmt_134A NUMBER(38, 2)
    , dtlastpmt_581D DATE
    , dtlastpmtallstes_3545839D	DATE
    , education_1138M VARCHAR(36)
    , employedfrom_700D DATE
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