-- Test: DIM_EMPLOYEE - SCD2 effective date ranges must not overlap
-- Validates that for each EMPLOYEE_EIN, the EFF_DT/DISC_DT ranges are non-overlapping

with versioned as (
    select
        EMPLOYEE_EIN,
        EFF_DT,
        DISC_DT,
        LEAD(EFF_DT) over (partition by EMPLOYEE_EIN order by EFF_DT) as NEXT_EFF_DT
    from {{ ref('DIM_EMPLOYEE') }}
    where IS_DELETED = false
)

select *
from versioned
where NEXT_EFF_DT is not null
  and DISC_DT >= NEXT_EFF_DT
