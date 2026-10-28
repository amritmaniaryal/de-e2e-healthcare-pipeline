{#
    Staging: encounters
    -------------------
    Reference model for the other ten staging tables -- copy this shape.

    Rules every staging model follows:
      * one model per source table, no joins
      * rename every column to snake_case
      * foreign keys get a descriptive name  (PATIENT -> patient_id)
      * keep every column -- dropping one here removes it for the whole project
      * no business logic: no case/when, no aggregation, no row filtering
      * never quote identifiers. Snowflake folds unquoted names to uppercase,
        which matches how the columns were loaded; quoting breaks that match.

    Why the three-part shape (source -> renamed -> final select):
    it separates "where the data comes from" from "what it is called", so a
    later cast or filter has an obvious place to go that does not disturb the
    column list.
#}

with source as (

    select * from {{ source('raw', 'encounters') }}

),

renamed as (

    select
        -- primary key
        Id                      as encounter_id,

        -- timestamps
        -- START is a RESERVED WORD in Snowflake and must be quoted. The loaded
        -- column is uppercase, so "START" matches it exactly. STOP is not
        -- reserved and needs no quotes. The same applies to conditions,
        -- medications and procedures -- all three also have a START column.
        "START"                 as started_at,
        STOP                    as stopped_at,

        -- foreign keys
        PATIENT                 as patient_id,
        ORGANIZATION            as organization_id,
        PROVIDER                as provider_id,
        PAYER                   as payer_id,

        -- attributes
        ENCOUNTERCLASS          as encounter_class,
        CODE                    as encounter_code,
        DESCRIPTION             as description,

        -- costs: already decimal(12,2) from the Spark layer, no cast needed
        BASE_ENCOUNTER_COST     as base_encounter_cost,
        TOTAL_CLAIM_COST        as total_claim_cost,
        PAYER_COVERAGE          as payer_coverage,

        -- reason code is null for 20,881 of 57,592 encounters; that is expected
        REASONCODE              as reason_code,
        REASONDESCRIPTION       as reason_description

    from source

)

select * from renamed
