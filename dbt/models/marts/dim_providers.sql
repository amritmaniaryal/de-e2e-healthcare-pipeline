{#
    Mart: dim_providers
    -------------------
    GRAIN: one row per provider (152 rows).

    A provider is a PERSON -- the individual clinician who delivers care.
    It is not the same as an organisation (the facility), even though in this
    dataset they happen to be 1:1:

        providers: 152      organisations: 152
        providers per organisation: exactly 1, and no organisation has zero.

    That 1:1 is an artefact of how Synthea generates a population, not a fact
    about healthcare. Real facilities have many providers. The two dimensions
    are kept separate because "claims by provider" and "claims by organisation"
    are different questions, and they diverge the moment the data is real.

    DELIBERATELY EXCLUDED

    encounter_count, procedure_count
        Synthea's own per-provider aggregates, and they do not reconcile:
            encounter_count sums to 152,046 but there are  57,592 encounters
            procedure_count sums to      0 but there are 151,636 procedures
        procedure_count is all zeros. Provider volume should be derived from
        the claim/encounter facts, where it is traceable to individual rows.

    organisation address / city / state / zip / lat / lon
        byte-identical to the provider's own -- 0 of 152 differ. Only
        organization_name is carried across, since nothing else adds anything.

    organisation revenue / utilization
        measures, not attributes; and they belong to the organisation grain,
        not to a person.

    STRUCTURALLY CONSTANT IN THIS DATASET
        specialty -- 1 distinct value (GENERAL PRACTICE) across all 152.
        Kept as a genuine domain attribute; regenerating Synthea with a wider
        population would give it variance. Do not build a breakdown chart on it
        without checking first.
#}

with providers as (

    select * from {{ ref('stg_providers') }}

),

organizations as (

    select
        organization_id,
        name        as organization_name
    from {{ ref('stg_organizations') }}

),

final as (
    select
        -- keys
        p.provider_id,
        p.npi,                      -- real-world business key, unique per provider
        p.organization_id,

        -- attributes
        p.name,
        p.gender,
        p.specialty,                -- constant in this dataset
        o.organization_name,

        -- location
        p.address,
        p.city,
        p.state,
        p.zip,
        p.lat,
        p.lon
    from providers p
    left join organizations o
    on p.organization_id = o.organization_id
)

select * from final