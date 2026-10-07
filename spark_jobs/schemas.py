"""
Per-table schemas for the Spark processing layer.

Each schema is a DDL string -- Spark accepts these directly:

    spark.read.option("header", True).schema(spec.schema).csv(path)

**IMPORTANT -- DDL syntax:** the reader uses the SQL DDL parser, which wants a
SPACE between name and type:

    Id string, BIRTHDATE date      <-- correct
    Id:string, BIRTHDATE:date      <-- PARSE_SYNTAX_ERROR

The colon form is the `struct<name:type>` syntax. createDataFrame() accepts it,
but spark.read.schema() does not.

We never rely on inference at runtime: it costs a full extra scan of every file
and it guesses wrong. Real examples from this dataset:
  - ZIP / FIPS / NPI came back as int, destroying leading zeros
  - procedures.CODE holds SNOMED codes up to 17 digits, so int would overflow
  - claims_transactions.PROCEDURECODE has values like "03" that lost a zero

--------------------------------------------------------------------------
HOW TO ADD A TABLE
--------------------------------------------------------------------------
1. Draft it in the Spark REPL:

       df = (spark.read.option("header", True).option("inferSchema", True)
             .csv(f"s3a://{B}/raw/{table}/ingestion_date={D}/{table}.csv"))
       print(df.schema.simpleString())

2. Paste the field list into a new constant below, using SPACES not colons.

3. Fix identifier columns back to string. The rule: a CODE or a LABEL rather
   than a QUANTITY. So ZIP, FIPS, SSN, NPI, PHONE, UUID *_ID, CODE,
   REASONCODE and DIAGNOSIS* are strings. Small surrogate integers
   (CHARGEID, DEPARTMENTID, UNITS, DISPENSES) stay int.

4. Register it in TABLES with its primary key.

Because the row counts are small, checking a column's real values locally is
often faster than a Spark round trip:

    python3 -c "import csv;r=list(csv.DictReader(open('file.csv')));print(sorted({x['COL'] for x in r})[:5])"
--------------------------------------------------------------------------
NOTE: DDL carries no metadata and no nullability. Fields come back
nullable=True, metadata={}. That is fine here -- nullability isn't enforced on
CSV reads, and metadata only matters when writing to a catalog (Snowflake /
Delta, Week 3-4). Switch a table to explicit StructField objects if you ever
need per-column comments or NOT NULL constraints.

NOTE ON NULLS: several columns are legitimately empty and must not be treated
as data-quality failures. They are called out per table below.
--------------------------------------------------------------------------
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    """Everything that differs between tables: the schema and the dedup key."""

    schema: str                     # DDL text, e.g. "Id string, ZIP string"
    primary_key: tuple[str, ...]    # ("Id",)  or a composite tuple


# ============================================================== patients
# 1,096 rows, 28 cols. PK: Id.
# Changes vs raw inference: ZIP/FIPS int->string (identifiers); money
# double->decimal(12,2).
# Legitimately null (8 cols): SUFFIX 1084, DEATHDATE 1000 (most patients are
# alive), MAIDEN 829, MARITAL 429, PASSPORT 302, PREFIX 278, DRIVERS 245,
# MIDDLE 236. All confirmed equal to the source blank counts.
PATIENTS = """
    Id string, BIRTHDATE date, DEATHDATE date, SSN string, DRIVERS string,
    PASSPORT string, PREFIX string, FIRST string, MIDDLE string, LAST string,
    SUFFIX string, MAIDEN string, MARITAL string, RACE string, ETHNICITY string,
    GENDER string, BIRTHPLACE string, ADDRESS string, CITY string, STATE string,
    COUNTY string, FIPS string, ZIP string, LAT double, LON double,
    HEALTHCARE_EXPENSES decimal(12,2), HEALTHCARE_COVERAGE decimal(12,2), INCOME int
"""


# ============================================================= providers
# 152 rows, 14 cols. PK: Id.
# ZIP and NPI are identifiers -> string. ENCOUNTERS/PROCEDURES are counts.
PROVIDERS = """
    Id string, ORGANIZATION string, NAME string, GENDER string, SPECIALITY string,
    ADDRESS string, CITY string, STATE string, ZIP string, LAT double, LON double,
    ENCOUNTERS int, PROCEDURES int, NPI string
"""


# ========================================================= organizations
# 152 rows, 12 cols. PK: Id.
# UTILIZATION is genuinely an integer in this dataset, not a ratio.
ORGANIZATIONS = """
    Id string, NAME string, ADDRESS string, CITY string, STATE string, ZIP string,
    LAT double, LON double, PHONE string, REVENUE decimal(12,2),
    UTILIZATION int, NPI string
"""


# ================================================================ payers
# 10 rows, 22 cols. PK: Id.
# Legitimately null: ADDRESS, CITY, STATE_HEADQUARTERED, ZIP, PHONE are blank
# in ALL 10 rows -- Synthea does not populate them for payers.
PAYERS = """
    Id string, NAME string, OWNERSHIP string, ADDRESS string, CITY string,
    STATE_HEADQUARTERED string, ZIP string, PHONE string,
    AMOUNT_COVERED decimal(12,2), AMOUNT_UNCOVERED decimal(12,2),
    REVENUE decimal(12,2), COVERED_ENCOUNTERS int, UNCOVERED_ENCOUNTERS int,
    COVERED_MEDICATIONS int, UNCOVERED_MEDICATIONS int, COVERED_PROCEDURES int,
    UNCOVERED_PROCEDURES int, COVERED_IMMUNIZATIONS int, UNCOVERED_IMMUNIZATIONS int,
    UNIQUE_CUSTOMERS int, QOLS_AVG double, MEMBER_MONTHS int
"""


# ===================================================== payer_transitions
# 37,700 rows, 8 cols. No Id -- composite key.
# Legitimately null: MEMBERID, PLAN_OWNERSHIP, OWNER_NAME (10,521);
# SECONDARY_PAYER (34,697 -- most people have one payer).
PAYER_TRANSITIONS = """
    PATIENT string, MEMBERID string, START_DATE timestamp, END_DATE timestamp,
    PAYER string, SECONDARY_PAYER string, PLAN_OWNERSHIP string, OWNER_NAME string
"""


# ============================================================ conditions
# 34,727 rows, 7 cols. No Id -- composite key.
# Legitimately null: STOP (8,857 -- ongoing conditions).
# CODE is SNOMED and runs to 17 digits, so it must be string, not int.
CONDITIONS = """
    START date, STOP date, PATIENT string, ENCOUNTER string, SYSTEM string,
    CODE string, DESCRIPTION string
"""


# =========================================================== medications
# 39,802 rows, 13 cols. No Id -- composite key.
# Legitimately null: STOP (2,540), REASONCODE/REASONDESCRIPTION (8,978).
# CODE and REASONCODE are codes -> string.
MEDICATIONS = """
    START timestamp, STOP timestamp, PATIENT string, PAYER string,
    ENCOUNTER string, CODE string, DESCRIPTION string,
    BASE_COST decimal(12,2), PAYER_COVERAGE decimal(12,2), DISPENSES int,
    TOTALCOST decimal(12,2), REASONCODE string, REASONDESCRIPTION string
"""


# ============================================================ encounters
# 57,592 rows, 15 cols. PK: Id.
# Legitimately null: REASONCODE/REASONDESCRIPTION (20,881).
ENCOUNTERS = """
    Id string, START timestamp, STOP timestamp, PATIENT string,
    ORGANIZATION string, PROVIDER string, PAYER string, ENCOUNTERCLASS string,
    CODE string, DESCRIPTION string, BASE_ENCOUNTER_COST decimal(12,2),
    TOTAL_CLAIM_COST decimal(12,2), PAYER_COVERAGE decimal(12,2),
    REASONCODE string, REASONDESCRIPTION string
"""


# ============================================================ procedures
# 151,636 rows, 10 cols. No Id -- composite key.
# Legitimately null: REASONCODE/REASONDESCRIPTION (77,642 -- most procedures
# have no coded reason).
PROCEDURES = """
    START timestamp, STOP timestamp, PATIENT string, ENCOUNTER string,
    SYSTEM string, CODE string, DESCRIPTION string, BASE_COST decimal(12,2),
    REASONCODE string, REASONDESCRIPTION string
"""


# ================================================================ claims
# 97,394 rows, 31 cols. PK: Id.
# DIAGNOSIS1-8 and CODE are diagnosis codes up to 17 digits -> string.
# OUTSTANDING1/2/P mix "0" and "0.00" in the source -> decimal, not string.
# Legitimately null: REFERRINGPROVIDERID (blank in ALL rows),
# SECONDARYPATIENTINSURANCEID (70,923), DIAGNOSIS2-8 (67,866+),
# STATUS2 / OUTSTANDING2 / LASTBILLEDDATE2 (26,473).
CLAIMS = """
    Id string, PATIENTID string, PROVIDERID string,
    PRIMARYPATIENTINSURANCEID string, SECONDARYPATIENTINSURANCEID string,
    DEPARTMENTID int, PATIENTDEPARTMENTID int,
    DIAGNOSIS1 string, DIAGNOSIS2 string, DIAGNOSIS3 string, DIAGNOSIS4 string,
    DIAGNOSIS5 string, DIAGNOSIS6 string, DIAGNOSIS7 string, DIAGNOSIS8 string,
    REFERRINGPROVIDERID string, APPOINTMENTID string,
    CURRENTILLNESSDATE timestamp, SERVICEDATE timestamp,
    SUPERVISINGPROVIDERID string, STATUS1 string, STATUS2 string, STATUSP string,
    OUTSTANDING1 decimal(12,2), OUTSTANDING2 decimal(12,2), OUTSTANDINGP decimal(12,2),
    LASTBILLEDDATE1 timestamp, LASTBILLEDDATE2 timestamp, LASTBILLEDDATEP timestamp,
    HEALTHCARECLAIMTYPEID1 int, HEALTHCARECLAIMTYPEID2 int
"""


# ================================================== claims_transactions
# 902,713 rows, 33 cols. PK: ID (uppercase, unlike every other table).
# PROCEDURECODE holds "03"-style values as well as SNOMED codes -> string.
# Legitimately null: MODIFIER1, MODIFIER2, LINENOTE (blank in ALL rows),
# TRANSFEROUTID (750,137), TRANSFERS (597,561), DIAGNOSISREF2-4, METHOD,
# TRANSFERTYPE, AMOUNT (all null exactly when the row is not that type).
CLAIMS_TRANSACTIONS = """
    ID string, CLAIMID string, CHARGEID int, PATIENTID string, TYPE string,
    AMOUNT decimal(12,2), METHOD string, FROMDATE timestamp, TODATE timestamp,
    PLACEOFSERVICE string, PROCEDURECODE string, MODIFIER1 string,
    MODIFIER2 string, DIAGNOSISREF1 int, DIAGNOSISREF2 int, DIAGNOSISREF3 int,
    DIAGNOSISREF4 int, UNITS int, DEPARTMENTID int, NOTES string,
    UNITAMOUNT decimal(12,2), TRANSFEROUTID int, TRANSFERTYPE string,
    PAYMENTS decimal(12,2), ADJUSTMENTS decimal(12,2), TRANSFERS decimal(12,2),
    OUTSTANDING decimal(12,2), APPOINTMENTID string, LINENOTE string,
    PATIENTINSURANCEID string, FEESCHEDULEID int, PROVIDERID string,
    SUPERVISINGPROVIDERID string
"""


TABLES: dict[str, TableSpec] = {
    "patients": TableSpec(schema=PATIENTS, primary_key=("Id",)),
    "providers": TableSpec(schema=PROVIDERS, primary_key=("Id",)),
    "organizations": TableSpec(schema=ORGANIZATIONS, primary_key=("Id",)),
    "payers": TableSpec(schema=PAYERS, primary_key=("Id",)),
    "payer_transitions": TableSpec(
        schema=PAYER_TRANSITIONS,
        primary_key=("PATIENT", "MEMBERID", "START_DATE", "PAYER"),
    ),
    "conditions": TableSpec(
        schema=CONDITIONS,
        primary_key=("PATIENT", "ENCOUNTER", "CODE", "START"),
    ),
    "medications": TableSpec(
        schema=MEDICATIONS,
        primary_key=("PATIENT", "ENCOUNTER", "CODE", "START"),
    ),
    "encounters": TableSpec(schema=ENCOUNTERS, primary_key=("Id",)),
    "procedures": TableSpec(
        schema=PROCEDURES,
        primary_key=("PATIENT", "ENCOUNTER", "CODE", "START"),
    ),
    "claims": TableSpec(schema=CLAIMS, primary_key=("Id",)),
    "claims_transactions": TableSpec(schema=CLAIMS_TRANSACTIONS, primary_key=("ID",)),
}
