# Databricks notebook source
# COMMAND ----------
# =========================
# MH ECDS - All ED Presentations (Delta)
# • FULL run: from 2020-01-01
# • UPDATE run: replace data for current Financial Year only (from 1 April)
# • Output Delta:
#   abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/
#   PATLondon/ECDS/Core_Tables/ECDS_All_Presentations_London/
# =========================

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, to_date, row_number, when, coalesce, current_date, year, month,
    concat, expr, split, trim, size, array, array_sort, array_distinct, ltrim, rtrim,
    datediff, max as spark_max
)
from pyspark.sql.types import FloatType, StringType, IntegerType, DateType
from delta.tables import DeltaTable
from datetime import date

spark = SparkSession.builder.getOrCreate()

# Conservative settings
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

# -------------------------
# Widgets / run mode
# -------------------------
if 'dbutils' in globals():
    dbutils.widgets.text("RUN_MODE", "full")  # "full" or "update"
    RUN_MODE = dbutils.widgets.get("RUN_MODE")
else:
    RUN_MODE = "full"

if RUN_MODE not in ("full","update"):
    RUN_MODE = "full"

# -------------------------
# Date boundaries
# -------------------------
today = current_date()
fy_year = when(month(today) >= 4, year(today)).otherwise(year(today) - 1)
fy_start_expr = to_date(concat(fy_year.cast("string"), lit("-04-01")))
full_start_expr = to_date(lit("2020-01-01"))
start_date_expr = when(lit(RUN_MODE) == lit("update"), fy_start_expr).otherwise(full_start_expr)

# -------------------------
# Paths (define ref_base!)
# -------------------------
ref_base  = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/"
cur_base  = "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
rest_base = "abfss://restricted@udalstdatacuratedprod.dfs.core.windows.net/"
rep_base  = "abfss://reporting@udalstdatacuratedprod.dfs.core.windows.net/"

presentations_path = (
    f"{ref_base}PATLondon/ECDS/Core_Tables/ECDS_All_Presentations_London/"
)

# -------------------------
# Read sources (best‑effort; tolerate missing)
# -------------------------
def _rd_parquet(p):
    try:
        return spark.read.option("header","true").option("recursiveFileLookup","true").parquet(p)
    except Exception:
        return None

def _rd_delta(p):
    try:
        return spark.read.format("delta").load(p)
    except Exception:
        return None

# ECDS core
ec_core = _rd_parquet(
    f"{rest_base}patientlevel/MESH/ECDS/EC_Core/Published/1/"
)
#display(ec_core)
# SNOMED (latest per code)
snomed_ref = _rd_parquet(
    f"{ref_base}PATLondon/ECDS/Reference_Files/SNOMED_Reference/"
)
if snomed_ref is not None:
    # Try to keep latest per SNOMED code
    if "Is_Latest" in snomed_ref.columns:
        snomed_ref = snomed_ref.filter(col("Is_Latest") == 1)
    # normalise expected columns
    # allow both lower/upper variants
    sc = [c for c in snomed_ref.columns]
    def _pick(cands):
        for c in cands:
            if c in sc: return c
        return None
    code_col = _pick(["SNOMED_Code","snomed_code","Code","code"])
    desc_col = _pick(["SNOMED_Description","snomed_description","Description","description","term","SNOMED_TERM"])
    if code_col and desc_col:
        snomed_ref = snomed_ref.select(
            col(code_col).alias("snomed_code"),
            col(desc_col).alias("snomed_description")
        ).dropDuplicates(["snomed_code"])
    else:
        snomed_ref = None

# Commissioner mapping & hierarchies
cc_changes = _rd_parquet(
    f"{cur_base}reference/Internal/Reference/ComCodeChanges/Published/"
)
o3 = _rd_parquet(
    f"{rep_base}unrestricted/reference/UKHD/ODS/Commissioner_Hierarchies_ICB/"
)

# Providers & sites (latest)
providers = _rd_parquet(
    f"{cur_base}reference/UKHD/ODS/All_Providers_SCD/Published/1/"
)
if providers is not None and "Is_Latest" in providers.columns:
    providers = providers.filter(col("Is_Latest")==1).select(
        col("Organisation_Code").alias("provider_code"),
        col("Organisation_Name").alias("provider_name"),
        col("Postcode").alias("provider_postcode")
    ).dropDuplicates(["provider_code"])

sites = _rd_parquet(
    f"{cur_base}reference/UKHD/ODS/NHS_Trust_Sites_Assets_And_Units_SCD/Published/1/"
)
if sites is not None and "Is_Latest" in sites.columns:
    sites = sites.filter(col("Is_Latest")==1).select(
        col("Organisation_Code").alias("site_code_of_treatment"),
        col("Organisation_Name").alias("site_name")
    ).dropDuplicates(["site_code_of_treatment"])

# GP & borough / ICS
gp_data = _rd_delta(
    f"{ref_base}PATLondon/MHUEC_Reference_Files/GP_Data/"
)
trust_boro = _rd_delta(
    f"{ref_base}PATLondon/MHUEC_Reference_Files/Trust_Borough_Mapping/"
)
postcode_la = _rd_delta(
    f"{ref_base}PATLondon/MHUEC_Reference_Files/PostCode_to_LA/"
)

# Ethnicity dictionary (latest)
eth_ref = _rd_parquet(
    f"{cur_base}reference/UKHD/Data_Dictionary/Ethnic_Category_Code_SCD/Published/1/"
)
if eth_ref is not None:
    if "Is_Latest" in eth_ref.columns:
        eth_ref = eth_ref.filter(col("Is_Latest")==1).select(
            col("Main_Code_Text").alias("ethnic_category_code"),
            col("Main_Description").alias("ethnic_main_description"),
            col("Category").alias("ethnic_category_group")
        )
    else:
        eth_ref = eth_ref.select(
            col("Main_Code_Text").alias("ethnic_category_code"),
            col("Main_Description").alias("ethnic_main_description"),
            col("Category").alias("ethnic_category_group")
        )

# Treatment function (tf_ref, keep latest)
tf_ref = _rd_parquet(
    f"{cur_base}reference/Internal/Reference/TreatmentFunctionDetails/Published/"
)
if tf_ref is not None:
    tf_keep = tf_ref.filter(col("Is_Latest")==1) if "Is_Latest" in tf_ref.columns else tf_ref
    tf_keep = tf_keep.select(
        col("Main_Code_Text").alias("tf_code"),
        col("Main_Description").alias("tf_description"),
        col("Category").alias("tf_category")
    )
else:
    tf_keep = None

# Comorbidities & referred-to
comorb = _rd_parquet(f"{ref_base}PATLondon/ECDS/Derived/MESH_ECDS_EC_Comorbidities/")
referred_to = _rd_parquet(f"{ref_base}PATLondon/ECDS/Derived/MESH_ECDS_EC_PatientReferredTo/")

# Date dim (for FY labelling if needed)
date_dim = _rd_delta(f"{ref_base}PATLondon/MHUEC_Reference_Files/Date_Dimension/")

# Inpatient spells (for last completed spell)
mh_spells = _rd_delta(f"{ref_base}PATLondon/MH_Spells/")

# -------------------------
# Filter to window & core exclusion logic (LONDON only)
# -------------------------
if ec_core is None:
    raise RuntimeError("EC_Core source not found. Please check curated path.")

a0 = (
    ec_core
      .filter(col("EC_Department_Type")=='01')
      .filter(to_date(col("Arrival_Date")) >= start_date_expr)
      .filter(to_date(col("Arrival_Date")) < current_date())
      .filter(col("Deleted") == 0)
)

# Join CCG code changes & hierarchies to restrict to London providers
if cc_changes is not None:
    a0 = a0.join(
        cc_changes.alias("cc"),
        col("Attendance_HES_CCG_From_Treatment_Site_Code")==col("cc.Org_Code"),
        "left"
    )
else:
    a0 = a0.withColumn("cc_New_Code", lit(None).cast(StringType()))
    a0 = a0.withColumn("cc_Org_Code", col("Attendance_HES_CCG_From_Treatment_Site_Code"))

if o3 is not None:
    a0 = a0.join(
        o3.alias("o3"),
        coalesce(col("cc.New_Code"), col("Attendance_HES_CCG_From_Treatment_Site_Code")) == col("o3.Organisation_Code"),
        "left"
    )
    a0 = a0.filter(coalesce(col("o3.Region_Name"), lit("Missing/Invalid")) == "LONDON")
else:
    # if hierarchies missing, keep all (but this would not be London-only)
    a0 = a0.withColumn("o3_Region_Name", lit(None).cast(StringType()))

# Exclusions (streamed/DOA + attendance category)
a0 = a0.filter(
    (col("EC_Discharge_Status_SNOMED_CT").isNull() |
     ~col("EC_Discharge_Status_SNOMED_CT").isin('1077031000000103','1077781000000101','63238001')) &
    (col("EC_AttendanceCategory").isNull() | col("EC_AttendanceCategory").isin('1','2','3'))
)

# De-dupe (as per SQL)
w = Window.partitionBy("Der_Pseudo_NHS_Number","Attendance_Unique_Identifier","Arrival_Date",
                       "Der_EC_Departure_Date_Time","Age_At_Arrival").orderBy("Arrival_Date")
a0 = a0.withColumn("roworder_ecds", row_number().over(w)).filter(col("roworder_ecds")==1)
display(providers)
# Provider/site names

if providers is not None:
    a0 = a0.alias("a0").join(
        providers.alias("o1"),
        col("a0.Provider_Code") == col("o1.Provider_Code"),
        "left"
    )
#SItes
if sites is not None:
    a0 = a0.alias("a0").join(
        sites.alias("st"),
        col("a0.Site_Code_of_Treatment") == col("st.site_code_of_treatment"),
        "left"
    )
# GP + borough/ICS
if gp_data is not None:
    a0 = a0.join(
        gp_data.alias("gp"),
        coalesce(col("PDS_General_Practice_Code"), col("GP_Practice_Code")) == col("gp.Practice_code"),
        "left"
    )


if trust_boro is not None:
    a0 = a0.join(
        trust_boro.alias("gpTm"),
        col("gpTm.Borough") == col("gp.Local_Authority_Name"),
        "left"
    )

# Provider postcode -> LA (approx)
if postcode_la is not None and providers is not None:
    # postcode without spaces on providers side
    a0 = a0.withColumn(
    "provider_postcode_nogaps",
    expr("regexp_replace(provider_postcode,' ','')")
    )

    a0 = a0.join(
        postcode_la.alias("la"),
        col("la.PCDS_NoGaps")==col("provider_postcode_nogaps"),
        "left"
    )

# Ethnicity (latest)
if eth_ref is not None:
    a0 = a0.join(
        eth_ref.alias("ec"),
        col("ec.ethnic_category_code")==col("Ethnic_Category"),
        "left"
    )

# -------------------------
# Primary diagnosis code (no spaces)
# -------------------------
b1 = a0.withColumn(
    "primary_diag_code",
    coalesce(
        expr("""
            substring(Der_EC_Diagnosis_All, 1,
                case when instr(Der_EC_Diagnosis_All, ',') > 0
                then instr(Der_EC_Diagnosis_All, ',') - 1
                else length(Der_EC_Diagnosis_All) end
            )
        """),
        col("Der_EC_Diagnosis_All")
    )
)

# SNOMED joins to fetch textual descriptions (arrival/attendance/injury/complaint/primary diag/discharge/followup/accommodation)
def snomed_join(df, lhs_col, alias_name):
    if snomed_ref is None:
        return df, None
    j = df.join(
        snomed_ref.select(
            col("snomed_code").alias(f"{alias_name}_code"),
            col("snomed_description").alias(f"{alias_name}_desc")
        ),
        trim(col(lhs_col)) == col(f"{alias_name}_code"),
        "left"
    ).drop(f"{alias_name}_code")
    return j, f"{alias_name}_desc"

b2, am_desc = snomed_join(b1, "EC_Arrival_Mode_SNOMED_CT", "am")
b3, ats_desc = snomed_join(b2, "EC_Attendance_Source_SNOMED_CT", "ats")
b4, ii_desc  = snomed_join(b3, "EC_Injury_Intent_SNOMED_CT",   "ii")
b5, cp_desc  = snomed_join(b4, "EC_Chief_Complaint_SNOMED_CT", "cp")
b6, pd_desc  = snomed_join(b5, "primary_diag_code",            "pd")
b6, dd_desc  = snomed_join(b6, "Discharge_Destination_SNOMED_CT", "dd")
b6, df_desc  = snomed_join(b6, "Discharge_Follow_Up_SNOMED_CT",   "df")
b6, ac_desc  = snomed_join(b6, "Accommodation_Status_SNOMED_CT",  "ac")

# Treatment function (tf_ref -> join into b6)
if tf_keep is not None:
    b6 = b6.join(
        tf_keep.alias("tf"),
        col("Decision_To_Admit_Treatment_Function_Code")==col("tf.tf_code"),
        "left"
    )

# -------------------------
# Flags & buckets
# -------------------------
mh_diag_set = [
    '52448006','2776000','33449004','72366004','197480006','35489007','13746004',
    '58214004','69322001','397923000','30077003','44376007','17226007','50705009'
]

b6 = (
    b6
    .withColumn("gender",
        when(col("Sex")=='0','Unknown').when(col("Sex")=='1','Male')
        .when(col("Sex")=='2','Female').when(col("Sex")=='9','Not specified')
    )
    .withColumn("age_group",
        when((col("Age_At_Arrival")<=18) & col("Age_At_Arrival").isNotNull(),'CYP')
        .when((col("Age_At_Arrival")>18) & col("Age_At_Arrival").isNotNull(),'Adult')
        .otherwise('Missing/Invalid')
    )
    .withColumn("agecat",
        when((col("Age_At_Arrival")>=0) & (col("Age_At_Arrival")<=11),'0-11')
        .when((col("Age_At_Arrival")>=12) & (col("Age_At_Arrival")<=17),'12-17')
        .when((col("Age_At_Arrival")>=18) & (col("Age_At_Arrival")<=25),'18-25')
        .when((col("Age_At_Arrival")>=26) & (col("Age_At_Arrival")<=64),'26-64')
        .when(col("Age_At_Arrival")>=65,'65+')
        .otherwise('Missing/Invalid')
    )
    .withColumn("time_grouper",
        when((col("EC_Departure_Time_Since_Arrival")>=0) & (col("EC_Departure_Time_Since_Arrival")<=240),'0-4')
        .when(col("EC_Departure_Time_Since_Arrival").isNull(),'0-4')
        .when((col("EC_Departure_Time_Since_Arrival")>240) & (col("EC_Departure_Time_Since_Arrival")<=720),'5-12')
        .when((col("EC_Departure_Time_Since_Arrival")>720) & (col("EC_Departure_Time_Since_Arrival")<=1440),'12-24')
        .when((col("EC_Departure_Time_Since_Arrival")>1440) & (col("EC_Departure_Time_Since_Arrival")<=2880),'24-48')
        .when((col("EC_Departure_Time_Since_Arrival")>2880) & (col("EC_Departure_Time_Since_Arrival")<=4320),'48-72')
        .when(col("EC_Departure_Time_Since_Arrival")>4320,'>72')
        .otherwise('Not recorded')
    )
    .withColumn("breach6hr",  when(col("EC_Departure_Time_Since_Arrival") > (60*6), lit(1)).otherwise(lit(0)))
    .withColumn("over_6hrs",  when(col("EC_Departure_Time_Since_Arrival") > (60*6), col("EC_Departure_Time_Since_Arrival") - (60*6)).otherwise(lit(0)))
    .withColumn("breach12hr", when(col("EC_Departure_Time_Since_Arrival") > (60*12), lit(1)).otherwise(lit(0)))
    .withColumn("over_12hrs", when(col("EC_Departure_Time_Since_Arrival") > (60*12), col("EC_Departure_Time_Since_Arrival") - (60*12)).otherwise(lit(0)))
    .withColumn("breach24hr", when(col("EC_Departure_Time_Since_Arrival") >= (24*60), lit(1)).otherwise(lit(0)))
    .withColumn("chief_complaint_flag",
        when(col("EC_Chief_Complaint_SNOMED_CT").isin('248062006','272022009','48694002','248020004','6471006','7011001','366979004'),1).otherwise(0)
    )
    .withColumn("injury_flag", when(col("EC_Injury_Date").isNotNull(),1).otherwise(0))
    .withColumn("injury_intent_flag", when(col("EC_Injury_Intent_SNOMED_CT")=='276853009',1).otherwise(0))
    .withColumn("diagnosis_flag", when(col("primary_diag_code").isin(mh_diag_set),1).otherwise(0))
    .withColumn("mental_health_presentation_flag",
        when(col("EC_Chief_Complaint_SNOMED_CT").isin('248062006','272022009','48694002','248020004','6471006','7011001','366979004'),1)
        .when(col("EC_Injury_Intent_SNOMED_CT")=='276853009',1)
        .when(col("primary_diag_code").isin(mh_diag_set),1)
        .otherwise(0)
    )
    .withColumn("self_harm_flag",
        when( (col("EC_Injury_Intent_SNOMED_CT")=='276853009') | (col("EC_Chief_Complaint_SNOMED_CT")=='248062006'), 1).otherwise(0)
    )
)
# ---- Safe helpers for column access ----
from pyspark.sql.functions import col, lit, when, coalesce as fcoalesce

def has(df, c: str) -> bool:
    return c in df.columns

def get(df, c: str, default=None):
    """Return a Column if it exists; otherwise a literal default (default=None)."""
    return col(c) if has(df, c) else lit(default)

# ----------------------------------------
# Build tempED from b6 (your current base)
# This avoids NPEs if some columns are missing or b6 has slightly different schema
# ----------------------------------------
if 'b6' not in globals() or b6 is None:
    raise RuntimeError("b6 is not defined — make sure all joins up to b6 completed successfully.")

tempED = (
    b6
    # Treatment function labels (present if you joined tf_ref into b6; else nulls)
    .withColumn("Treatment_Function_Desc", get(b6, "Treatment_Function_Description"))
    .withColumn("Treatment_Function_Group", get(b6, "Category"))
    .withColumnRenamed("Der_EC_Diagnosis_All", "mh_all_ed_snomed_diagnosis_codes")

    # Gender from Sex
    .withColumn(
        "Gender",
        when(get(b6, "Sex") == '0', 'Unknown')
        .when(get(b6, "Sex") == '1', 'Male')
        .when(get(b6, "Sex") == '2', 'Female')
        .when(get(b6, "Sex") == '9', 'Not specified')
        .otherwise(lit(None))
    )

    # Age group and categories
    .withColumn(
        "Age_Group",
        when((get(b6, "Age_At_Arrival") <= 18) & get(b6, "Age_At_Arrival").isNotNull(), 'CYP')
        .when((get(b6, "Age_At_Arrival") > 18) & get(b6, "Age_At_Arrival").isNotNull(), 'Adult')
        .otherwise('Missing/Invalid')
    )
    .withColumn(
        "AgeCat",
        when((get(b6, "Age_At_Arrival") >= 0) & (get(b6, "Age_At_Arrival") <= 11), '0-11')
        .when((get(b6, "Age_At_Arrival") >= 12) & (get(b6, "Age_At_Arrival") <= 17), '12-17')
        .when((get(b6, "Age_At_Arrival") >= 18) & (get(b6, "Age_At_Arrival") <= 25), '18-25')
        .when((get(b6, "Age_At_Arrival") >= 26) & (get(b6, "Age_At_Arrival") <= 64), '26-64')
        .when(get(b6, "Age_At_Arrival") >= 65, '65+')
        .otherwise('Missing/Invalid')
    )

    # Time_Grouper: guard with coalesce so null minutes behave like 0–4 hrs bucket
    .withColumn(
        "Time_Grouper",
        when((fcoalesce(get(b6, "EC_Departure_Time_Since_Arrival"), lit(0)) >= 0) &
             (fcoalesce(get(b6, "EC_Departure_Time_Since_Arrival"), lit(0)) <= 240), '0-4')
        .when(get(b6, "EC_Departure_Time_Since_Arrival").isNull(), '0-4')
        .when((get(b6, "EC_Departure_Time_Since_Arrival") > 240)  & (get(b6, "EC_Departure_Time_Since_Arrival") <= 720),  '5-12')
        .when((get(b6, "EC_Departure_Time_Since_Arrival") > 720)  & (get(b6, "EC_Departure_Time_Since_Arrival") <= 1440), '12-24')
        .when((get(b6, "EC_Departure_Time_Since_Arrival") > 1440) & (get(b6, "EC_Departure_Time_Since_Arrival") <= 2880), '24-48')
        .when((get(b6, "EC_Departure_Time_Since_Arrival") > 2880) & (get(b6, "EC_Departure_Time_Since_Arrival") <= 4320), '48-72')
        .when(get(b6, "EC_Departure_Time_Since_Arrival") > 4320, '>72')
        .otherwise('Not recorded')
    )

    # Breach helpers (optional, commonly used later)
    .withColumn("Breach6hr",  when(get(b6,"EC_Departure_Time_Since_Arrival") > (60*6),  lit(1)).otherwise(lit(0)))
    .withColumn("Time_over_6hrs",
               when(get(b6,"EC_Departure_Time_Since_Arrival") > (60*6),
                    get(b6,"EC_Departure_Time_Since_Arrival") - lit(60*6)).otherwise(lit(0)))
    .withColumn("Breach12hr", when(get(b6,"EC_Departure_Time_Since_Arrival") > (60*12), lit(1)).otherwise(lit(0)))
    .withColumn("Time_over_12hrs",
               when(get(b6,"EC_Departure_Time_Since_Arrival") > (60*12),
                    get(b6,"EC_Departure_Time_Since_Arrival") - lit(60*12)).otherwise(lit(0)))
    .withColumn("Breach24hr", when(get(b6,"EC_Departure_Time_Since_Arrival") >= (24*60), lit(1)).otherwise(lit(0)))
)
# -------------------------
# Clean diag split + SNOMED lookups + merge into tempED
# -------------------------
from pyspark.sql.functions import (
    col, lit, when, coalesce, split, trim, expr, concat_ws
)

# 0) Guard key columns used later
#    - ensure ec_ident exists (some feeds call it EC_Ident)
if "ec_ident" not in tempED.columns and "EC_Ident" in tempED.columns:
    tempED = tempED.withColumn("ec_ident", col("EC_Ident"))

#    - make unique_record_id (SQL logic)
if "unique_record_id" not in tempED.columns:
    tempED = tempED.withColumn(
        "unique_record_id",
        concat_ws("|",
            col("Generated_Record_ID"),
            col("Unique_CDS_identifier"),
            col("Attendance_Unique_Identifier"),
            col("ec_ident")
        )
    )

#    - ensure consolidated diagnosis list column exists with expected name
codes_col = "mh_all_ed_snomed_diagnosis_codes"
if codes_col not in tempED.columns:
    if "Der_EC_Diagnosis_All" in tempED.columns:
        tempED = tempED.withColumn(codes_col, col("Der_EC_Diagnosis_All"))
    else:
        raise ValueError(f"Expected '{codes_col}' (or Der_EC_Diagnosis_All) not found on tempED")

# 1) Build dfx with d1..d4 extracted from the concatenated list
dfx = (
    tempED
    .select("ec_ident", "unique_record_id", col(codes_col).alias("diag_all"))
    .withColumn("codes", split(coalesce(col("diag_all"), lit("")), ","))
    .withColumn("d1", trim(expr("element_at(codes, 1)")))
    .withColumn("d2", trim(expr("element_at(codes, 2)")))
    .withColumn("d3", trim(expr("element_at(codes, 3)")))
    .withColumn("d4", trim(expr("element_at(codes, 4)")))
)

# 2) Normalise SNOMED reference → (snomed_code, snomed_description)
def _normalise_snomed(df):
    if df is None:
        return None
    lc = {c.lower(): c for c in df.columns}
    code = lc.get("snomed_code") or lc.get("code")
    desc = lc.get("snomed_description") or lc.get("description") or lc.get("term") or lc.get("snomed_term")
    if not code or not desc:
        return None
    return df.select(
        col(code).alias("snomed_code"),
        col(desc).alias("snomed_description")
    ).dropDuplicates(["snomed_code"])

_snomed = _normalise_snomed(snomed_ref)

# 3) Attach descriptions for d1..d4
#    - if SNOMED not available, create null placeholders so downstream never breaks
if _snomed is not None:
    # primary (d1) description
    dfx = dfx.join(
        _snomed.select(col("snomed_code").alias("pd_code"),
                       col("snomed_description").alias("pd_desc")),
        trim(col("d1")) == col("pd_code"),
        "left"
    ).drop("pd_code")
    # secondary/third/fourth
    for i in (2, 3, 4):
        dfx = dfx.join(
            _snomed.select(col("snomed_code").alias(f"k{i}"),
                           col("snomed_description").alias(f"d{i}_desc")),
            trim(col(f"d{i}")) == col(f"k{i}"),
            "left"
        ).drop(f"k{i}")
else:
    dfx = (dfx
           .withColumn("pd_desc",  lit(None).cast("string"))
           .withColumn("d2_desc", lit(None).cast("string"))
           .withColumn("d3_desc", lit(None).cast("string"))
           .withColumn("d4_desc", lit(None).cast("string")))

# 4) Ensure target columns exist on tempED before we coalesce into them
seed_cols = {
    "primary_diag_code":            "string",
    "primary_diagnosis_desc":       "string",
    "secondary_diag_code":          "string",
    "secondary_diagnosis_desc":     "string",
    "third_diag_code":              "string",
    "third_diagnosis_desc":         "string",
    "fourth_diag_code":             "string",
    "fourth_diagnosis_desc":        "string",
    "reduction_in_inappropriate_flag": "int",
}
for c, typ in seed_cols.items():
    if c not in tempED.columns:
        tempED = tempED.withColumn(c, lit(None).cast(typ))

# 5) Merge back into tempED
tempED = (
    tempED.alias("t")
    .join(dfx.alias("d"), ["ec_ident", "unique_record_id"], "left")
    # keep existing primary if set; otherwise take d1/pd_desc
    .withColumn("primary_diag_code",       coalesce(col("t.primary_diag_code"),      col("d.d1")))
    .withColumn("primary_diagnosis_desc",  coalesce(col("t.primary_diagnosis_desc"), col("d.pd_desc")))
    # fill secondary/third/fourth (respect any existing values)
    .withColumn("secondary_diag_code",         coalesce(col("t.secondary_diag_code"),        col("d.d2")))
    .withColumn("secondary_diagnosis_desc",    coalesce(col("t.secondary_diagnosis_desc"),   col("d.d2_desc")))
    .withColumn("third_diag_code",             coalesce(col("t.third_diag_code"),            col("d.d3")))
    .withColumn("third_diagnosis_desc",        coalesce(col("t.third_diagnosis_desc"),       col("d.d3_desc")))
    .withColumn("fourth_diag_code",            coalesce(col("t.fourth_diag_code"),           col("d.d4")))
    .withColumn("fourth_diagnosis_desc",       coalesce(col("t.fourth_diagnosis_desc"),      col("d.d4_desc")))
    # reduction flag: diagnosis_flag==1 and no secondary code
    .withColumn(
        "reduction_in_inappropriate_flag",
        when( (col("diagnosis_flag")==1) & col("secondary_diag_code").isNull(), lit(1))
        .otherwise(col("reduction_in_inappropriate_flag"))
    )
)

# (Optional) if you prefer exactly the SQL-style names, you can also create aliases:
# tempED = (tempED
#   .withColumn("MH Primary SNOMED Diagnosis Code", col("primary_diag_code"))
#   .withColumn("MH Primary Diagnosis Description", col("primary_diagnosis_desc"))
#   .withColumn("Secondary Diagnosis Code", col("secondary_diag_code"))
#   .withColumn("Secondary Diagnosis Description", col("secondary_diagnosis_desc"))
#   .withColumn("Third Diagnosis Code", col("third_diag_code"))
#   .withColumn("Third Diagnosis Description", col("third_diagnosis_desc"))
#   .withColumn("Fourth Diagnosis Code", col("fourth_diag_code"))
#   .withColumn("Fourth Diagnosis Description", col("fourth_diagnosis_desc"))
# )

# reduction_in_inappropriate_flag
tempED = tempED.withColumn(
    "reduction_in_inappropriate_flag",
    when( (col("diagnosis_flag")==1) & col("secondary_diag_code").isNull(), lit(1)).otherwise(lit(None))
)

# -------------------------
# Comorbidities → first 4 SNOMED terms (if available)
# -------------------------
if comorb is not None and snomed_ref is not None:
    # collect all comorbidity_* columns that exist
    com_cols = [c for c in comorb.columns if c.lower().startswith("comorbidity_")]
    if com_cols:
        cdf = comorb.select("EC_Ident","Generated_Record_ID", *com_cols)
        arr = array(*[col(c) for c in com_cols])
        cdf = cdf.withColumn("cmb", array_distinct(array_sort(arr)))
        from pyspark.sql.functions import posexplode_outer
        ex = cdf.select("EC_Ident","Generated_Record_ID", posexplode_outer("cmb").alias("pos","code"))\
                .filter(col("code").isNotNull())
        ex = ex.join(
            snomed_ref.select(col("snomed_code").alias("code"), col("snomed_description").alias("desc")),
            "code","left"
        )
        lim = ex.where(col("pos") < 4).groupBy("EC_Ident").agg(
            spark_max(when(col("pos")==0, col("desc"))).alias("c1"),
            spark_max(when(col("pos")==1, col("desc"))).alias("c2"),
            spark_max(when(col("pos")==2, col("desc"))).alias("c3"),
            spark_max(when(col("pos")==3, col("desc"))).alias("c4")
        )
        tempED = (tempED.alias("t").join(lim.alias("cm"), "ec_ident", "left")
            .withColumn("comorbidity_01", coalesce(col("cm.c1"), col("comorbidity_01")))
            .withColumn("comorbidity_02", coalesce(col("cm.c2"), col("comorbidity_02")))
            .withColumn("comorbidity_03", coalesce(col("cm.c3"), col("comorbidity_03")))
            .withColumn("comorbidity_04", coalesce(col("cm.c4"), col("comorbidity_04")))
        )

# -------------------------
# Referred-to services up to 4 (if available)
# -------------------------
if referred_to is not None and snomed_ref is not None:
    # assume columns Referred_To_Service_01..04 + dates/times exist (adjust if names differ)
    rt = referred_to.select([c for c in referred_to.columns if "EC_Ident" in c or "Referred_To_Service" in c or "Service_" in c])
    # join to SNOMED for each slot
    for slot in ["01","02","03","04"]:
        code_col = f"Referred_To_Service_{slot}"
        if code_col in rt.columns:
            rt = rt.join(
                snomed_ref.select(col("snomed_code").alias(f"r{slot}_code"), col("snomed_description").alias(f"r{slot}_desc")),
                trim(col(code_col)) == col(f"r{slot}_code"),
                "left"
            ).drop(f"r{slot}_code")
    # collapse per EC_Ident (pick any non-null)
    agg = rt.groupBy("EC_Ident").agg(
        spark_max(col("r01_desc")).alias("s01"),
        spark_max(col("Service_Request_Date_01")).alias("d01"),
        spark_max(col("Service_Request_Time_01")).alias("t01"),
        spark_max(col("Service_Assessment_Date_01")).alias("ad01"),
        spark_max(col("Service_Assessment_Time_01")).alias("at01"),

        spark_max(col("r02_desc")).alias("s02"),
        spark_max(col("Service_Request_Date_02")).alias("d02"),
        spark_max(col("Service_Request_Time_02")).alias("t02"),
        spark_max(col("Service_Assessment_Date_02")).alias("ad02"),
        spark_max(col("Service_Assessment_Time_02")).alias("at02"),

        spark_max(col("r03_desc")).alias("s03"),
        spark_max(col("Service_Request_Date_03")).alias("d03"),
        spark_max(col("Service_Request_Time_03")).alias("t03"),
        spark_max(col("Service_Assessment_Date_03")).alias("ad03"),
        spark_max(col("Service_Assessment_Time_03")).alias("at03"),

        spark_max(col("r04_desc")).alias("s04"),
        spark_max(col("Service_Request_Date_04")).alias("d04"),
        spark_max(col("Service_Request_Time_04")).alias("t04"),
        spark_max(col("Service_Assessment_Date_04")).alias("ad04"),
        spark_max(col("Service_Assessment_Time_04")).alias("at04"),
    )
    tempED = (tempED.alias("t").join(agg.alias("r"), col("t.ec_ident")==col("r.EC_Ident"), "left")
        .withColumn("referred_to_service_01", coalesce(col("r.s01"), col("referred_to_service_01")))
        .withColumn("service_request_date_01", coalesce(col("r.d01"), col("service_request_date_01")))
        .withColumn("service_request_time_01", coalesce(col("r.t01"), col("service_request_time_01")))
        .withColumn("service_assessment_date_01", coalesce(col("r.ad01"), col("service_assessment_date_01")))
        .withColumn("service_assessment_time_01", coalesce(col("r.at01"), col("service_assessment_time_01")))
        .withColumn("referred_to_service_02", coalesce(col("r.s02"), col("referred_to_service_02")))
        .withColumn("service_request_date_02", coalesce(col("r.d02"), col("service_request_date_02")))
        .withColumn("service_request_time_02", coalesce(col("r.t02"), col("service_request_time_02")))
        .withColumn("service_assessment_date_02", coalesce(col("r.ad02"), col("service_assessment_date_02")))
        .withColumn("service_assessment_time_02", coalesce(col("r.at02"), col("service_assessment_time_02")))
        .withColumn("referred_to_service_03", coalesce(col("r.s03"), col("referred_to_service_03")))
        .withColumn("service_request_date_03", coalesce(col("r.d03"), col("service_request_date_03")))
        .withColumn("service_request_time_03", coalesce(col("r.t03"), col("service_request_time_03")))
        .withColumn("service_assessment_date_03", coalesce(col("r.ad03"), col("service_assessment_date_03")))
        .withColumn("service_assessment_time_03", coalesce(col("r.at03"), col("service_assessment_time_03")))
        .withColumn("referred_to_service_04", coalesce(col("r.s04"), col("referred_to_service_04")))
        .withColumn("service_request_date_04", coalesce(col("r.d04"), col("service_request_date_04")))
        .withColumn("service_request_time_04", coalesce(col("r.t04"), col("service_request_time_04")))
        .withColumn("service_assessment_date_04", coalesce(col("r.ad04"), col("service_assessment_date_04")))
        .withColumn("service_assessment_time_04", coalesce(col("r.at04"), col("service_assessment_time_04")))
    )
# -------------------------
# Last completed inpatient spell + deltas (safe)
# -------------------------
from pyspark.sql.functions import col, lit, datediff, to_date, max as spark_max

# Ensure tempED has arrival_date (lowercase) for joins
if "arrival_date" not in tempED.columns and "Arrival_Date" in tempED.columns:
    tempED = tempED.withColumn("arrival_date", to_date(col("Arrival_Date")))

# Ensure expected output columns exist so downstream never breaks
_out_cols = {
    "last_completed_ip_spell": "date",
    "ip_spell_provider_name":  "string",
    "uniqhospprovspellid":     "string",
    "ip_spell_uniqservreqid":  "string",
    "days_between_last_ip_and_ed": "int",
    "ed_within_28d_of_last_ip":    "int",
}
for c, t in _out_cols.items():
    if c not in tempED.columns:
        tempED = tempED.withColumn(c, lit(None).cast(t))

if mh_spells is not None:
    # Base spells: only completed (DischDateHospProvSpell not null)
    ho = (
        mh_spells
        .select(
            "UniqServReqID",
            "Der_Pseudo_NHS_Number",
            "UniqHospProvSpellID",
            "StartDateHospProvSpell",
            "DischDateHospProvSpell",
            "Provider_Name"
        )
        .filter(col("DischDateHospProvSpell").isNotNull())
        .dropDuplicates()
    )

    # Keys from tempED (dedup to shrink join)
    keys = (
        tempED
        .select("der_pseudo_nhs_number", "arrival_date")
        .dropDuplicates()
        .filter(col("der_pseudo_nhs_number").isNotNull() & col("arrival_date").isNotNull())
    )

    # Find the latest discharge date prior to the ED arrival for each (person, arrival_date)
    last_spell = (
        ho.alias("h")
        .join(keys.alias("x"),
              col("h.Der_Pseudo_NHS_Number") == col("x.der_pseudo_nhs_number"),
              "inner")
        .filter(col("h.DischDateHospProvSpell") < col("x.arrival_date"))
        .groupBy(col("x.der_pseudo_nhs_number").alias("k_nhs"),
                 col("x.arrival_date").alias("k_arrival"))
        .agg(spark_max(col("h.DischDateHospProvSpell")).alias("last_spell_date"))
    )

    # Attach the last completed spell date to tempED
    tempED = (
        tempED.alias("t")
        .join(
            last_spell.alias("ls"),
            (col("t.der_pseudo_nhs_number") == col("ls.k_nhs")) &
            (col("t.arrival_date") == col("ls.k_arrival")),
            "left"
        )
        .withColumn("last_completed_ip_spell", col("ls.last_spell_date"))
        .drop("k_nhs", "k_arrival", "last_spell_date")
    )

    # Bring spell keys/details for that last spell
    keys2 = (
        ho.select(
            col("Der_Pseudo_NHS_Number").alias("k_nhs"),
            col("DischDateHospProvSpell").alias("k_disch"),
            "Provider_Name",
            "UniqHospProvSpellID",
            "UniqServReqID"
        ).dropDuplicates()
    )

    tempED = (
        tempED.alias("t")
        .join(
            keys2.alias("k"),
            (col("t.der_pseudo_nhs_number") == col("k.k_nhs")) &
            (col("t.last_completed_ip_spell") == col("k.k_disch")),
            "left"
        )
        .withColumn("ip_spell_provider_name", col("k.Provider_Name"))
        .withColumn("uniqhospprovspellid", col("k.UniqHospProvSpellID"))
        .withColumn("ip_spell_uniqservreqid", col("k.UniqServReqID"))
        .withColumn(
            "days_between_last_ip_and_ed",
            when(col("t.last_completed_ip_spell").isNotNull(),
                 datediff(col("t.arrival_date"), col("t.last_completed_ip_spell")))
            .otherwise(lit(None))
        )
        .withColumn(
            "ed_within_28d_of_last_ip",
            when(col("days_between_last_ip_and_ed").isNotNull() &
                 (col("days_between_last_ip_and_ed") <= 28), lit(1))
            .otherwise(lit(None))
        )
        .drop("k_nhs", "k_disch")
    )
# else: do nothing — the placeholder columns are already present as nulls


# -------------------------
# Ethnicity population per 100k (London) – fill when null
# -------------------------
try:
    eth_pop = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(
        f"{ref_base}PATLondon/MHUEC_Reference_Files/Ethnicity_Population/London/"
    )
    tempED = (tempED.alias("y").join(eth_pop.alias("ep"),
        (col("ep.Broad_Ethnic_Category")==col("y.derived_broad_ethnic_category")) &
        (col("ep.Borough")==col("y.patient_gp_local_authority_name")),
        "left")
        .withColumn("ethnic_per_100k_london_borough_2020",
            coalesce(col("y.ethnic_per_100k_london_borough_2020"),
                     (lit(1.0)/when(col("ep.Value").cast("float")!=0, col("ep.Value").cast("float"))) * lit(100000.0)))
        .drop("ep.*")
    )
except Exception:
    pass

# -------------------------
# Known to MH services flags (6m / 24m / previously known)
# This section expects a pre-built table of referrals; if not present, skip silently.
# -------------------------
try:
    ed_referrals = _rd_delta(f"{ref_base}PATLondon/MH_ALL_ED_Referrals/")
    if ed_referrals is not None:
        # For performance, limit referrals to those <= arrival_date and within 24m horizon
        r = ed_referrals.select(
            "UniqServReqID","Der_Pseudo_NHS_Number","ReferralRequestReceivedDate",
            "Source of Referral","ServDischDate","ReferRejectionDate"
        ).withColumnRenamed("Source of Referral","source_of_referral")
        t = tempED.alias("x").join(
            r.alias("ccc"),
            (col("ccc.Der_Pseudo_NHS_Number")==col("x.der_pseudo_nhs_number")) &
            (col("ccc.ReferralRequestReceivedDate")<=col("x.arrival_date")),
            "left"
        )
        # 6 months window
        t = t.withColumn("known_to_mh_services_flag",
                when( (datediff(col("x.arrival_date"), col("ccc.ReferralRequestReceivedDate"))>=0) &
                      (datediff(col("x.arrival_date"), col("ccc.ReferralRequestReceivedDate"))<= (30*6)),
                      lit(1))
                .when( (datediff(col("x.arrival_date"), col("ccc.ReferralRequestReceivedDate"))> (30*6)) &
                       ( (col("ccc.ServDischDate").isNull()) | (col("ccc.ServDischDate")>col("x.arrival_date")) ) &
                       ( (col("ccc.ReferRejectionDate").isNull()) | (col("ccc.ReferRejectionDate")>col("x.arrival_date")) ),
                       lit(1))
                .otherwise(col("x.known_to_mh_services_flag"))
        )
        # If same-day and source = Acute Secondary Care: ECD, null out (match SQL)
        t = t.withColumn("known_to_mh_services_flag",
                when( (to_date(col("ccc.ReferralRequestReceivedDate"))==col("x.arrival_date")) &
                      (col("ccc.source_of_referral")=="Acute Secondary Care: Emergency Care Department"), lit(None).cast(StringType()))
                .otherwise(col("known_to_mh_services_flag"))
        )
        # 24 months
        t = t.withColumn("known_in_last_24months",
                when( (datediff(col("x.arrival_date"), col("ccc.ReferralRequestReceivedDate"))>=0) &
                      (datediff(col("x.arrival_date"), col("ccc.ReferralRequestReceivedDate"))<= (30*24)),
                      lit(1))
                .when( (datediff(col("x.arrival_date"), col("ccc.ReferralRequestReceivedDate"))> (30*24)) &
                       ( (col("ccc.ServDischDate").isNull()) | (col("ccc.ServDischDate")>col("x.arrival_date")) ) &
                       ( (col("ccc.ReferRejectionDate").isNull()) | (col("ccc.ReferRejectionDate")>col("x.arrival_date")) ),
                       lit(1))
                .otherwise(col("x.known_in_last_24months"))
        )
        # previously known (>24m)
        t = t.withColumn("previously_known",
                when( (datediff(col("x.arrival_date"), col("ccc.ReferralRequestReceivedDate"))> (30*24)), lit(1))
                .otherwise(col("x.previously_known"))
        )
        tempED = t.select("x.*","known_to_mh_services_flag","known_in_last_24months","previously_known")
except Exception:
    pass

# ========= De-duplicate & sanitize columns before Delta write =========
def deambiguate_columns(df):
    """
    1) Make *every* column name unique by position (toDF).
    2) Normalize names: lower, spaces -> underscores, strip risky chars.
    3) Collapse case-insensitive duplicates by keeping the first occurrence.
    """
    # 1) make unique by position
    seen = set()
    positional = []
    for i, c in enumerate(df.columns):
        name = c
        while name in seen:
            name = f"{c}__dup{i}"
        seen.add(name)
        positional.append(name)
    df1 = df.toDF(*positional)  # rename by position only (no ambiguity)

    # 2) normalize
    def norm(s: str) -> str:
        s = s.lower()
        for ch in [" ", "/", "\\", "-", "(", ")", "[", "]", ".", ","]:
            s = s.replace(ch, "_")
        while "__" in s:
            s = s.replace("__", "_")
        return s.strip("_")

    normalized = []
    used = set()
    for i, c in enumerate(df1.columns):
        base = norm(c)
        name = base
        k = 2
        while name in used:
            name = f"{base}_{k}"
            k += 1
        used.add(name)
        normalized.append(name)
    df2 = df1.toDF(*normalized)

    # 3) drop case-insensitive duplicates (keep first)
    lower_seen = set()
    keep = []
    for c in df2.columns:
        lc = c.lower()
        if lc in lower_seen:
            continue
        lower_seen.add(lc)
        keep.append(c)
    return df2.select(*keep)

# Apply just before writing
tempED_clean = deambiguate_columns(tempED)

# -------------------------
# Write / Merge logic (FULL vs UPDATE)  — uses tempED_clean
# -------------------------

# Ensure arrival_date exists after normalization (deambiguate_columns lowercases)
if "arrival_date" not in tempED_clean.columns and "Arrival_Date" in tempED_clean.columns:
    from pyspark.sql.functions import to_date, col
    tempED_clean = tempED_clean.withColumn("arrival_date", to_date(col("Arrival_Date")))

# If still missing, you likely need to map from your source column name:
# tempED_clean = tempED_clean.withColumn("arrival_date", to_date(col("<your source arrival date col>")))

# Create empty Delta at target if it doesn't exist
try:
    if not DeltaTable.isDeltaTable(spark, presentations_path):
        tempED_clean.limit(0).write.format("delta").mode("overwrite").save(presentations_path)
except Exception:
    pass

# Register external table (optional but handy)
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS delta.`{presentations_path}`
  USING DELTA
  LOCATION '{presentations_path}'
""")

if RUN_MODE.lower() == "full":
    (tempED_clean
        .repartition(64)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema","true")
        .save(presentations_path)
    )
else:
    # replace rows for current FY only
    from datetime import date
    fy_start_year = date.today().year if date.today().month >= 4 else date.today().year - 1
    fy_start_str  = f"{fy_start_year}-04-01"

    # Defensive: ensure arrival_date exists before the delete/append
    if "arrival_date" not in tempED_clean.columns:
        raise RuntimeError("arrival_date column is required for FY replace but was not found on tempED_clean.")

    spark.sql(f"DELETE FROM delta.`{presentations_path}` WHERE arrival_date >= date('{fy_start_str}')")

    (tempED_clean
        .repartition(64)
        .write
        .format("delta")
        .mode("append")
        .save(presentations_path)
    )

print("✅ ECDS_All_Presentations_London written to:", presentations_path)
tempED_clean.printSchema()

