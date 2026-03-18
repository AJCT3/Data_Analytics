# Databricks notebook source
# COMMAND ----------
"""
PySpark build of MH Act pipeline (path-based, same pattern as Spells script)
----------------------------------------------------------------------------
🎯 Output:
  - CORE_BASE/MH_Act/      (delta)
  - Updates CORE_BASE/MH_Spells/ with MHA link + Admission_Type + Linked_S136_Prior_to_Adm
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, to_date, coalesce
from delta.tables import DeltaTable
import re

# ─── 0. Setup ────────────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.databricks.io.cache.enabled", "false")
spark.conf.set("spark.sql.sources.fileListingCacheEnabled", "false")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

MESH_BASE = (
    "abfss://reporting@udalstdatacuratedprod.dfs.core.windows.net/"
    "restricted/patientlevel/MESH/MHSDS/"
)
REF_BASE = (
    "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/"
    "PATLondon/MHUEC_Reference_Files/"
)
CORE_BASE = (
    "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/"
    "PATLondon/MHSDS/Core_Tables/Core_Tables/"
)
LOOKUP_BASE = (
    "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/reference/UKHD/"
)
# OUTPUT TARGETS (path-based, like your Spells script)
target_mh_act  = f"{CORE_BASE}MH_Act/"
target_spells  = f"{CORE_BASE}MH_Spells/"

# Basic reader (same as Spells)
read_df = lambda p, f="parquet": spark.read.format(f).load(p)

# ─── helpers ────────────────────────────────────────────────────────────────
def sanitise_columns(df):
    """
    Replace invalid Delta characters [ ,;{}()\n\t=] with underscores in column names.
    """
    new_cols = []
    for c in df.columns:
        new_c = re.sub(r"[ ,;{}()\n\t=]", "_", c)
        new_c = re.sub(r"_+", "_", new_c).strip("_")
        new_cols.append(new_c)

    for old, new in zip(df.columns, new_cols):
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df

def pick_col(df, *names, default=None):
    """
    Returns a Column for the first column name that exists in df.
    If none exist, returns lit(default).
    """
    cols = set(df.columns)
    for n in names:
        if n in cols:
            return col(n)
    return lit(default)

def is_delta_path(path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False

def ensure_delta_columns(path: str, col_type_map: dict):
    """
    Ensure a delta dataset at `path` has the columns in col_type_map.
    Uses ALTER TABLE delta.`path` ADD COLUMNS to avoid overwrite.
    """
    df = read_df(path, "delta")
    existing = set(df.columns)

    missing = [(c, t) for c, t in col_type_map.items() if c not in existing]
    if not missing:
        return

    # One ALTER per missing column (keeps it simple/robust)
    for c, t in missing:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({c} {t})")

def make_ts(date_col, time_col, default_time="00:00:00"):
    """
    Timestamp from date+time columns (time can be null).
    """
    return F.to_timestamp(
        F.concat_ws(
            " ",
            col(date_col).cast("string"),
            F.coalesce(col(time_col).cast("string"), lit(default_time))
        )
    )

def best_row_per_key(df, key_cols, order_cols):
    """
    Keep row_number()==1 per key using ordering.
    """
    w = Window.partitionBy(*key_cols).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

# ─── 1. Sources (same style as spells: src dict) ─────────────────────────────
src = {
    # MESH feeds (Published)
    "ActPeriod":   read_df(f"{MESH_BASE}MHS401MHActPeriod_Published/"),
    "SubmissionF": read_df(f"{MESH_BASE}MHSDS_SubmissionFlags_Published/"),
    "MPI":         read_df(f"{MESH_BASE}MHS001MPI_Published/"),
    "GP":          read_df(f"{MESH_BASE}MHS002GP_Published/"),

    # These two variants exist in some environments; we load both if available.
    # If your environment only has one, comment out the other.
    "Referral1":   read_df(f"{MESH_BASE}MHS101Referral_Published/"),


    "CTO":         read_df(f"{MESH_BASE}MHS404CommTreatOrder_Published/"),
    "Recall":      read_df(f"{MESH_BASE}MHS405CommTreatOrderRecall_Published/"),
    "CondDis":     read_df(f"{MESH_BASE}MHS403ConditionalDischarge_Published/"),

    # Project layer (delta paths)
    "ReferralsCC": read_df(f"{CORE_BASE}MH_Referrals_with_Care_Contacts_London/", "delta"),
    "MonthRef":    read_df(f"{REF_BASE}Month_Reference/", "delta"),
    "GPDir":       read_df(f"{REF_BASE}GP_Data/", "delta"),
    "TrustICS":    read_df(f"{REF_BASE}ICS_Trust_Mapping/", "delta"),

    # Existing spells output (delta path)
    "Spells":      read_df(target_spells, "delta"),

    # Unified provider reference (same as your newer spells work)
    "DimProvider": (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(
            "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
            "reference/TCUBE/Reference/DimProvider/Published/1/"
        )
    ),
}
def read_df_recursive(path, fmt="parquet"):
    reader = spark.read.format(fmt)
    if fmt != "delta":
        reader = reader.option("recursiveFileLookup", "true")
    return reader.load(path)


lookup = {
    "ICD10": read_df_recursive(
        f"{LOOKUP_BASE}ICD10/Codes_And_Titles_And_MetaData/Published/1/"
    ),
    "Gender": read_df_recursive(
        f"{LOOKUP_BASE}Data_Dictionary/Gender_Identity_Code_SCD/Published/1/"
    ),
    "Ethnicity": read_df_recursive(
        f"{LOOKUP_BASE}Data_Dictionary/Ethnic_Category_Code_SCD/Published/1/"
    ),
    "ClassificationCode": read_df_recursive(
        f"{LOOKUP_BASE}Data_Dictionary/Mental_Health_Act_Legal_Status_Classification_Code_SCD/Published/1/"
    ),
}
# Dictionaries / lookups (left as metastore tables because they are stable and easier than guessing paths)
eth_dict = lookup["Ethnicity"].filter(col("is_latest") == 1)
gen_dict = lookup["Gender"].filter(col("is_latest") == 1)
mha_legal_ref = lookup["ClassificationCode"].filter(col("is_latest") == 1)
# Local reference tables (if you DO have these under REF_BASE, switch them to read_df)

eth_pop_ref    = (
    spark.read.option("header", "true").option("recursiveFileLookup", "true")
         .parquet(f"{REF_BASE}Ethnicity_Population/London/")
)

#display(mha_legal_ref)


# ─── 2. temp19 (Month spine) — matches your SQL #temp19 ─────────────────────
temp19 = (
    src["MonthRef"]
    .select(
        col("UniqMonthID"),
        col("FinYear_YYYY_YY"),
        to_date(col("MonthStartDate")).alias("MonthDate"),
    )
    .distinct()
)

# ─── 3. tempRef — matches your SQL #tempRef ─────────────────────────────────
#display(src["ReferralsCC"])

tempRef = (
    src["ReferralsCC"]
    .select(
        col("UniqServReqID"),
        col("Referring_Organisation"),
        col("Referring_Org_Type"),
        col("Referring_Care_Professional_Staff_Group"),
        col("Source_of_Referral_Derived").alias("Referral_Source"),
        col("Primary_Reason_for_Referral"),
        col("Clinical_Response_Priority_Type").alias("Clinical_Priority"),
    )
    .distinct()
)
#display(tempRef)
# SQL: @ENDRPDATE = max(ReferralRequestReceivedDate) from ReferralsCC
ENDRPDATE = src["ReferralsCC"].select(F.max(col("ReferralRequestReceivedDate")).alias("mx")).collect()[0]["mx"]

# ─── 4. Build tempMHA (base) — mirrors your SQL SELECT INTO #tempMHA ─────────
m = src["ActPeriod"].alias("m")
um = temp19.alias("um")
mp = src["MPI"].alias("mp")
gp = src["GP"].alias("gp")
gpd = src["GPDir"].alias("gpd")
r1 = src["Referral1"].alias("r1")

s  = src["SubmissionF"].alias("s")

#display(src["GP"])
# Exclusion joins (CTO/Recall/CondDis): distinct episode+month
cto = (
    src["CTO"]
    .groupBy("UniqMHActEpisodeID", "UniqMonthID")
    .agg(F.max(col("StartDateCommTreatOrd")).alias("StartDateCommTreatOrd"))
    .alias("cto")
)

recall = (
    src["Recall"]
    .groupBy("UniqMHActEpisodeID", "UniqMonthID")
    .agg(F.max(col("StartDateCommTreatOrdRecall")).alias("StartDateCommTreatOrdRecall"))
    .alias("rec")
)

cond_dis = (
    src["CondDis"]
    .groupBy("UniqMHActEpisodeID", "UniqMonthID")
    .agg(F.max(col("StartDateMHCondDisch")).alias("StartDateMHCondDisch"))
    .alias("cd")
)

# Provider mapping (DimProvider)
dp = src["DimProvider"].alias("dp")

dp_org_code = pick_col(src["DimProvider"], "OrgCode", "Organisation_Code", "provider_code", "Provider_Code")
dp_org_name = pick_col(src["DimProvider"], "OrgName", "Organisation_Name", "provider_name", "Provider_Name")
dp_postcode = pick_col(src["DimProvider"], "Postcode", "provider_postcode", "ProviderPostCode", "Provider_Postcode")

# Ethnicity broad bucket logic (same as your SQL)
derived_broad_eth = (
    when((col("ec.Main_Description").isNull()) | (col("ec.Main_Description") == "") |
         (col("ec.Main_Description").isin("Not stated", "Not known")),
         lit("Not Known / Not Stated / Incomplete"))
    .when(col("ec.Category") == lit("Asian or Asian British"), lit("Asian"))
    .when(col("ec.Category") == lit("Black or Black British"), lit("Black"))
    .when(col("ec.Main_Description").isin("mixed", "Any other ethnic group", "White & Black Caribbean",
                                         "Any other mixed background", "Chinese"), lit("Mixed/ Other"))
    .otherwise(col("ec.Category"))
)

base = (
    m
    .join(um, col("um.UniqMonthID") == col("m.UniqMonthID"), "inner")
    .join(mp, col("mp.RecordNumber") == col("m.RecordNumber"), "left")
    .join(gp, (col("gp.RecordNumber") == col("m.RecordNumber")) & (col("gp.UniqMonthID") == col("mp.UniqMonthID")), "left")
    .join(gpd, col("gpd.Practice_Code") == col("gp.GMPReg"), "left")
)
#display(base)
# The join above is a hacky pattern to avoid referencing missing columns in GPDir; do it properly:
# Rebuild base cleanly with explicit column exists checks:
gpdir_cols = set(src["GPDir"].columns)
# gpdir_key = "GP_Practice_Code" if "GP_Practice_Code" in gpdir_cols else ("GP_Practice_Code" if "GP_Practice_Code" in gpdir_cols else None)
#mha_legal_ref.display()


# Create gp_rownum per RecordNumber+UniqMonthID so we can use it in MostRecent ordering
gp_sub_id = F.coalesce(
    pick_col(src["GP"], "UniqSubmissionID", "NHSEUniqSubmissionID", "MHS002UniqID", default=None).cast("long"),
    F.lit(0).cast("long")
)

w_gp = (
    Window
    .partitionBy("RecordNumber", "UniqMonthID")
    .orderBy(gp_sub_id.asc_nulls_last())   # oldest->newest, so newest gets the largest row_number
)

gp_ranked = (
    src["GP"]
    .withColumn("gp_rownum", F.row_number().over(w_gp))
)


base = (
    m
    .join(um, col("um.UniqMonthID") == col("m.UniqMonthID"), "inner")
    .join(mp, col("mp.RecordNumber") == col("m.RecordNumber"), "left")
    .join(gp_ranked.alias("gp"),
      (col("gp.RecordNumber") == col("m.RecordNumber")) & (col("gp.UniqMonthID") == col("mp.UniqMonthID")),
      "left")

    .join(gpd, col("gpd.Practice_Code") == col("gp.GMPReg"), "left")
    .join(r1, (col("m.RecordNumber") == col("r1.RecordNumber")) & (col("m.Person_ID") == col("r1.Person_ID")), "left")
    .join(s, (col("m.NHSEUniqSubmissionID") == col("s.NHSEUniqSubmissionID")) & (col("s.Der_IsLatest") == lit("Y")), "inner")
    .join(cto, (col("cto.UniqMHActEpisodeID") == col("m.UniqMHActEpisodeID")) & (col("cto.UniqMonthID") == col("m.UniqMonthID")), "left")
    .join(recall, (col("rec.UniqMHActEpisodeID") == col("m.UniqMHActEpisodeID")) & (col("rec.UniqMonthID") == col("m.UniqMonthID")), "left")
    .join(cond_dis, (col("cd.UniqMHActEpisodeID") == col("m.UniqMHActEpisodeID")) & (col("cd.UniqMonthID") == col("m.UniqMonthID")), "left")
    .join(eth_dict.alias("ec"), col("ec.Main_Code_Text") == col("mp.NHSDEthnicity"), "left")
    .join(gen_dict.alias("gen"), col("gen.Main_Code_Text") == col("mp.Gender"), "left")
    .join(mha_legal_ref.alias("mha_ref"), col("mha_ref.Main_Code_Text") == col("m.NHSDLegalStatus"), "left")
    .join(dp, dp_org_code == col("m.OrgIDProv"), "left")
    .join(src["TrustICS"].alias("tm"), col("tm.Site") == dp_org_name, "left")
    .filter(col("um.MonthDate") >= lit("2024-04-01"))
    .filter(col("cto.UniqMHActEpisodeID").isNull())
    .filter(col("rec.UniqMHActEpisodeID").isNull())
    .filter(col("cd.UniqMHActEpisodeID").isNull())
)


#display(src["GPDir"])
tempMHA_pre = (
    base.select(
        col("m.Der_Person_ID").alias("Der_Person_ID"),
        col("m.Person_ID").alias("Person_ID"),
        col("mp.Der_Pseudo_NHS_Number").alias("Der_Pseudo_NHS_Number"),

        col("gpd.GP_Name").alias("GP_Practice_Name"),
        col("gpd.Local_Authority_Name").alias("GP_Local_Authority"),
        col("gpd.Practice_Code").alias("GP_Practice_Code"),
        col("gpd.GP_Region_Name").alias("Patient_GP_Practice_Region"),
        col("gpd.Lower_Super_Output_Area_Code").alias("GP_LSOA"),
        col("gp.gp_rownum").alias("gp_rownum"),
        col("ec.Main_Description").alias("Ethnic_Category"),
        derived_broad_eth.alias("Derived_Broad_Ethnic_Category"),
        col("gen.Main_Description").alias("Gender"),

        lit(None).cast("string").alias("Referring_Organisation"),
        lit(None).cast("string").alias("Referring_Org_Type"),
        lit(None).cast("string").alias("Referring_Care_Professional_Staff_Group"),
        lit(None).cast("string").alias("Referral_Source"),
        lit(None).cast("string").alias("Primary_Reason_for_Referral"),
        lit(None).cast("string").alias("Clinical_Priority"),

        lit(None).cast("double").alias("Ethnic_proportion_per_100000_of_London_Borough_2020"),

        col("m.RecordNumber").alias("RecordNumber"),
        col("m.UniqMonthID").alias("UniqMonthID"),
        col("um.MonthDate").alias("MonthDate"),
        col("um.FinYear_YYYY_YY").alias("FinYear_YYYY_YY"),

        col("s.ReportingPeriodStartDate").alias("ReportingPeriodStart"),
        col("s.ReportingPeriodEndDate").alias("ReportingPeriodEnd"),

        col("m.OrgIDProv").alias("OrgIDProv"),
        dp_org_name.alias("Provider_Name"),
        col("tm.ICB").alias("Provider_ICB"),
        dp_postcode.alias("ProviderPostCode"),
        F.regexp_replace(dp_postcode.cast("string"), " ", "").alias("ProviderPostCodeNoGaps"),

        col("m.UniqMHActEpisodeID").alias("UniqMHActEpisodeID"),
        col("m.NHSDLegalStatus").alias("SectionType"),
        col("mha_ref.Main_Description").alias("NHS_Legal_Status_Description"),

        col("m.StartDateMHActLegalStatusClass").alias("StartDate"),
        col("m.StartTimeMHActLegalStatusClass").alias("StartTime"),
        col("m.EndDateMHActLegalStatusClass").alias("EndDate"),
        col("m.EndTimeMHActLegalStatusClass").alias("EndTime"),

        col("m.MHS401UniqID").alias("UniqID"),

        lit(None).cast("int").alias("IP_Flag"),
        lit(None).cast("int").alias("IP_Spell_LOS"),
        lit(None).cast("string").alias("UniqHospProvSpellID"),

       col("r1.UniqServReqID").alias("UniqServReqID"),
    )
)

# ─── 5. Keep most recent flowed month per UniqMHActEpisodeID ─────────────────
w_most_recent = (
    Window
    .partitionBy(col("UniqMHActEpisodeID"))
    .orderBy(
        col("UniqMonthID").desc(),
        col("gp_rownum").desc_nulls_last(),
        col("UniqID").desc_nulls_last(),
    )
)


tempMHA_pre = tempMHA_pre.withColumn("MostRecentFlagSpells", F.row_number().over(w_most_recent))
tempMHA = tempMHA_pre.filter(col("MostRecentFlagSpells") == 1)

#tempMHA_pre.display()

#tempMHA_pre.select("UniqMHActEpisodeID", "UniqMonthID", "MostRecentFlagSpells").groupBy("MostRecentFlagSpells").count().show()


# ─── 6. Enrich referral fields from tempRef (NO duplicate columns) ───────────

def dedupe_colnames(df):

    seen = {}
    new_names = []
    for c in df.columns:
        if c not in seen:
            seen[c] = 0
            new_names.append(c)
        else:
            seen[c] += 1
            new_names.append(f"{c}__dup{seen[c]}")
    return df.toDF(*new_names)

# 6a) If tempMHA already has duplicate column names, fix them BEFORE aliasing


tempMHA = dedupe_colnames(tempMHA)
tempRef = dedupe_colnames(tempRef)

ref_overwrite = [
    "Referring_Organisation",
    "Referring_Org_Type",
    "Referring_Care_Professional_Staff_Group",
    "Referral_Source",
    "Primary_Reason_for_Referral",
    "Clinical_Priority",
]

# 6b) Select all base columns from tempMHA EXCEPT:
#     - any duplicate remnants (*__dup*)
#     - the referral fields we are overwriting
base_cols = []
seen = set()
for c in tempMHA.columns:
    if "__dup" in c:
        continue
    if c in ref_overwrite:
        continue
    if c not in seen:
        base_cols.append(c)
        seen.add(c)

f_cols = [col(f"f.{c}") for c in base_cols]

# 6c) Join to tempRef and add referral fields ONCE
tempMHA = (
    tempMHA.alias("f")
    .join(tempRef.alias("g"), col("g.UniqServReqID") == col("f.UniqServReqID"), "left")
    .select(
        *f_cols,
        col("g.Referring_Organisation").alias("Referring_Organisation"),
        col("g.Referring_Org_Type").alias("Referring_Org_Type"),
        col("g.Referring_Care_Professional_Staff_Group").alias("Referring_Care_Professional_Staff_Group"),
        col("g.Referral_Source").alias("Referral_Source"),
        col("g.Primary_Reason_for_Referral").alias("Primary_Reason_for_Referral"),
        col("g.Clinical_Priority").alias("Clinical_Priority"),
    )
)

# ─── 7. Link to MH_Spells (3-pass logic like SQL, DATE-ONLY) ─────────────────
spells_base = src["Spells"]  # should be the delta/df for PATLondon MH_Spells

# DATE-only boundaries (ignore all time fields completely)
mha_start_dt = col("f.StartDate").cast("date")

spell_start_dt = col("b.StartDateHospProvSpell").cast("date")

spell_end_dt = F.coalesce(
    col("b.DischDateHospProvSpell").cast("date"),
    lit(str(ENDRPDATE)).cast("date")
)

# SQL had DATEADD(HOUR,-24, start_ts) -> treat as "start date minus 1 day"
time_cond = (
    (mha_start_dt >= F.date_sub(spell_start_dt, 1)) &
    (mha_start_dt <= spell_end_dt)
)

def attach_best_spell(df_mha, join_cond, fill_servreq=True):
    # Fix any duplicate column names before aliasing
    df_mha = dedupe_colnames(df_mha)

    # Keep only one copy of each base column and ignore any *__dup* columns
    seen = set()
    base_cols = []
    for c in df_mha.columns:
        if "__dup" in c:
            continue
        if c not in seen:
            base_cols.append(c)
            seen.add(c)

    # These are overwritten and must appear ONLY ONCE in output
    overwrite = {"IP_Flag", "IP_Spell_LOS", "UniqHospProvSpellID", "UniqServReqID"}
    f_cols = [col(f"f.{c}") for c in base_cols if c not in overwrite]

    j = (
        df_mha.alias("f")
        .join(spells_base.alias("b"), join_cond & time_cond, "left")
        # scoring: closest spell start date to MHA start date
        .withColumn("f_start_dt", col("f.StartDate").cast("date"))
        .withColumn("b_start_dt", col("b.StartDateHospProvSpell").cast("date"))
        .withColumn("_abs_diff_days", F.abs(F.datediff(col("b_start_dt"), col("f_start_dt"))))
    )

    picked = best_row_per_key(
        j,
        key_cols=["f.UniqMHActEpisodeID"],
        order_cols=[col("_abs_diff_days").asc_nulls_last(), col("b.HOSP_LOS").desc_nulls_last()]
    )

    return (
        picked.select(
            *f_cols,
            when(col("b.UniqHospProvSpellID").isNotNull(), lit(1))
              .otherwise(col("f.IP_Flag")).alias("IP_Flag"),
            coalesce(col("b.HOSP_LOS"), col("f.IP_Spell_LOS")).alias("IP_Spell_LOS"),
            coalesce(col("b.UniqHospProvSpellID"), col("f.UniqHospProvSpellID")).alias("UniqHospProvSpellID"),
            (coalesce(col("b.UniqServReqID"), col("f.UniqServReqID")) if fill_servreq else col("f.UniqServReqID"))
              .alias("UniqServReqID"),
        )
        .drop("f_start_dt", "b_start_dt", "_abs_diff_days")
    )

# PASS 1: match on person key + RecordNumber
pass1 = attach_best_spell(
    tempMHA,
    join_cond=(
        (F.coalesce(col("b.Der_Person_ID"), col("b.Person_ID")) ==
         F.coalesce(col("f.Der_Person_ID"), col("f.Person_ID")))
        & (col("b.RecordNumber") == col("f.RecordNumber"))
    ),
    fill_servreq=True
)

# PASS 2: where still null, match on person key + Provider Name
pass2_input = pass1.filter(col("IP_Flag").isNull())
pass2_done = attach_best_spell(
    pass2_input,
    join_cond=(
        (F.coalesce(col("b.Der_Person_ID"), col("b.Person_ID")) ==
         F.coalesce(col("f.Der_Person_ID"), col("f.Person_ID")))
        & (col("b.Provider_Name") == col("f.Provider_Name"))
    ),
    fill_servreq=True
)
pass2 = pass1.filter(col("IP_Flag").isNotNull()).unionByName(pass2_done, allowMissingColumns=True)

# PASS 3: where still null, match on UniqServReqID
pass3_input = pass2.filter(col("IP_Flag").isNull())
pass3_done = attach_best_spell(
    pass3_input,
    join_cond=(col("b.UniqServReqID") == col("f.UniqServReqID")),
    fill_servreq=False
)
tempMHA = pass2.filter(col("IP_Flag").isNotNull()).unionByName(pass3_done, allowMissingColumns=True)

#eth_pop_ref.display()
# ─── 8. Ethnicity proportion per 100000 — matches your SQL update y ──────────
# display(tempMHA)
# display(eth_pop_ref)
tempMHA = (
    tempMHA.alias("y")
    .join(
        eth_pop_ref.alias("ep"),
        (col("ep.Broad_Ethnic_Category") == col("y.Derived_Broad_Ethnic_Category")) &
        (col("ep.Borough") == col("y.GP_Local_Authority")),
        "left"
    )
    .withColumn(
        "Ethnic_proportion_per_100000_of_London_Borough_2020",
        (lit(1.0) / F.nullif(col("ep.Value").cast("double"), lit(0.0))) * lit(100000.0)
    )
    .select("y.*", col("Ethnic_proportion_per_100000_of_London_Borough_2020"))
)
# display(tempMHA)
# ─── 9. OVERWRITE MH_Act (full rebuild every run) ────────────────────────────
tempMHA_out = sanitise_columns(tempMHA)

tempMHA_out = sanitise_columns(tempMHA).drop("gp_rownum")


(
    tempMHA_out
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target_mh_act)
)


# ─── 10. Update MH_Spells with MHA fields + Admission Type + Linked S136 ─────
# Ensure the columns exist on spells delta (no overwrite)
ensure_delta_columns(target_spells, {
    "UniqMHActEpisodeID": "STRING",
    "SectionType": "STRING",
    "NHS_Legal_Status_Description": "STRING",
    "Legal_Status_Start_Date": "DATE",
    "Legal_Status_Start_Time": "STRING",
    "Legal_Status_End_Date": "DATE",
    "Legal_Status_End_Time": "STRING",
    "Linked_S136_Prior_to_Adm": "INT",
    "Admission_Type": "STRING",
})

spells_df = read_df(target_spells, "delta")
mh_act_df = read_df(target_mh_act, "delta")

display(spells_df)
# Pick ONE “best” act episode per spell:
# - Prefer S2/S3 episodes if any exist on the spell
# - Else earliest StartDate
is_23 = col("NHS_Legal_Status_Description").isin(
    "Formally detained under Mental Health Act Section 2",
    "Formally detained under Mental Health Act Section 3"
).cast("int")

mha_for_spells = (
    mh_act_df
    .filter(col("UniqHospProvSpellID").isNotNull())
    .withColumn("_is_23", is_23)
    .withColumn("_start_ts", F.to_timestamp(F.concat_ws(" ", col("StartDate").cast("string"), F.coalesce(col("StartTime").cast("string"), lit("00:00:00")))))
)

mha_best = best_row_per_key(
    mha_for_spells,
    key_cols=["UniqHospProvSpellID"],
    order_cols=[col("_is_23").desc(), col("_start_ts").asc_nulls_last()]
).select(
    col("UniqHospProvSpellID"),
    col("UniqMHActEpisodeID"),
    col("SectionType"),
    col("NHS_Legal_Status_Description"),
    col("StartDate").alias("Legal_Status_Start_Date"),
    col("StartTime").alias("Legal_Status_Start_Time"),
    col("EndDate").alias("Legal_Status_End_Date"),
    col("EndTime").alias("Legal_Status_End_Time"),
)

# S136 flag if any S136 within 2 days prior to spell start
mha_s136 = (
    mh_act_df
    .filter(col("UniqHospProvSpellID").isNotNull())
    .filter(col("NHS_Legal_Status_Description") == lit("Formally detained under Mental Health Act Section 136"))
    .select(col("UniqHospProvSpellID"), col("StartDate").alias("S136StartDate"))
)

sp_upd = (
    spells_df.alias("sp")
    .join(mha_best.alias("m"), col("m.UniqHospProvSpellID") == col("sp.UniqHospProvSpellID"), "left")
    .join(mha_s136.alias("s136"), col("s136.UniqHospProvSpellID") == col("sp.UniqHospProvSpellID"), "left")
    .withColumn(
        "Linked_S136_Prior_to_Adm",
        when(
            (col("s136.S136StartDate").isNotNull()) &
            (col("s136.S136StartDate") <= col("sp.StartDateHospProvSpell")) &
            (F.datediff(col("sp.StartDateHospProvSpell"), col("s136.S136StartDate")) <= lit(2)),
            lit(1)
        ).otherwise(col("sp.Linked_S136_Prior_to_Adm"))
    )
    .withColumn(
        "Admission_Type",
        when(
            col("m.NHS_Legal_Status_Description").isin(
                "Formally detained under Mental Health Act Section 2",
                "Formally detained under Mental Health Act Section 3"
            ),
            lit("Formal")
        ).when(
            col("m.NHS_Legal_Status_Description").isNull(),
            lit("No Link to MHA")
        ).otherwise(lit("Informal"))
    )
    .select(
        col("sp.UniqHospProvSpellID").alias("UniqHospProvSpellID"),
        col("m.UniqMHActEpisodeID").alias("UniqMHActEpisodeID"),
        col("m.SectionType").alias("SectionType"),
        col("m.NHS_Legal_Status_Description").alias("NHS_Legal_Status_Description"),
        col("m.Legal_Status_Start_Date").alias("Legal_Status_Start_Date"),
        col("m.Legal_Status_Start_Time").alias("Legal_Status_Start_Time"),
        col("m.Legal_Status_End_Date").alias("Legal_Status_End_Date"),
        col("m.Legal_Status_End_Time").alias("Legal_Status_End_Time"),
        col("Linked_S136_Prior_to_Adm").alias("Linked_S136_Prior_to_Adm"),
        col("Admission_Type").alias("Admission_Type"),
    )
)
# Ensure ONE source row per spell for MERGE safety
w = Window.partitionBy("UniqHospProvSpellID").orderBy(
    col("Legal_Status_Start_Date").desc_nulls_last(),
    col("UniqMHActEpisodeID").desc_nulls_last()
)

sp_upd = (
    sp_upd
    .withColumn("_rn", F.row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

sp_dt = DeltaTable.forPath(spark, target_spells)
(sp_dt.alias("t")
 .merge(sp_upd.alias("s"), "t.UniqHospProvSpellID = s.UniqHospProvSpellID")
 .whenMatchedUpdate(set={
     "UniqMHActEpisodeID": col("s.UniqMHActEpisodeID"),
     "SectionType": col("s.SectionType"),
     "NHS_Legal_Status_Description": col("s.NHS_Legal_Status_Description"),
     "Legal_Status_Start_Date": col("s.Legal_Status_Start_Date"),
     "Legal_Status_Start_Time": col("s.Legal_Status_Start_Time"),
     "Legal_Status_End_Date": col("s.Legal_Status_End_Date"),
     "Legal_Status_End_Time": col("s.Legal_Status_End_Time"),
     "Linked_S136_Prior_to_Adm": col("s.Linked_S136_Prior_to_Adm"),
     "Admission_Type": col("s.Admission_Type"),
 })
 .execute())

# ─── 11. Quick checks ────────────────────────────────────────────────────────
# spark.read.format("delta").load(target_mh_act).selectExpr("max(StartDate) as max_StartDate").show()
# spark.read.format("delta").load(target_spells).groupBy("Admission_Type").count().show()

