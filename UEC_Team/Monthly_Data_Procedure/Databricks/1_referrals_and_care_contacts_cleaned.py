# Databricks notebook source
# COMMAND ----------
# ===========================================================
# Referrals + Care Contacts (year-windowed, FULL or UPDATE)
# - Year/date parameters (YEAR_START, YEAR_END)
# - Optional single-referral debug filter (REF_ID)
# - AttendOrDNACode, GP coalesce(GMPReg,GMPCodeReg)
# - Correct parentheses in filters
# - ContactTypeSubCategory present where used
# - Last_Reported_Month distinct
# - ICS mapping at the end, safe fallback
# ===========================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    col, lit, when, ltrim, rtrim, coalesce, to_date, row_number, datediff,
    current_date, year, month, concat, max as spark_max, add_months, broadcast, lag
)
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# ---------- Widgets / Params ----------
if 'dbutils' in globals():
    # Create widgets if missing
    try: dbutils.widgets.get('RUN_MODE')
    except: dbutils.widgets.text('RUN_MODE', 'full')   # 'full' or 'update'
    try: dbutils.widgets.get('YEAR_START')
    except: dbutils.widgets.text('YEAR_START', '2025-04-01')
    try: dbutils.widgets.get('YEAR_END')
    except: dbutils.widgets.text('YEAR_END', '2026-01-01')
    try: dbutils.widgets.get('REF_ID')
    except: dbutils.widgets.text('REF_ID', '')         # leave blank for all

    RUN_MODE    = dbutils.widgets.get('RUN_MODE').strip().lower()
    YEAR_START  = dbutils.widgets.get('YEAR_START').strip()
    YEAR_END    = dbutils.widgets.get('YEAR_END').strip()
    REF_ID      = dbutils.widgets.get('REF_ID').strip()
else:
    RUN_MODE   = 'full'
    YEAR_START = '2025-04-01'
    YEAR_END   = '2025-12-01'
    REF_ID     = ''  # e.g. 'RAT3397159Ref' to debug one referral

# ---------- Dynamic date tokens helper (inserted here) ----------
from datetime import date, timedelta

def iso(d):
    return d.strftime("%Y-%m-%d")

def first_of_month(d):
    return d.replace(day=1)

def fy_start_for(d):
    # UK FY starts Apr 1
    start_year = d.year - 1 if d.month <= 3 else d.year
    return date(start_year, 4, 1)

today = date.today()

token = (YEAR_START or "").strip().lower()

if token in ("fytd", "fy_to_date", "auto_fy"):
    YEAR_START = iso(fy_start_for(today))
    YEAR_END   = iso(today + timedelta(days=1))     # half-open window
elif token in ("last_8d", "rolling_8d"):
    YEAR_START = iso(today - timedelta(days=8))
    YEAR_END   = iso(today + timedelta(days=1))
elif token in ("prev_month", "last_month"):
    start_prev = (first_of_month(today) - timedelta(days=1)).replace(day=1)
    end_prev   = first_of_month(today)
    YEAR_START = iso(start_prev)
    YEAR_END   = iso(end_prev)
elif (YEAR_START or "").strip() == "" and (YEAR_END or "").strip() == "":
    # Default if nothing provided: FYTD
    YEAR_START = iso(fy_start_for(today))
    YEAR_END   = iso(today + timedelta(days=1))

# Guardrail
if YEAR_START >= YEAR_END:
    raise ValueError(f"Bad window: YEAR_START={YEAR_START} must be < YEAR_END={YEAR_END}")


if RUN_MODE not in ('full', 'update'):
    RUN_MODE = 'full'

# ---------- Spark configs ----------
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

# ---------- Paths ----------
mesh_base = "abfss://reporting@udalstdatacuratedprod.dfs.core.windows.net/"
ref_base  = "abfss://analytics-projects@udalstdataanalysisprod.dfs.core.windows.net/"

mesh_map = {
    "HospSpell":       f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHS501HospProvSpell_Published/",
    "CareContact":     f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHS201CareContact_Published/",
    "SubmissionFlags": f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHSDS_SubmissionFlags_Published/",
    "Referral":        f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHS101Referral_Published/",
    "ServiceType":     f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHS102ServiceTypeReferredTo_Published/",
    "TeamDetails":     f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHS902ServiceTeamDetails_Published/",
    "MPI":             f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHS001MPI_Published/",
    "GP":              f"{mesh_base}restricted/patientlevel/MESH/MHSDS/MHS002GP_Published/",
}

lookup_map = {
    "ConsultationMechanism":  f"{ref_base}PATLondon/MHUEC_Reference_Files/Care_Contact_Consultation_Mechanism/",
    "DateDim":                f"{ref_base}PATLondon/MHUEC_Reference_Files/Date_Dimension/",  # Delta
    "ProfGroup":              f"{ref_base}PATLondon/MHUEC_Reference_Files/Referring_Care_Professional_Staff_Group/",
    "SourceOfReferral":       f"{ref_base}PATLondon/MHUEC_Reference_Files/Source_Of_Referral_for_Mental_Health_Services/",
    "ReasonOAR":              f"{ref_base}PATLondon/MHUEC_Reference_Files/Reason_for_Out_Of_Area_Referral/",
    "ServiceTeamTypeLookup":  f"{ref_base}PATLondon/MHUEC_Reference_Files/Care_Contact_Service_or_Team_Type_Referred_to/",
    "PrimaryReasonReferral":  f"{ref_base}PATLondon/MHUEC_Reference_Files/Primary_Reason_For_Referral/",
}

# Target
core_data_base = f"{ref_base}PATLondon/MHSDS/Core_Tables/Core_Tables/"
core_map = { "ReferralsWithContacts": f"{core_data_base}MH_Referrals_with_Care_Contacts_London/" }

# Extra refs
date_full_path = (
    "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
    "reference/Internal/Reference/Date_Full/Published/1/"
)
ics_map_path = f"{ref_base}PATLondon/MHUEC_Reference_Files/ICS_Trust_Mapping/"

# ---------- 1) Load sources ----------
dfs = {}

# 1a) MESH
for name, path in mesh_map.items():
    df = spark.read.parquet(path)
    if REF_ID and name in ("Referral","CareContact","HospSpell","ServiceType"):
        df = df.filter(col("UniqServReqID") == lit(REF_ID))
    dfs[name] = df

# 1b) Lookups
for name, path in lookup_map.items():
    if name == "DateDim":
        dfs[name] = spark.read.format("delta").load(path)
    else:
        dfs[name] = spark.read.parquet(path)

# 1c) Core target (delta if present)
for name, path in core_map.items():
    try:
        dfs[name] = spark.read.format("delta").load(path)
    except Exception:
        dfs[name] = None

# 1d) Other refs
dfs["GPData"] = (
    spark.read.format("delta")
         .option("header", "true")
         .option("recursiveFileLookup", "true")
         .load(f"{ref_base}PATLondon/MHUEC_Reference_Files/GP_Data/")
)

dfs["CodeChanges"] = (
    spark.read.option("header", "true").option("recursiveFileLookup", "true")
         .parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
                  "reference/Internal/Reference/ComCodeChanges/Published/")
)
#display(dfs["CodeChanges"])
dfs["CommissionerHierarchies"] = (
    spark.read.option("header", "true").option("recursiveFileLookup", "true")
         .parquet("abfss://reporting@udalstdatacuratedprod.dfs.core.windows.net/"
                  "unrestricted/reference/UKHD/ODS/Commissioner_Hierarchies_ICB/")
)
#display(dfs["CommissionerHierarchies"])
dfs["OrgRef"] = (
    spark.read.option("header", "true").option("recursiveFileLookup", "true")
         .parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
                  "reference/UKHD/ODS_API/vwOrganisation_SCD_IsLatestEqualsOneWithRole/Published/1/")
).filter(col("Is_Latest") == 1)

#display(dfs["OrgRef"])

dfs["AllProviders"] = (
    spark.read.parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
                       "reference/UKHD/ODS/All_Providers_SCD/Published/1/")
).filter(col("Is_Latest") == 1)

#  display(dfs["AllProviders"])
#  display(dfs["OrgRef"])
# display(dfs["CommissionerHierarchies"])

#Practitioners
dfs["AllCodes"] = spark.read.parquet(
    "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
    "reference/UKHD/ODS/All_Codes/Published/1/"
)

#display(dfs["AllCodes"])


dfs["EthnicityLondon"] = (
    spark.read.option("header", "true").option("recursiveFileLookup", "true")
         .parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
                  "reference/UKHD/Data_Dictionary/Ethnic_Category_Code_SCD/Published/1/")
)

dfs["GenderCode"] = (
    spark.read.option("header", "true").option("recursiveFileLookup", "true")
         .parquet("abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
                  "reference/UKHD/Data_Dictionary/Gender_Identity_Code_SCD/Published/1/")
)

dfs["PostcodeToLA"] = spark.read.format("delta").load(
    f"{ref_base}PATLondon/MHUEC_Reference_Files/PostCode_to_LA/"
)

dfs["EthnicityPopLondon"] = (
    spark.read.option("header", "true").option("recursiveFileLookup", "true")
         .parquet(f"{ref_base}PATLondon/MHUEC_Reference_Files/Ethnicity_Population/London/")
)

dfs["DateFull"] = spark.read.parquet(date_full_path)

# ---------- 2) Helpers ----------
tempHOc = (
    dfs["HospSpell"]
    .select("UniqServReqID", "Der_Person_ID", "MHS501UniqID")
    .distinct()
)

ExistRef = (
    dfs["ReferralsWithContacts"].select("UniqServReqID").distinct()
    if dfs.get("ReferralsWithContacts") is not None
    else spark.createDataFrame([], "UniqServReqID string")
)

CareContact   = dfs["CareContact"]
SubmissionF   = dfs["SubmissionFlags"]
Referral      = dfs["Referral"]
ServiceType   = dfs["ServiceType"]
TeamDetails   = dfs["TeamDetails"]
DateDim       = dfs["DateDim"]
MPI           = dfs["MPI"]
GP            = dfs["GP"]
GPData        = dfs["GPData"]
CodeChanges   = dfs["CodeChanges"]
Commissioners = dfs["CommissionerHierarchies"]
OrgRef        = dfs["OrgRef"]
ProfGroup     = dfs["ProfGroup"]
SrcOfReferral = dfs["SourceOfReferral"]
ReasonOAR     = dfs["ReasonOAR"]

prov_list = ["RAT","RKL","RPG","RQY","RRP","RV3","RV5","RWK","TAF","RKE","G6V2S"]

sf_latest = (
    SubmissionF
    .select("NHSEUniqSubmissionID","Der_IsLatest")
    .filter(col("Der_IsLatest").isin("Y","1"))
)


from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, lit

cc_post = dfs["CareContact"].filter(to_date(col("CareContDate")) >= lit("2024-04-01"))

cc_post.groupBy(col("AttendOrDNACode").cast("string").alias("AttendOrDNACode")) \
       .count() \
       .orderBy(F.desc("count")) \
       .show(50, truncate=False)

cc_post.agg(
    F.sum(col("AttendOrDNACode").isNull().cast("int")).alias("AttendOrDNACode_null_rows"),
    F.count("*").alias("rows_post_2024_04_01")
).show(truncate=False)

# Date window
range_start = to_date(lit(YEAR_START))
range_end   = to_date(lit(YEAR_END))

# ---------- 3) CCPre ----------
# ---------- 3) CCPre ----------
CCPre = (
    CareContact.alias("cc")
      .join(sf_latest.alias("f"),
            col("f.NHSEUniqSubmissionID") == col("cc.NHSEUniqSubmissionID"), "left")
      .join(Referral.alias("g"),
            col("g.UniqServReqID") == col("cc.UniqServReqID"), "inner")
      .filter(
          (col("cc.AttendOrDNACode").isin("5","6")) &
          (
              col("cc.ConsMechanismMH").isin("01","02","04") |
              ((col("cc.UniqMonthID") < 1459) & (col("cc.ConsMechanismMH") == "03")) |
              ((col("cc.UniqMonthID") >= 1459) & (col("cc.ConsMechanismMH") == "11"))
          ) &
          (to_date(col("cc.CareContDate")) >= range_start) &
          (to_date(col("cc.CareContDate")) <  range_end) &
          (col("g.OrgIDProv").isNotNull() & col("g.OrgIDProv").isin(prov_list))
      )
      .select(col("cc.UniqServReqID").alias("UniqServReqID"))
      .distinct()
)


# If your actual column is CareContDate (not CcareContDate), swap both occurrences above:
# (to_date(col("cc.CareContDate")) >= range_start) & (to_date(col("cc.CareContDate")) < range_end)

# ---------- 4) tempUR_all + tempUR ----------
a = Referral.alias("a")
st = ServiceType.select(
        "RecordNumber","UniqServReqID","UniqSubmissionID","ServTeamTypeRefToMH","Effective_From"
     ).alias("st")

# TeamDetails de-dup -> rtd_1
from pyspark.sql.functions import row_number
keys = ["UniqCareProfTeamLocalID","UniqMonthID","NHSEUniqSubmissionID"]
rtd_keys = (
    a.select(*keys)
     .where(col(keys[0]).isNotNull() & col(keys[1]).isNotNull() & col(keys[2]).isNotNull())
     .distinct()
)
rtd_base = TeamDetails.join(rtd_keys, keys, "inner")
order_cols = []
if "Is_Latest" in rtd_base.columns:      order_cols.append(col("Is_Latest").desc_nulls_last())
if "Effective_From" in rtd_base.columns: order_cols.append(col("Effective_From").desc_nulls_last())
if "Load_Datetime" in rtd_base.columns:  order_cols.append(col("Load_Datetime").desc_nulls_last())
if not order_cols: order_cols = [col(keys[0]).asc()]

w_rtd = Window.partitionBy(*keys).orderBy(*order_cols)
rtd_1 = (
    rtd_base.withColumn("rn", row_number().over(w_rtd))
            .filter(col("rn")==1)
            .select(*keys, "serviceTypeName")
            .alias("rtd")
)

# DateDim robust column
date_cols = set(DateDim.columns)
date_col = "Calendar_Day" if "Calendar_Day" in date_cols else ("Date" if "Date" in date_cols else None)
dt = DateDim.alias("dt")
_on_dt = (to_date(col("a.ReferralRequestReceivedDate")) == col(f"dt.{date_col}")) if date_col else lit(False)

# MPI subset 'b'
mpi_cols = set(MPI.columns)
b_base_cols = [
    "Person_ID","UniqSubmissionID","UniqMonthID","RecordNumber",
    "Der_Pseudo_NHS_Number","LSOA2011","DefaultPostcode","Gender",
    "EmploymentNationalLatest","AccommodationNationalLatest",
    "EthnicCategory","NHSDEthnicity"
]
if "OrgIDSubICBLocResidence" in mpi_cols: b_base_cols.append("OrgIDSubICBLocResidence")
if "OrgIDCCGRes"            in mpi_cols: b_base_cols.append("OrgIDCCGRes")

b_keys = a.select("Person_ID","UniqSubmissionID","UniqMonthID","RecordNumber").distinct()

b = MPI.join(
        b_keys, ["Person_ID","UniqSubmissionID","UniqMonthID","RecordNumber"], "inner"
    ).select(*[col(c) for c in b_base_cols]).alias("b")

subicb_col = col("b.OrgIDSubICBLocResidence") if "OrgIDSubICBLocResidence" in b.columns else lit(None)
ccg_col    = col("b.OrgIDCCGRes")             if "OrgIDCCGRes"             in b.columns else lit(None)
org_key    = coalesce(subicb_col, ccg_col)


# tempUR_all
tempUR_all = (
    a.alias("a")
      .join(sf_latest.alias("sf"), col("sf.NHSEUniqSubmissionID") == col("a.NHSEUniqSubmissionID"), "left")
      .join(st, (col("st.RecordNumber") == col("a.RecordNumber")) & (col("st.UniqServReqID") == col("a.UniqServReqID")), "left")
      .join(broadcast(rtd_1), keys, "left")
      .join(dt, _on_dt, "left")
      .join(b, (col("b.Person_ID") == col("a.Person_ID")) &
               (col("b.UniqSubmissionID") == col("a.UniqSubmissionID")) &
               (col("b.UniqMonthID") == col("a.UniqMonthID")) &
               (col("b.RecordNumber") == col("a.RecordNumber")), "left")
      .join(GP.alias("gp"), (col("gp.RecordNumber") == col("b.RecordNumber")) &
                             (col("gp.UniqSubmissionID") == col("a.UniqSubmissionID")), "left")
      .join(GPData.alias("gpd"),
            col("gpd.Practice_Code") == coalesce(col("gp.GMPReg"), col("gp.GMPCodeReg")), "left")
      .join(CodeChanges.alias("cc"), col("cc.Org_Code") == org_key, "left")
      .join(Commissioners.alias("c"), coalesce(col("cc.New_Code"), org_key) == col("c.Organisation_Code"), "left")
      .join(tempHOc.alias("ho"), (col("ho.Der_Person_ID") == col("a.Der_Person_ID")) &
                                  (col("ho.UniqServReqID") == col("a.UniqServReqID")), "left")
      .join(OrgRef.alias("ORef"), col("ORef.ODS_Code") == col("a.OrgIDReferringOrg"), "left")
      .join(ProfGroup.alias("pg"), col("pg.Code") == col("a.ReferringCareProfessionalStaffGroup"), "left")
      .join(SrcOfReferral.alias("sor"), col("sor.Code") == col("a.SourceOfReferralMH"), "left")
      .join(ReasonOAR.alias("oop"), col("oop.Code").cast("string") == col("a.ReasonOAT").cast("string"), "left")
      .join(ExistRef.alias("er"), col("er.UniqServReqID") == col("a.UniqServReqID"), "left")
      .join(CCPre.alias("ccpre"), col("ccpre.UniqServReqID") == col("a.UniqServReqID"), "left")
      .filter(
          (
            col("er.UniqServReqID").isNull() &
            (col("a.OrgIDProv").isNotNull() & col("a.OrgIDProv").isin(prov_list)) &
            (to_date(col("a.ReferralRequestReceivedDate")) >= range_start) &
            (to_date(col("a.ReferralRequestReceivedDate")) <  range_end)
          ) |
          (col("ccpre.UniqServReqID").isNotNull())
      )
      .select(
          col("dt.Financial_Year").alias("Referral_Fin_Year"),
          col("dt.Month_Start_Date").alias("Referral_Month"),
          row_number().over(
              Window.partitionBy(col("a.UniqServReqID"), col("a.OrgIDProv"))
                    .orderBy(
                        col("a.UniqMonthID").desc(),
                        col("a.UniqSubmissionID").desc(),
                        col("st.UniqSubmissionID").desc(),
                        col("st.Effective_From").desc()
                    )
          ).alias("RowOrder"),
          col("a.UniqServReqID"),
          col("a.Der_Person_ID"),
          col("a.RecordNumber"),
          col("a.Person_ID"),
          col("a.UniqSubmissionID"),
          col("a.UniqMonthID"),
          col("a.FirstContactEverDate"),
          col("a.ReferralRequestReceivedDate"),
          col("a.ReferralRequestReceivedTime"),
          when(col("a.ReferRejectionDate").isNotNull(), lit(1)).alias("Referral_Rejected_Flag"),
          col("a.ReferRejectionDate"),
          col("a.ServDischDate"),
          col("b.Der_Pseudo_NHS_Number"),
          col("b.LSOA2011").alias("Patient_LSOA"),
          col("b.DefaultPostcode").alias("Patient_PostCode"),
          col("b.OrgIDSubICBLocResidence"),
          col("b.OrgIDCCGRes"),
          col("b.Gender"),
          col("b.EmploymentNationalLatest"),
          col("b.AccommodationNationalLatest"),
          col("b.EthnicCategory"),
          col("b.NHSDEthnicity"),
          col("a.AgeServReferRecDate"),
          when(col("ho.MHS501UniqID").isNotNull(), lit(1)).alias("Inpatient_Services_Flag"),
          when(col("c.Region_Code") == "Y56", lit("London_Patient")).otherwise(lit("Out_of_London_or_Not_Recorded")).alias("Patient_Region"),
          col("a.PrimReasonReferralMH"),
          when(ltrim(rtrim(col("st.ServTeamTypeRefToMH"))).isin("A05","A06","A08","A09","A12","A13","A16","C03","C10"), lit(1)).alias("Core_Community_Service_Team_Flag_OLD"),
          when(ltrim(rtrim(col("st.ServTeamTypeRefToMH"))) == "A06", lit(1)).alias("Core_Community_Service_Team_Flag"),
          col("gpd.Practice_Code").alias("ODS_GPPrac_OrgCode"),
          col("gpd.PCDS_NoGaps").alias("ODS_GPPrac_PostCode"),
          col("gpd.GP_Code").alias("MPI_GP_Code"),
          col("gpd.GP_Name").alias("Registered_GP_Practice_Name"),
          col("gpd.Local_Authority_Name").alias("GP_Local_Authority"),
          col("gpd.GP_Region_Name"),
          col("gpd.Lower_Super_Output_Area_Code").alias("GP_LSOA"),
          col("gpd.Longitude").alias("GP_Longitude"),
          col("gpd.Latitude").alias("GP_Latitude"),
          col("a.OrgIDReferringOrg").alias("OrgIDReferring"),
          col("ORef.Name").alias("Referring_Organisation"),
          col("ORef.role").alias("Referring_Org_Type"),
          col("rtd.serviceTypeName").alias("Type_of_Service_Referred_to"),
          col("a.SourceOfReferralMH"),
          col("oop.Description").alias("Reason_for_Out_of_Area_Referral"),
          col("sor.Description").alias("Source_of_Referral"),
          col("a.OrgIDProv"),
          col("pg.Description").alias("Referring_Care_Professional_Staff_Group"),
          when(col("a.SourceOfReferralMH") == "H1", lit("Emergency_Department"))
            .when(col("a.SourceOfReferralMH") == "H2", lit("Acute_Secondary_Care"))
            .when(col("a.SourceOfReferralMH").isin("A1","A2","A3","A4"), lit("Primary_Care"))
            .when(col("a.SourceOfReferralMH").isin("B1","B2"), lit("Self"))
            .when(col("a.SourceOfReferralMH").isin("E1","E2","E3","E4","E5","E6"), lit("Justice"))
            .when(col("a.SourceOfReferralMH").isin("F1","F2","F3","G1","G2","G3","G4","I1","I2","M1","M2","M3","M4","M5","M6","M7","C1","C2","C3","D1","D2","N3"), lit("Other"))
            .when(col("a.SourceOfReferralMH") == "P1", lit("Internal"))
            .otherwise(lit("Missing/Invalid")).alias("Source_of_Referral_Derived"),
          when(col("a.SourceOfReferralMH").isin("H1","H2"), col("Source_of_Referral_Derived")).otherwise(lit("Other")).alias("Source_of_Referral_Simplified"),
          when(col("a.ClinRespPriorityType") == "1", lit("Emergency"))
            .when(col("a.ClinRespPriorityType").isin("2","U"), lit("Urgent"))
            .when(col("a.ClinRespPriorityType") == "3", lit("Routine"))
            .when(col("a.ClinRespPriorityType") == "4", lit("Very_Urgent"))
            .otherwise(lit("Unknown")).alias("Clinical_Response_Priority_Type")
      )
)

tempUR = tempUR_all.filter(col("RowOrder")==1).cache()

# ---------- 5) CC (contacts) ----------
r_subset = (
    tempUR.select("UniqServReqID","Der_Person_ID","Type_of_Service_Referred_to","ReferralRequestReceivedDate","Clinical_Response_Priority_Type")
          .distinct()
          .filter(col("ReferralRequestReceivedDate").isNotNull())
)


st2    = ServiceType.alias("srf")
stt_lu = dfs["ServiceTeamTypeLookup"].alias("stt")
cm_lu  = dfs["ConsultationMechanism"].alias("cm")

CC_with_row2 = (
    CareContact.alias("cc")
      .join(sf_latest.alias("f"), col("f.NHSEUniqSubmissionID") == col("cc.NHSEUniqSubmissionID"), "left")
      .join(r_subset.alias("r"),
            (col("r.UniqServReqID") == col("cc.UniqServReqID")) &
            (col("r.Der_Person_ID") == col("cc.Der_Person_ID")) &
            (col("r.ReferralRequestReceivedDate") <= col("cc.CareContDate")),
            "inner")
      .join(st2, (col("srf.UniqServReqID") == col("cc.UniqServReqID")) &
                 (col("srf.UniqMonthID") == col("cc.UniqMonthID")) &
                 (col("srf.UniqSubmissionID") == col("cc.UniqSubmissionID")), "left")
      .join(stt_lu, col("stt.Code") == col("srf.ServTeamTypeRefToMH"), "left")
      .join(cm_lu, col("cm.Code") == col("cc.ConsMechanismMH"), "left")
      .filter(
          (col("f.Der_IsLatest").isin("Y","1")) &
          (col("cc.AttendOrDNACode").isin("5","6")) &
          (
            col("cc.ConsMechanismMH").isin("01","02","04") |
            ((col("cc.UniqMonthID") < 1459) & (col("cc.ConsMechanismMH") == "03")) |
            ((col("cc.UniqMonthID") >= 1459) & (col("cc.ConsMechanismMH") == "11"))
          )
      )
      .withColumn(
          "RowOrder",
          row_number().over(
              Window.partitionBy("cc.UniqServReqID","cc.CareContDate","cc.CareContTime","cc.ConsMechanismMH")
                    .orderBy(col("cc.UniqSubmissionID").desc())
          )
      )
      .select(
          col("RowOrder"),
          col("cc.UniqServReqID").alias("CC_UniqServReqID"),
          col("cc.RecordNumber"),
          col("cc.Der_Person_ID"),
          col("srf.ServTeamTypeRefToMH").alias("Service_Team_Type_Code"),
          coalesce(col("r.Type_of_Service_Referred_to"), col("stt.Description")).alias("Type_of_Service_Referred_to"),
          col("r.ReferralRequestReceivedDate").alias("ReferralRequestReceivedDate"),
          col("cc.CareContDate"),
          col("cc.CareContTime"),
          col("cc.UniqSubmissionID"),
          col("cc.MHS201UniqID"),
          col("cc.AttendOrDNACode"),
          col("cc.ConsMechanismMH"),
          col("r.Clinical_Response_Priority_Type"),
          when(col("cc.ConsMechanismMH").isin("01","02","04"), lit("face to face, telephone or talk type"))
            .when(((col("cc.UniqMonthID")<1459) & (col("cc.ConsMechanismMH")=="03")) |
                  ((col("cc.UniqMonthID")>=1459) & (col("cc.ConsMechanismMH")=="11")), lit("video"))
            .otherwise(lit(None)).alias("ContactTypeDesc"),
          col("cm.Description").alias("ContactTypeSubCategory"),
          when(
              (col("cc.AttendOrDNACode").isin("5","6")) &
              ( col("cc.ConsMechanismMH").isin("01","02","04","11") |
                ((col("cc.OrgIDProv")=="DFC") & col("cc.ConsMechanismMH").isin("05","09","10","13")) ),
              lit(1)
          ).alias("Der_Contact"),
          when(col("cc.AttendOrDNACode").isin("5","6") & col("cc.ConsMechanismMH").isin("01","02","04","11"), lit(1)).alias("Der_DirectContact"),
          when(col("cc.AttendOrDNACode").isin("5","6") & col("cc.ConsMechanismMH").isin("01","11"), lit(1)).alias("Der_FacetoFaceContact")
      )
)


CC = CC_with_row2.filter(col("RowOrder")==1).cache()

# ---------- 6) Enrich tempUR to 'd' ----------
ec_lu  = dfs["EthnicityLondon"].alias("ec").filter(col("ec.is_latest")==lit(1))
pm_lu  = dfs["PrimaryReasonReferral"].alias("pm")
gdf_lu = dfs["GenderCode"].alias("gdf").filter(col("gdf.is_latest")==lit(1))
pc_lu  = dfs["PostcodeToLA"].alias("pc")

last_month = (
    dfs["Referral"].groupBy("UniqServReqID").agg(spark_max("UniqMonthID").alias("Max_UniqMonthID"))
)

last_month_date = (
    last_month.alias("lm")
      .join(dfs["DateFull"].select(col("CMHT_MonthID").alias("k"), col("Month_Start_112")), col("lm.Max_UniqMonthID")==col("k"), "left")
      .select(
          col("lm.UniqServReqID"),
          to_date(col("Month_Start_112").cast("string"), "yyyyMMdd").alias("Last_Reported_Month")
      )
      .distinct()
)

d = (
    tempUR.alias("d")
      .join(ec_lu,  col("ec.Main_Code_Text")==col("d.EthnicCategory"), "left")
      .join(pm_lu,  col("pm.Code")==col("d.PrimReasonReferralMH"), "left")
      .join(gdf_lu, col("gdf.Main_Code_Text")==col("d.Gender"), "left")
      .join(pc_lu,  coalesce(col("pc.Postcode_3"), col("pc.Postcode_1"), col("pc.PCDS_NoGaps")).substr(1,4)
                   == coalesce(col("d.Patient_PostCode"), col("d.ODS_GPPrac_PostCode")).substr(1,4), "left")
      .join(last_month_date.alias("lm"), col("lm.UniqServReqID")==col("d.UniqServReqID"), "left")
      .select(
          col("d.*"),
          col("ec.Main_Description").alias("Ethnic_Category"),
          col("ec.Category").alias("Broad_Ethnic_Category"),
          col("gdf.Main_Description").alias("Gender_Description"),
          col("pm.Description").alias("Primary_Reason_For_Referral"),
          col("pc.Local_Authority_Name").alias("Patient_Postcode_Borough"),
          col("pc.Lower_Super_Output_Area_Code").alias("Patient_Postcode_LSOA"),
          when(col("ec.Category")=="Asian or Asian British", lit("Asian"))
            .when(col("ec.Category")=="Black or Black British", lit("Black"))
            .when(col("ec.Main_Description").isin("mixed","Any other ethnic group","White & Black Caribbean","Any other mixed background","Chinese"), lit("Mixed/Other"))
            .otherwise(col("ec.Category")).alias("Derived_Broad_Ethnic_Category"),
          col("lm.Last_Reported_Month")
      )
      .withColumn(
          "Discarded_Referral",
          when(
              col("ReferRejectionDate").isNull() &
              col("ServDischDate").isNull() &
              col("Last_Reported_Month").isNotNull() &
              (current_date() > add_months(col("Last_Reported_Month"), 6)),
              lit(1)
          ).otherwise(lit(None))
      )
)

# ---------- 7) Build RefCC ----------
Providers = dfs["AllProviders"]
epop     = dfs["EthnicityPopLondon"]

RefCC = (
    d.alias("d")
      .join(CC.alias("c"), col("c.CC_UniqServReqID")==col("d.UniqServReqID"), "left")
      .join(Providers.alias("pro"),
            (col("pro.Organisation_Code")==col("d.OrgIDProv")) & (col("pro.Is_Latest")==lit(1)),
            "left")
      .join(epop.alias("ep"),
            (col("ep.Borough")==col("d.Patient_Postcode_Borough")) &
            (col("ep.Broad_Ethnic_Category")==col("d.Derived_Broad_Ethnic_Category")),
            "left")
      .select(
          col("d.UniqServReqID"),
          col("d.Der_Person_ID"),
          col("c.MHS201UniqID"),
          col("d.Der_Pseudo_NHS_Number"),
          col("d.Patient_Postcode_LSOA"),
          col("d.Patient_LSOA").alias("Patient_LSOA_from_MPI"),
          col("d.Patient_PostCode"),

          col("d.Gender_Description").alias("Gender"),
          col("d.AgeServReferRecDate").alias("Age_at_Referral"),
          when((col("d.AgeServReferRecDate")<=18) & col("d.AgeServReferRecDate").isNotNull(), lit("CYP"))
            .when((col("d.AgeServReferRecDate")>18)  & col("d.AgeServReferRecDate").isNotNull(), lit("Adult"))
            .otherwise(lit(None)).alias("Age_Group"),
          col("d.Ethnic_Category"),
          col("d.Broad_Ethnic_Category"),
          col("d.Derived_Broad_Ethnic_Category"),
          ((lit(1) / when(col("ep.Value").cast("float") != 0, col("ep.Value").cast("float"))) * lit(100000)).alias("Ethnic_proportion_per_100000_of_London_Borough_2020"),
          col("d.EmploymentNationalLatest"),
          col("d.AccommodationNationalLatest"),
          coalesce(col("d.ODS_GPPrac_OrgCode"), col("d.MPI_GP_Code")).alias("Registered_GP_Practice_OrgCode"),
          col("d.Registered_GP_Practice_Name"),
          col("d.GP_Local_Authority"),
          col("d.OrgIDCCGRes").alias("OrgIDCCGRes"),
          col("d.GP_Region_Name"),
          col("d.ReferralRequestReceivedDate"),
          col("d.Primary_Reason_For_Referral"),
          col("d.SourceOfReferralMH"),
          col("d.Source_of_Referral"),
          col("d.Source_of_Referral_Derived"),
          col("d.Source_of_Referral_Simplified"),
          col("d.Core_Community_Service_Team_Flag_OLD"),
          col("d.Inpatient_Services_Flag"),
          col("d.Core_Community_Service_Team_Flag"),
          col("d.OrgIDProv"),
          col("pro.Organisation_Name").alias("Provider"),
          col("d.OrgIDReferring").alias("OrgIDReferring"),
          col("d.Referring_Organisation"),
          col("d.Referring_Org_Type"),
          col("d.Referring_Care_Professional_Staff_Group"),
          col("d.Reason_for_Out_of_Area_Referral"),
          col("d.FirstContactEverDate"),
          col("d.ReferralRequestReceivedTime"),
          col("c.CareContDate"),
          col("c.CareContTime"),
          col("d.ServDischDate"),
          col("d.Referral_Rejected_Flag"),
          col("d.ReferRejectionDate"),
          col("c.ContactTypeDesc"),
          col("c.ContactTypeSubCategory"),
          col("c.Der_Contact"),
          col("c.Der_DirectContact"),
          col("c.Der_FacetoFaceContact"),
          datediff(col("c.CareContDate"), col("d.ReferralRequestReceivedDate")).alias("Days_Between_Referral_and_Care_Contact"),
          datediff(coalesce(col("d.ReferRejectionDate"), col("d.ServDischDate"), to_date(lit("2099-12-31"))), col("d.ReferralRequestReceivedDate")).alias("Days_Between_Referral_and_Closure"),
          col("d.Type_of_Service_Referred_to"),
          col("d.Clinical_Response_Priority_Type"),
          col("d.Last_Reported_Month"),
          col("d.Discarded_Referral")
      )
)

# Ordering
RefCC = RefCC.withColumn(
    "Der_ContactOrder",
    row_number().over(
        Window.partitionBy("Der_Person_ID","UniqServReqID")
              .orderBy(col("CareContDate").asc(), col("CareContTime").asc(), col("MHS201UniqID").asc_nulls_last())
    )
)

F2FOrder = (
    RefCC.filter(col("Der_FacetoFaceContact").isNotNull())
         .select(col("UniqServReqID"), col("Der_ContactOrder").alias("orig_Der_ContactOrder"))
         .distinct()
         .withColumn("RowOrder",
             row_number().over(
                 Window.partitionBy("UniqServReqID").orderBy(col("orig_Der_ContactOrder"))
             )
         )
)

RefCC = (
    RefCC.alias("f")
         .join(F2FOrder.alias("g"),
               (col("g.UniqServReqID")==col("f.UniqServReqID")) &
               (col("g.orig_Der_ContactOrder")==col("f.Der_ContactOrder")),
               "left")
         .withColumn("Face_to_Face_Order", col("g.RowOrder"))
         .select("f.*", col("g.RowOrder").alias("Face_to_Face_Order"))
)

# Fallback names
RefCC = (
    RefCC.alias("g")
         .join(dfs["AllCodes"].alias("h"), col("h.code")==col("g.OrgIDProv"), "left")
         .withColumn("Provider", when(col("g.Provider").isNull(), col("h.Name")).otherwise(col("g.Provider")))
         .drop("h.Name")
)

RefCC = (
    RefCC.alias("g")
         .join(dfs["OrgRef"].alias("h"), col("h.ODS_code")==col("g.OrgIDReferring"), "left")
         .withColumn("Referring_Organisation", when(col("g.Referring_Organisation").isNull(), col("h.Name")).otherwise(col("g.Referring_Organisation")))
         .withColumn("Referring_Org_Type", when(col("g.Referring_Org_Type").isNull(), col("h.role")).otherwise(col("g.Referring_Org_Type")))
         .drop("h.Name","h.role")
)

RefCC = RefCC.withColumn("Der_ContactOrder", when(col("CareContDate").isNull(), lit(None)).otherwise(col("Der_ContactOrder")))

# ---------- 8) Final projection ----------
newRowsDF = (
    RefCC.select(
        lit(None).cast("long").alias("Overall_Order"),
        col("UniqServReqID"),
        col("Der_Person_ID"),
        col("Der_Pseudo_NHS_Number"),
        coalesce(col("Patient_Postcode_LSOA"), col("Patient_LSOA_from_MPI")).alias("Patient_LSOA"),
        col("Patient_PostCode"),
        col("Gender"),
        col("Age_at_Referral"),
        when((col("Age_at_Referral") <= 18) & col("Age_at_Referral").isNotNull(), lit("CYP"))
          .when((col("Age_at_Referral") > 18) & col("Age_at_Referral").isNotNull(), lit("Adult"))
          .otherwise(lit(None)).alias("Age_Group_at_Referral"),
        lit(None).cast("string").alias("Age_Category_at_Referral"),
        col("Ethnic_Category"),
        col("Broad_Ethnic_Category"),
        col("Derived_Broad_Ethnic_Category"),
        col("Ethnic_proportion_per_100000_of_London_Borough_2020"),
        col("EmploymentNationalLatest"),
        col("AccommodationNationalLatest"),
        coalesce(col("Registered_GP_Practice_OrgCode"), col("Registered_GP_Practice_OrgCode")).alias("Registered_GP_Practice_OrgCode"),
        col("Registered_GP_Practice_Name"),
        col("GP_Local_Authority").alias("Registered_GP_Local_Authority_Name"),
        col("OrgIDCCGRes"),
        col("GP_Region_Name").alias("Registered_GP_Region"),
        col("ReferralRequestReceivedDate"),
        col("ReferRejectionDate"),
        col("ServDischDate"),
        col("SourceOfReferralMH"),
        col("Source_of_Referral"),
        col("Source_of_Referral_Derived"),
        col("Source_of_Referral_Simplified"),
        col("Primary_Reason_For_Referral").alias("Primary_Reason_for_Referral"),
        col("Clinical_Response_Priority_Type"),
        col("Type_of_Service_Referred_to"),
        col("Referring_Organisation"),
        col("Referring_Org_Type"),
        col("Referring_Care_Professional_Staff_Group"),
        col("Reason_for_Out_of_Area_Referral"),
        col("FirstContactEverDate"),
        col("Provider").alias("Referred_to_Provider"),
        col("Core_Community_Service_Team_Flag").alias("Community_Mental_Health_Team_Flag"),
        # FIX: replace the non-existent Core_Community_Services_Flag with the correct column
        col("Core_Community_Service_Team_Flag").alias("Core_Community_Services_Flag"),
        col("Inpatient_Services_Flag").alias("Referral_linked_to_Inpatient_Spell_Flag"),
        col("CareContDate"),
        col("CareContTime"),
        col("ContactTypeDesc").alias("Contact_Type_Group"),
        col("ContactTypeSubCategory").alias("Contact_Type_Sub_Category"),
        col("Der_ContactOrder").alias("Care_Contact_Order"),
        col("Days_Between_Referral_and_Care_Contact"),
        lit(None).cast("int").alias("Days_since_previous_Care_Contact_for_this_referral"),
        col("Days_Between_Referral_and_Closure").alias("Days_Between_Referral_and_Date_Referral_Closed_or_Date_of_Last_Extract"),
        col("Der_Contact"),
        col("Der_DirectContact"),
        col("Der_FacetoFaceContact"),
        col("Face_to_Face_Order"),
        col("ReferralRequestReceivedTime"),
        lit(None).cast("long").alias("Der_Direct_Contact_Order"),
        col("Last_Reported_Month"),
        col("Discarded_Referral")
    )
)

# ---------- 9) ICS mapping at the end ----------
try:
    ics_map = spark.read.format("delta").load(ics_map_path)
    site_col = "Site" if "Site" in ics_map.columns else ("Provider" if "Provider" in ics_map.columns else None)
    if site_col and {"ICS","ICB"}.issubset(set(ics_map.columns)):
        map_df = (
            ics_map.select(
                col(site_col).alias("map_key"),
                col("ICS").cast("string").alias("Referred_to_MH_Trust_ICS_Full_Name"),
                col("ICB").cast("string").alias("Referred_to_MH_Trust_ICS_Abbrev")
            ).dropDuplicates(["map_key"])
        )
        join_key = "Referred_to_Provider" if "Referred_to_Provider" in newRowsDF.columns else "Provider"
        NewDF = (
            newRowsDF.alias("a")
            .join(broadcast(map_df).alias("m"), col(f"a.{join_key}") == col("m.map_key"), "left")
            .drop("map_key")
        )
    else:
        NewDF = (
            newRowsDF
            .withColumn("Referred_to_MH_Trust_ICS_Full_Name", lit(None).cast("string"))
            .withColumn("Referred_to_MH_Trust_ICS_Abbrev",     lit(None).cast("string"))
        )
except Exception:
    NewDF = (
        newRowsDF
        .withColumn("Referred_to_MH_Trust_ICS_Full_Name", lit(None).cast("string"))
        .withColumn("Referred_to_MH_Trust_ICS_Abbrev",     lit(None).cast("string"))
    )

# ---------- 10) Per-referral window derivations ----------
# drop pre-existing placeholders if any
for c in ["Overall_Order","Days_since_previous_Care_Contact_for_this_referral","Der_Direct_Contact_Order"]:
    if c in NewDF.columns:
        NewDF = NewDF.drop(c)

base = NewDF.repartition("UniqServReqID").persist()

w_referral_order = (
    Window.partitionBy("UniqServReqID")
          .orderBy(
              col("ReferralRequestReceivedDate").asc_nulls_last(),
              col("UniqServReqID"),
              coalesce(col("Care_Contact_Order"), lit(1))
          )
)

w_referral_by_ccdate = (
    Window.partitionBy("UniqServReqID").orderBy(col("CareContDate").asc_nulls_last())
)

NewDF = (base
    .withColumn("Overall_Order", row_number().over(w_referral_order).cast("long"))
    .withColumn("Days_since_previous_Care_Contact_for_this_referral",
                when(col("Care_Contact_Order").isNotNull() & (col("Care_Contact_Order") > 1),
                     datediff(col("CareContDate"), lag(col("CareContDate")).over(w_referral_by_ccdate)))
                .otherwise(lit(None).cast("int")))
    .withColumn("Der_Direct_Contact_Order",
                when(col("Der_DirectContact") == 1,
                     row_number().over(w_referral_by_ccdate))
                .otherwise(lit(None).cast("long")))
)

base.unpersist()

# ---------- 11) Write logic ----------
target_path = core_map["ReferralsWithContacts"]

if RUN_MODE == "update":
    try:
        DeltaTable.forPath(spark, target_path)
    except Exception:
        NewDF.limit(0).write.format("delta").mode("overwrite").save(target_path)

if RUN_MODE == "full":
    (
        NewDF
          .coalesce(32)
          .write.format("delta")
          .mode("overwrite")
          .option("overwriteSchema","true")
          .save(target_path)
    )
else:
    # delete/append for only IDs in this run window
    idsDF = tempUR.select("UniqServReqID").distinct()
    idsDF.createOrReplaceTempView("upd_ids")
    spark.sql(f"DELETE FROM delta.`{target_path}` WHERE UniqServReqID IN (SELECT UniqServReqID FROM upd_ids)")
    (
        NewDF
          .coalesce(16)
          .write.format("delta")
          .mode("append")
          .option("mergeSchema","true")
          .save(target_path)
    )

print("Done. Rows:", NewDF.count())

# COMMAND ----------
# =============================================================================
# Databricks-friendly FULL CareContact inspection AFTER a given date
# - Uses display() for readable wide output
# - Shows schema
# - Shows null-population report (as a table you can sort/filter)
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, lit

AFTER_DATE = "2024-04-01"

cc = dfs["CareContact"]

cc_after = (
    cc.filter(to_date(col("CareContDate")) >= lit(AFTER_DATE))
      .cache()
)

# 1) Sanity + max date
cc_after.agg(
    F.count("*").alias("rows_after_date"),
    F.max(to_date(col("CareContDate"))).alias("max_care_date")
).show(truncate=False)

# 2) Schema (quickly scan for any new/replacement fields)
cc_after.printSchema()

# 3) Browse sample rows (ALL columns) in Databricks UI
#    - You can increase the limit as needed.
display(
    cc_after.orderBy(col("CareContDate").desc_nulls_last(), col("CareContTime").desc_nulls_last())
            .limit(200)
)

# 4) Null-population report (find columns that are actually populated after this date)
total = cc_after.count()

null_exprs = [F.sum(col(c).isNull().cast("int")).alias(c) for c in cc_after.columns]
nulls_row = cc_after.agg(*null_exprs).collect()[0].asDict()

rows = []
for c in cc_after.columns:
    null_rows = int(nulls_row.get(c, 0))
    non_null = total - null_rows
    pct_non_null = (non_null / total) * 100 if total else None
    rows.append((c, non_null, null_rows, pct_non_null))

report = spark.createDataFrame(rows, ["column", "non_null_rows", "null_rows", "pct_non_null"]) \
              .orderBy(F.desc("pct_non_null"))

display(report)

# 5) Optional: spotlight likely "replacement" candidates for Attend/DNA
keywords = ["attend", "dna", "outcome", "status", "reason", "contact", "type", "seen", "appt", "appointment"]
candidates = [c for c in cc_after.columns if any(k in c.lower() for k in keywords)]

display(report.filter(col("column").isin(candidates)).orderBy(F.desc("pct_non_null")))

