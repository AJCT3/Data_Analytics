# Databricks notebook source
# COMMAND ----------
"""
PySpark translation of "2. Inpatient Spells and Ward Stays.sql"
----------------------------------------------------------------
🎯 Spells table (temp11) + Ward-at-admission (MH_Ward_Stays) + final MH_Spells
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    when,
    row_number,
    to_date,
    coalesce,
    datediff,
    last_day,
    lower,
    months_between,
)
import re

# ─── 0. Setup ────────────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

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

# OUTPUT TARGETS – adjust if you want different locations
target_temp11 = f"{CORE_BASE}MH_Spells_temp11/"  # staging version of temp11 (NOT written below)
target_ward = f"{CORE_BASE}MH_Ward_Stays/"       # ward staging
target_spells = f"{CORE_BASE}MH_Spells/"         # final enriched spells table


# ─── helper: sanitise column names for Delta ─────────────────────────────────
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


# Basic reader
read_df = lambda p, f="parquet": spark.read.format(f).load(p)

FIN_YEAR_START = to_date(lit("2018-01-01"))

# ─── 1. Sources & look-ups (your working version + DimProvider) ─────────────
src = {
    "HospSpell": read_df(f"{MESH_BASE}MHS501HospProvSpell_Published/"),
    "WardStay": read_df(f"{MESH_BASE}MHS502WardStay_Published/"),
    "SubmissionF": read_df(f"{MESH_BASE}MHSDS_SubmissionFlags_Published/"),
    "PrimDiag": read_df(f"{MESH_BASE}MHS604PrimDiag_Published/"),
    "SecDiag": read_df(f"{MESH_BASE}MHS605SecDiag_Published/"),
    "ProvDiag": read_df(f"{MESH_BASE}MHS603ProvDiag_Published/"),
    "MPI": read_df(f"{MESH_BASE}MHS001MPI_Published/"),
    "GP": read_df(f"{MESH_BASE}MHS002GP_Published/"),

    # feeds for ward / spell extras
    "PoliceReq": read_df(f"{MESH_BASE}MHS516PoliceAssistanceRequest_Published/"),
    "AWOL": read_df(f"{MESH_BASE}MHS511AbsenceWithoutLeave_Published/"),
    "LOA": read_df(f"{MESH_BASE}MHS510LeaveOfAbsence_Published/"),
    "HospSpellComm": read_df(f"{MESH_BASE}MHS512HospSpellComm_Published/"),

    # project layer (analysis account) – DO NOT CHANGE THESE PATHS
    "ReferralsCC": read_df(f"{CORE_BASE}MH_Referrals_with_Care_Contacts_London/", "delta"),
    "MonthRef": read_df(f"{REF_BASE}Month_Reference/", "delta"),
    "GPDir": read_df(f"{REF_BASE}GP_Data/", "delta"),
    "TrustICS": read_df(f"{REF_BASE}ICS_Trust_Mapping/", "delta"),
    "LSOAMap": read_df(f"{REF_BASE}LSOA_Map_2/"),
    "BoroughTrust": read_df(f"{REF_BASE}Trust_Borough_Mapping/", "delta"),
    "RefMethodAdm": read_df(f"{REF_BASE}Admission_Method/", "delta"),

    # NEW unified provider/site/region/type ref from TCUBE
    "DimProvider": (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(
            "abfss://unrestricted@udalstdatacuratedprod.dfs.core.windows.net/"
            "reference/TCUBE/Reference/DimProvider/Published/1/"
        )
    ),
}

# Recursive lookup reader for UKHD refs
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
    # ⛔ CommissionerHierarchies dropped – replaced by DimProvider
}

# ─── 2. tempSpellURef + tempRef ─────────────────────────────────────────────
tempSpellURef = src["HospSpell"].select("UniqServReqID").distinct().cache()

tempRef = (
    src["ReferralsCC"].alias("r")
    .join(tempSpellURef.alias("s"), "UniqServReqID")
    .select(
        "r.UniqServReqID",
        "r.ReferralRequestReceivedDate",
        "r.Referring_Organisation",
        "r.Referring_Org_Type",
        "r.Referring_Care_Professional_Staff_Group",
        col("r.Source_of_Referral").alias("Referral_Source"),
        "r.Primary_Reason_for_Referral",
        col("r.Clinical_Response_Priority_Type").alias("Clinical_Priority"),
        "r.Age_at_Referral",
    )
    .distinct()
)

# ─── 3. Diagnoses (latest per level) ────────────────────────────────────────
mh_icd10 = (
    lookup["ICD10"]
    .filter(
        (col("Chapter_Description") == "Mental and behavioural disorders")
        & col("Effective_To").isNull()
    )
    .select(
        col("Alt_Code").alias("ICD10"),
        col("Description").alias("Desc"),
        col("Category_2_Description").alias("Chapter"),
    )
)


def latest(df_key, level, code_col):
    w = (
        Window.partitionBy("UniqServReqID")
        .orderBy(col("UniqMonthID").desc(), col("UniqSubmissionID").desc())
    )
    return (
        src[df_key]
        .join(mh_icd10, col(code_col) == col("ICD10"))
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(
            "UniqServReqID",
            col(code_col).alias(f"{level}_Diag_Code"),
            col("Desc").alias(f"{level}_Diag_Desc"),
            col("Chapter").alias(f"{level}_Diag_Chapter"),
        )
    )


prim = latest("PrimDiag", "Prim", "PrimDiag")
sec = latest("SecDiag", "Sec", "SecDiag")
prov = latest("ProvDiag", "Prov", "ProvDiag")

# ─── 4. Last spell date, MonthRef serial ────────────────────────────────────
hosp = src["HospSpell"]

last_date_row = hosp.agg(F.max("StartDateHospProvSpell").alias("LastDate")).first()
last_date = last_date_row["LastDate"]

end_date_row = (
    hosp.agg(F.max(last_day("StartDateHospProvSpell")).alias("EndDate")).first()
)
end_date = end_date_row["EndDate"]

ref_other = src["MonthRef"]

date_serial_row = (
    ref_other.filter(F.col("MonthEndDate") == F.lit(end_date)).select("UniqMonthID").first()
)
date_serial = date_serial_row["UniqMonthID"]

# ─── 5. temp11 (spell-level staging) ────────────────────────────────────────
w_spell = (
    Window.partitionBy(col("a.UniqHospProvSpellID"))
    .orderBy(col("a.UniqMonthID").desc(), col("a.UniqSubmissionID").desc())
)

temp11 = (
    src["HospSpell"].alias("a")
    .join(
        src["SubmissionF"].alias("s"),
        (col("s.NHSEUniqSubmissionID") == col("a.NHSEUniqSubmissionID"))
        & (col("s.Der_IsLatest") == "Y"),
        "inner",
    )
    .join(
        prim.alias("prim"),
        col("a.UniqServReqID") == col("prim.UniqServReqID"),
        "left",
    )
    .join(
        sec.alias("sec"),
        col("a.UniqServReqID") == col("sec.UniqServReqID"),
        "left",
    )
    .join(
        prov.alias("prov"),
        col("a.UniqServReqID") == col("prov.UniqServReqID"),
        "left",
    )
    .join(
        src["MPI"].alias("m"),
        (col("m.Person_ID") == col("a.Person_ID"))
        & (col("m.UniqMonthID") == col("a.UniqMonthID")),
        "left",
    )
    .join(
        lookup["Gender"].alias("gen"),
        col("gen.Main_Code_Text") == col("m.Gender"),
        "left",
    )
    .join(
        lookup["Ethnicity"].alias("ec"),
        col("ec.Main_Code_Text") == col("m.EthnicCategory"),
        "left",
    )
    .join(
        src["GP"].alias("gp"),
        (col("gp.RecordNumber") == col("a.RecordNumber"))
        & (col("gp.UniqSubmissionID") == col("a.UniqSubmissionID")),
        "left",
    )
    .join(
        src["GPDir"].alias("gpd"),
        col("gpd.Practice_Code") == col("gp.GMPCodeReg"),
        "left",
    )
    # postcode → LA → Trust/ICS mapping
    .join(
        src["LSOAMap"].alias("la"),
        col("la.LSOA11CD") == col("m.LSOA2011"),
        "left",
    )
    .join(
        src["BoroughTrust"].alias("h"),
        col("h.Borough") == col("la.LAD17NM"),
        "left",
    )
    .withColumn("RowOrder", row_number().over(w_spell))
    .withColumn(
        "AdmissionCat",
        when(col("a.DischDateHospProvSpell").isNotNull(), lit("Closed"))
        .when(
            col("a.DischDateHospProvSpell").isNull()
            & (col("a.UniqMonthID") >= lit(date_serial)),
            lit("Open"),
        )
        .when(
            col("a.DischDateHospProvSpell").isNull()
            & (col("a.UniqMonthID") < lit(date_serial)),
            lit("Inactive"),
        ),
    )
    .withColumn(
        "HOSP_LOS",
        when(
            col("a.DischDateHospProvSpell").isNotNull(),
            datediff(
                col("a.DischDateHospProvSpell").cast("date"),
                col("a.StartDateHospProvSpell").cast("date"),
            )
            + 1,
        ),
    )
    .withColumn(
        "HOSP_LOS_NEW",
        when(
            col("a.DischDateHospProvSpell").isNotNull(),
            datediff(
                col("a.DischDateHospProvSpell").cast("date"),
                col("a.StartDateHospProvSpell").cast("date"),
            ),
        ),
    )
    .withColumn(
        "HOSP_LOS_at_Last_Update_for_Incomplete_Spells",
        when(
            col("a.DischDateHospProvSpell").isNull(),
            datediff(
                coalesce(col("a.DischDateHospProvSpell").cast("date"), lit(end_date)),
                col("a.StartDateHospProvSpell").cast("date"),
            )
            + 1,
        ),
    )
    .filter(col("RowOrder") == 1)
    .select(
        col("a.UniqHospProvSpellID"),
        col("a.UniqServReqID"),
        col("a.UniqSubmissionID"),
        col("a.Person_ID"),
        col("a.RecordNumber"),

        col("gen.Main_Description").alias("Gender"),
        when(col("ec.Category") == "Asian or Asian British", lit("Asian"))
        .when(col("ec.Category") == "Black or Black British", lit("Black"))
        .when(
            col("ec.Main_Description").isin(
                "mixed",
                "Any other ethnic group",
                "White & Black Caribbean",
                "Any other mixed background",
                "Chinese",
            ),
            lit("Mixed/Other"),
        )
        .otherwise(col("ec.Category"))
        .alias("Derived_Broad_Ethnic_Category"),

        col("gpd.GP_Name"),
        col("gpd.GP_Region_Name"),
        col("gpd.Local_Authority_Name").alias("GP_Borough"),
        col("gpd.Lower_Super_Output_Area_Name").alias("GP_LSOA"),
        col("gpd.Longitude").alias("GP_Longitude"),
        col("gpd.Latitude").alias("GP_Latitude"),

        when(
            (col("m.AgeRepPeriodStart") >= 0) & (col("m.AgeRepPeriodStart") <= 11),
            "0-11",
        )
        .when(
            (col("m.AgeRepPeriodStart") >= 12) & (col("m.AgeRepPeriodStart") <= 17),
            "12-17",
        )
        .when(
            (col("m.AgeRepPeriodStart") >= 18) & (col("m.AgeRepPeriodStart") <= 25),
            "18-25",
        )
        .when(
            (col("m.AgeRepPeriodStart") >= 26) & (col("m.AgeRepPeriodStart") <= 64),
            "26-64",
        )
        .when(col("m.AgeRepPeriodStart") >= 65, "65+")
        .otherwise("Missing/Invalid")
        .alias("AgeBand"),

        col("a.OrgIDProv"),
        col("a.UniqMonthID"),
        col("a.StartDateHospProvSpell"),
        col("a.StartTimeHospProvSpell"),
        col("a.DischDateHospProvSpell"),
        col("a.DischTimeHospProvSpell"),
        col("a.EstimatedDischDateHospProvSpell"),
        col("a.PlannedDischDateHospProvSpell"),

        # keep the raw source code column and an aliased version for downstream logic
        col("a.SourceAdmMHHospProvSpell").alias("SourceAdmCodeHospProvSpell"),
        col("a.MethAdmMHHospProvSpell"),

        # NEW geography columns
        col("m.LSOA2011").alias("LSOA2011"),
        col("la.LAD17NM").alias("LAName"),
        col("h.Trust").alias("Res_MH_Trust_by_PatPostcode"),
        col("h.ICS").alias("ICB_of_Res_MH_Trust_by_PatPostcode"),
        col("h.Borough").alias("Borough_Res_MH_Trust_by_PatPostcode"),

        # Diagnoses
        col("prim.Prim_Diag_Code").alias("Primary_ICD10_code"),
        col("prim.Prim_Diag_Desc").alias("Primary_diagnosis_description"),
        col("prim.Prim_Diag_Chapter").alias("Primary_diagnosis_ICD10_chapter"),
        col("sec.Sec_Diag_Code").alias("Secondary_ICD10_code"),
        col("sec.Sec_Diag_Desc").alias("Secondary_diagnosis_description"),
        col("sec.Sec_Diag_Chapter").alias("Secondary_diagnosis_ICD10_chapter"),
        col("prov.Prov_Diag_Code").alias("Provisional_ICD10_code"),
        col("prov.Prov_Diag_Desc").alias("Provisional_diagnosis_description"),
        col("prov.Prov_Diag_Chapter").alias("Provisional_diagnosis_ICD10_chapter"),

        col("AdmissionCat"),
        col("HOSP_LOS"),
        col("HOSP_LOS_NEW"),
        col("HOSP_LOS_at_Last_Update_for_Incomplete_Spells"),
    )
)

# ─────────────────────────────────────────────────────────────────────────────
# 6. WARD STAY LOGIC – REPLICATION OF #WardAtAdmission
# ─────────────────────────────────────────────────────────────────────────────
ward_base = (
    src["WardStay"].alias("a")
    .join(
        src["SubmissionF"].alias("s"),
        (col("s.NHSEUniqSubmissionID") == col("a.NHSEUniqSubmissionID"))
        & (col("s.Der_IsLatest") == "Y"),
        "inner",
    )
    .join(
        temp11.alias("b"),
        (col("b.UniqHospProvSpellID") == col("a.UniqHospProvSpellID"))
        & (col("b.RecordNumber") == col("a.RecordNumber")),
        "inner",
    )
    .join(
        src["PoliceReq"].alias("par"),
        (col("par.UniqWardStayID") == col("a.UniqWardStayID"))
        & (col("par.UniqHospProvSpellID") == col("a.UniqHospProvSpellID"))
        & (col("par.UniqServReqID") == col("a.UniqServReqID")),
        "left",
    )
    .join(
        src["AWOL"].alias("awl"),
        (col("awl.UniqWardStayID") == col("a.UniqWardStayID"))
        & (col("awl.UniqHospProvSpellID") == col("a.UniqHospProvSpellID"))
        & (col("awl.UniqServReqID") == col("a.UniqServReqID")),
        "left",
    )
    .join(
        src["LOA"].alias("loa"),
        (col("loa.UniqWardStayID") == col("a.UniqWardStayID"))
        & (col("loa.UniqHospProvSpellID") == col("a.UniqHospProvSpellID"))
        & (col("loa.UniqServReqID") == col("a.UniqServReqID")),
        "left",
    )
    .withColumn(
        "MonthRowOrder",
        row_number().over(
            Window.partitionBy(col("a.UniqHospProvSpellID"), col("a.UniqWardStayID")).orderBy(
                col("a.UniqMonthID").asc(), col("a.UniqSubmissionID").asc()
            )
        ),
    )
    .select(
        col("a.RecordNumber"),
        col("a.Der_Person_ID"),
        col("a.Person_ID"),
        lit(None).cast("int").alias("WardStayOrder"),
        col("MonthRowOrder"),
        col("a.UniqMonthID"),
        lit(None).cast("int").alias("Last_Submission"),
        col("a.UniqHospProvSpellID"),
        col("a.UniqWardStayID"),
        col("a.UniqSubmissionID"),
        # AdmissionTypeNHSE
        when(
            col("a.MHAdmittedPatientClass").isin("10", "200", "11", "201", "12", "202"),
            lit("Adult Acute (CCG commissioned)"),
        )
        .when(
            col("a.MHAdmittedPatientClass").isin(
                "13",
                "203",
                "14",
                "204",
                "15",
                "16",
                "17",
                "18",
                "19",
                "20",
                "21",
                "22",
                "205",
                "206",
                "207",
                "208",
                "209",
                "210",
                "211",
                "212",
                "213",
            ),
            lit("Adult Specialist"),
        )
        .when(
            col("a.MHAdmittedPatientClass").isin(
                "23",
                "24",
                "25",
                "26",
                "27",
                "28",
                "29",
                "30",
                "31",
                "32",
                "33",
                "34",
                "300",
                "301",
                "302",
                "303",
                "304",
                "305",
                "306",
                "307",
                "307",
                "308",
                "309",
                "310",
                "311",
            ),
            lit("CYP"),
        )
        .otherwise(lit("Missing/Invalid"))
        .alias("AdmissionTypeNHSE"),
        # AdmissionType_MHUEC
        when(col("a.MHAdmittedPatientClass").isin("10"), lit("Adult acute"))
        .when(col("a.MHAdmittedPatientClass").isin("11"), lit("Older adult acute"))
        .when(
            col("a.MHAdmittedPatientClass").isin(
                "12",
                "13",
                "14",
                "15",
                "17",
                "19",
                "20",
                "21",
                "22",
                "35",
                "36",
                "37",
                "38",
                "39",
                "40",
            ),
            lit("Adult specialist"),
        )
        .when(col("a.MHAdmittedPatientClass").isin("23", "24"), lit("CYP acute"))
        .when(
            col("a.MHAdmittedPatientClass").isin(
                "25", "26", "27", "28", "29", "30", "31", "32", "33", "34"
            ),
            lit("CYP specialist"),
        )
        .otherwise(lit("Unknown"))
        .alias("AdmissionType_MHUEC"),
        col("a.HospitalBedTypeName"),
        col("b.UniqServReqID"),
        coalesce(col("a.SpecialisedMHServiceCode"), lit("Non Specialised Service")).alias(
            "SpecialisedMHServiceCode"
        ),
        col("a.OrgIDProv"),
        col("a.SiteIDOfWard").alias("SiteIDOfTreat"),
        col("a.WardType"),
        col("a.WardIntendedSex").alias("WardSexTypeCode"),
        col("a.WardCode"),
        col("a.MHAdmittedPatientClass").alias("HospitalBedTypeMH"),
        col("a.WardLocDistanceHome"),
        col("a.StartDateWardStay").cast("date").alias("Start_DateWardStay"),
        col("a.StartTimeWardStay").alias("Start_TimeWardStay"),
        col("a.EndDateWardStay").cast("date").alias("End_DateWardStay"),
        col("a.EndTimeWardStay").alias("End_TimeWardStay"),
        col("a.BedDaysWSEndRP"),
        col("a.Der_Age_at_StartWardStay"),
        col("a.EFFECTIVE_FROM"),
        lit(None).cast("string").alias("Main_Reason_for_AWOL"),
        col("awl.StartDateMHAbsWOLeave"),
        col("awl.StartTimeMHAbsWOLeave"),
        col("awl.EndDateMHAbsWOLeave"),
        col("awl.EndTimeMHAbsWOLeave"),
        col("awl.AWOLDaysEndRP").alias("AWOL_Days"),
        col("par.PoliceAssistArrDate"),
        col("par.PoliceAssistArrTime"),
        col("par.PoliceAssistReqDate"),
        col("par.PoliceAssistReqTime"),
        col("par.PoliceRestraintForceUsedInd"),
        col("loa.StartDateMHLeaveAbs"),
        col("loa.StartTimeMHLeaveAbs"),
        col("loa.EndDateMHLeaveAbs"),
        col("loa.EndTimeMHLeaveAbs"),
        col("loa.LOADaysRP"),
        lit(None).cast("string").alias("MHLeaveAbsEndReason"),
    )
)

ward_latest = (
    ward_base.withColumn(
        "MaxMonthRowOrder",
        F.max("MonthRowOrder").over(
            Window.partitionBy("UniqHospProvSpellID", "UniqWardStayID")
        ),
    )
    .withColumn(
        "Last_Submission",
        when(col("MonthRowOrder") == col("MaxMonthRowOrder"), lit(1)),
    )
    .filter(col("Last_Submission").isNotNull())
    .drop("MaxMonthRowOrder")
)

wardAtAdmission = ward_latest.withColumn(
    "WardStayOrder",
    row_number().over(
        Window.partitionBy("UniqHospProvSpellID").orderBy(
            col("Start_DateWardStay").asc(), col("Start_TimeWardStay").asc()
        )
    ),
)

# ─── 7. BUILD FINAL MH_SPELLS TABLE (spells_df) ─────────────────────────────

# sanitise staging tables before joins/writes
wardAtAdmission = sanitise_columns(wardAtAdmission)
temp11 = sanitise_columns(temp11)

# base with joins to ward-at-admission, tempRef, MPI, DimProvider, ICS, etc.
spells_base = (
    temp11.alias("a")
    .join(
        wardAtAdmission.alias("w"),
        (col("w.UniqHospProvSpellID") == col("a.UniqHospProvSpellID"))
        & (col("w.WardStayOrder") == lit(1)),
        "left",
    )
    .join(
        tempRef.alias("ccc"),
        col("ccc.UniqServReqID") == col("a.UniqServReqID"),
        "left",
    )
    .join(
        src["MPI"].alias("mpi"),
        (col("mpi.RecordNumber") == col("a.RecordNumber"))
        & (col("mpi.UniqMonthID") == col("a.UniqMonthID")),
        "left",
    )
    # DimProvider: provider (OrgIDProv)
    .join(
        src["DimProvider"].alias("dp_prov"),
        col("dp_prov.OrgCode") == col("a.OrgIDProv"),
        "left",
    )
    # DimProvider: admission site (SiteIDOfTreat from ward)
    .join(
        src["DimProvider"].alias("dp_site"),
        col("dp_site.OrgCode") == col("w.SiteIDOfTreat"),
        "left",
    )
    # ICS mapping – tm (join on site OrgName)
    .join(
        src["TrustICS"].alias("tm"),
        col("tm.Site") == col("dp_site.OrgName"),
        "left",
    )
    # HospSpell commissioning – hs
    .join(
        src["HospSpellComm"].alias("hs"),
        (col("hs.RecordNumber") == col("a.RecordNumber"))
        & (col("hs.UniqHospProvSpellID") == col("a.UniqHospProvSpellID")),
        "left",
    )
    # method of admission – moa
    .join(
        src["RefMethodAdm"].alias("moa"),
        col("moa.code") == col("a.MethAdmMHHospProvSpell"),
        "left",
    )
)

# diagnosis flags
diag_psychosis = (
    lower(coalesce(col("a.Primary_diagnosis_description"), lit(""))).contains("psychosis")
    | lower(
        coalesce(col("a.Secondary_diagnosis_description"), lit(""))
    ).contains("psychosis")
    | lower(
        coalesce(col("a.Provisional_diagnosis_description"), lit(""))
    ).contains("psychosis")
)

diag_personality = (
    lower(coalesce(col("a.Primary_diagnosis_description"), lit(""))).contains("personality")
    | lower(
        coalesce(col("a.Secondary_diagnosis_description"), lit(""))
    ).contains("personality")
    | lower(
        coalesce(col("a.Provisional_diagnosis_description"), lit(""))
    ).contains("personality")
)

diag_bipolar = (
    lower(coalesce(col("a.Primary_diagnosis_description"), lit(""))).contains("bipolar")
    | lower(
        coalesce(col("a.Secondary_diagnosis_description"), lit(""))
    ).contains("bipolar")
    | lower(
        coalesce(col("a.Provisional_diagnosis_description"), lit(""))
    ).contains("bipolar")
)

# LoS tranche
los_tranche = (
    when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 1)
        & (col("a.HOSP_LOS") < 8),
        "Up to 1 week",
    )
    .when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 8)
        & (col("a.HOSP_LOS") < 15),
        "BTWn 1 and 2 wks",
    )
    .when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 15)
        & (col("a.HOSP_LOS") < 31),
        "Btwn 2wks and 1mth",
    )
    .when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 31)
        & (col("a.HOSP_LOS") < 91),
        "BTWn 1 mth and 3mths",
    )
    .when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 91)
        & (col("a.HOSP_LOS") < 181),
        "BTWn 3 mths and 6 mths",
    )
    .when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 181)
        & (col("a.HOSP_LOS") < 366),
        "BTWn 6 mths and 1 yr",
    )
    .when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 366),
        "1 yr and above",
    )
)

stranded_status = (
    when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 60)
        & (col("a.HOSP_LOS") < 90),
        "Stranded",
    ).when(
        (col("a.DischDateHospProvSpell").isNotNull())
        & (col("a.HOSP_LOS") >= 90),
        "Super Stranded",
    )
)

new_los_col = col("a.HOSP_LOS_NEW")

# Known / previously known
months_diff = months_between(
    F.to_date(col("a.StartDateHospProvSpell")),
    F.to_date(col("ccc.ReferralRequestReceivedDate")),
)

known_to_mh = when(
    (col("ccc.ReferralRequestReceivedDate").isNotNull())
    & (
        datediff(
            F.to_date(col("a.StartDateHospProvSpell")),
            F.to_date(col("ccc.ReferralRequestReceivedDate")),
        )
        >= 0
    )
    & (months_diff <= 6.0),
    lit(1),
)

known_last_24m = when(
    (col("ccc.ReferralRequestReceivedDate").isNotNull())
    & (
        datediff(
            F.to_date(col("a.StartDateHospProvSpell")),
            F.to_date(col("ccc.ReferralRequestReceivedDate")),
        )
        >= 0
    )
    & (months_diff <= 24.0),
    lit(1),
)

previously_known = when(
    (col("ccc.ReferralRequestReceivedDate").isNotNull())
    & (
        datediff(
            F.to_date(col("a.StartDateHospProvSpell")),
            F.to_date(col("ccc.ReferralRequestReceivedDate")),
        )
        >= 0
    )
    & (months_diff > 24.0),
    lit(1),
)

# AWOL flags
awol_flag = when(col("w.StartDateMHAbsWOLeave").isNotNull(), lit(1))
awol_wardstay_id = when(
    col("w.StartDateMHAbsWOLeave").isNotNull(), col("w.UniqWardStayID").cast("string")
)

# Psychosis/personality/bipolar flags using Age_at_Referral + Gender
male_psychosis_18_44 = when(
    diag_psychosis
    & (col("ccc.Age_at_Referral").between(18, 44))
    & (col("a.Gender") == "Male"),
    lit(1),
)

male_personality_18_44 = when(
    diag_personality
    & (col("ccc.Age_at_Referral").between(18, 44))
    & (col("a.Gender") == "Male"),
    lit(1),
)

bipolar_flag = when(diag_bipolar, lit(1))

# Provider_Type (from DimProvider.ODSOrgType)
provider_type = (
    when(col("dp_prov.ODSOrgType").isin("NHS TRUST", "CARE TRUST"), "NHS TRUST")
    .when(
        col("dp_prov.ODSOrgType").isin(
            "INDEPENDENT SECTOR HEALTHCARE PROVIDER",
            "INDEPENDENT SECTOR H/C PROVIDER SITE",
            "NON-NHS ORGANISATION",
        ),
        "NON-NHS TRUST",
    )
    .otherwise("Missing/Invalid")
)

# Adm_MonthYear – first day of month of StartDateHospProvSpell
adm_month_year = F.trunc(col("a.StartDateHospProvSpell"), "Month")

# Source of admission grouping
source_of_admission = (
    when(col("a.SourceAdmCodeHospProvSpell") == "19", "Usual place of residence")
    .when(col("a.SourceAdmCodeHospProvSpell") == "29", "Temporary place of residence")
    .when(
        col("a.SourceAdmCodeHospProvSpell").isin("37", "40", "42"),
        "Criminal setting",
    )
    .when(
        col("a.SourceAdmCodeHospProvSpell").isin("49", "51", "52", "53"),
        "NHS healthcare provider",
    )
    .when(
        col("a.SourceAdmCodeHospProvSpell") == "87",
        "Independent sector healthcare provider",
    )
    .when(
        col("a.SourceAdmCodeHospProvSpell").isin("55", "56", "66", "88"), "Other"
    )
    .when(col("a.SourceAdmCodeHospProvSpell").isNull(), "Null")
    .otherwise("Missing/Invalid")
)

# Specialised commissioning code (big IN set)
spec_comm_list = [
    "13N","13R","13V","13X","13Y","14C","14D","14E","14F","14G","85J","27T","14A",
    "14E","14G","14F","13R","L5H9Q","N8S0C","Q7O8U","X8H3R","P7L6U","F3I2L","S7T0C",
    "Z1U2L","C9Z7X","F9H5S","K5B5Y","S6Z6H","J3T7D","I0H0N","O5V1Z","E2S1E","A8R9E",
    "S5L0S","N5T4E","O6H3T","I2T5F","K4Z4O","Z0X9Q","B9Q0L","I3Q3V","X4I1M","N9S3D",
    "D8D1G","Z4P6N","D4U5V","P9W2J","L4H0W","B5S8O","G1U9X","X6C7V","C8S2X","R7G8O",
    "H3F5A","I4B8X","X4L0A","B0N9F","N5E8H","M4X2K","A3Y0R","W6B3O","O1N4A","Z0B3G",
]

spec_comm_code = when(col("hs.OrgIDComm").isin(spec_comm_list), "Yes").otherwise("No")

spells_df = (
    spells_base
    .withColumn("LoS_Tranche", los_tranche)
    .withColumn("Stranded_Status", stranded_status)
    .withColumn("NewLOS", new_los_col)
    .withColumn("Known_to_MH_Services_Flag", known_to_mh)
    .withColumn("KnownInLast24Months", known_last_24m)
    .withColumn("PreviouslyKnown", previously_known)
    .withColumn("AWOL_Flag", awol_flag)
    .withColumn("AWOL_WardStay_ID", awol_wardstay_id)
    .withColumn("Male_Psychosis_18_44_Flag", male_psychosis_18_44)
    .withColumn(
        "Male_Personality_Disorder_18_44_Flag",
        male_personality_18_44,
    )
    .withColumn("BiPolar_Flag", bipolar_flag)
    .withColumn("Der_HospSpellStatus", col("a.AdmissionCat"))
    .withColumn("Provider_Type", provider_type)
    .withColumn("Adm_MonthYear", adm_month_year)
    .withColumn("SourceOfAdmission", source_of_admission)
    .withColumn("SpecCommCode", spec_comm_code)
    .withColumn("Der_AdmissionMethod", col("moa.description"))
    .select(
        # core IDs
        col("a.UniqMonthID"),
        col("a.UniqHospProvSpellID"),
        col("a.UniqSubmissionID"),
        col("a.Person_ID"),
        col("a.UniqServReqID"),
        col("a.RecordNumber"),

        # from MPI if present
        col("mpi.Der_Person_ID"),
        col("mpi.Der_Pseudo_NHS_Number"),

        # geography from temp11
        col("a.LSOA2011"),
        col("a.LAName").alias("Pat_Postcode_Lan_Name"),
        col("a.Res_MH_Trust_by_PatPostcode"),
        col("a.ICB_of_Res_MH_Trust_by_PatPostcode"),
        col("a.Borough_Res_MH_Trust_by_PatPostcode"),

        # GP + borough from temp11
        col("a.GP_Name"),
        col("a.GP_Region_Name"),
        col("a.GP_Borough"),
        col("a.GP_LSOA"),
        col("a.GP_Longitude"),
        col("a.GP_Latitude"),

        # provider fields (DimProvider + ICS)
        col("a.OrgIDProv").alias("OrgIDProv"),
        col("dp_prov.OrgName").alias("Provider_Name"),
        col("dp_prov.NHSE_RegionName").alias("Provider_Region_Name"),
        col("dp_site.OrgName").alias("Admission_Site_Name"),
        col("tm.ICS").alias("Provider_ICS_Full_Name"),
        col("tm.ICB").alias("Provider_ICS_Abbrev"),
        col("Provider_Type"),
        col("SpecCommCode"),

        # demographics
        col("a.Gender"),
        col("a.Derived_Broad_Ethnic_Category").alias("Derived_Broad_Ethnic_Category"),
        col("a.AgeBand").alias("AgeBand_Snapshot"),

        # spell dates/times
        col("a.StartDateHospProvSpell"),
        col("a.StartTimeHospProvSpell"),
        col("a.DischDateHospProvSpell"),
        col("a.DischTimeHospProvSpell"),
        col("a.EstimatedDischDateHospProvSpell"),
        col("a.PlannedDischDateHospProvSpell"),
        col("Adm_MonthYear"),

        # source of admission & method
        col("a.SourceAdmCodeHospProvSpell"),
        col("SourceOfAdmission"),
        col("Der_AdmissionMethod"),

        # ward at admission info
        col("w.WardStayOrder"),
        col("w.AdmissionTypeNHSE"),
        col("w.AdmissionType_MHUEC"),
        col("w.HospitalBedTypeName"),
        col("w.SpecialisedMHServiceCode"),
        col("w.HospitalBedTypeMH"),
        col("w.WardType"),
        col("w.WardSexTypeCode"),
        col("w.WardCode"),
        col("w.SiteIDOfTreat"),
        col("w.Start_DateWardStay"),
        col("w.Start_TimeWardStay"),
        col("w.End_DateWardStay"),
        col("w.End_TimeWardStay"),
        col("w.BedDaysWSEndRP"),
        col("w.Der_Age_at_StartWardStay"),

        # diagnoses
        col("a.Primary_ICD10_code"),
        col("a.Primary_diagnosis_description"),
        col("a.Primary_diagnosis_ICD10_chapter"),
        col("a.Secondary_ICD10_code"),
        col("a.Secondary_diagnosis_description"),
        col("a.Secondary_diagnosis_ICD10_chapter"),
        col("a.Provisional_ICD10_code"),
        col("a.Provisional_diagnosis_description"),
        col("a.Provisional_diagnosis_ICD10_chapter"),

        # LoS metrics
        col("a.HOSP_LOS"),
        col("a.HOSP_LOS_at_Last_Update_for_Incomplete_Spells"),
        col("LoS_Tranche"),
        col("Stranded_Status"),
        col("NewLOS"),
        col("Der_HospSpellStatus"),

        # referral context (from tempRef)
        col("ccc.ReferralRequestReceivedDate"),
        col("ccc.Referring_Organisation"),
        col("ccc.Referring_Org_Type"),
        col("ccc.Referring_Care_Professional_Staff_Group"),
        col("ccc.Referral_Source"),
        col("ccc.Primary_Reason_for_Referral"),
        col("ccc.Clinical_Priority"),
        col("ccc.Age_at_Referral"),

        # AWOL / LOA fields
        col("w.Main_Reason_for_AWOL"),
        col("w.StartDateMHAbsWOLeave"),
        col("w.StartTimeMHAbsWOLeave"),
        col("w.EndDateMHAbsWOLeave"),
        col("w.EndTimeMHAbsWOLeave"),
        col("w.AWOL_Days"),
        col("w.StartDateMHLeaveAbs"),
        col("w.StartTimeMHLeaveAbs"),
        col("w.EndDateMHLeaveAbs"),
        col("w.EndTimeMHLeaveAbs"),
        col("w.LOADaysRP"),
        col("w.MHLeaveAbsEndReason"),
        col("AWOL_Flag"),
        col("AWOL_WardStay_ID"),

        # diagnosis-based flags
        col("Male_Psychosis_18_44_Flag"),
        col("Male_Personality_Disorder_18_44_Flag"),
        col("BiPolar_Flag"),

        # known-service flags
        col("Known_to_MH_Services_Flag"),
        col("KnownInLast24Months"),
        col("PreviouslyKnown"),
    )
)

spells_df = sanitise_columns(spells_df)

# ─── 8. WRITE OUT STAGING + FINAL TABLES (Delta) ────────────────────────────

print("=== CHECKPOINT: about to write MH_Ward_Stays and MH_Spells ===")
print("target_ward   =", target_ward)
print("target_spells =", target_spells)

print("\nSchema of spells_df just before write:")
spells_df.printSchema()

print("\nRow count of spells_df (this triggers the computation):")
spells_df.selectExpr("count(*) as rows").show(truncate=False)

# ─── HARD DELETE EXISTING FOLDERS (clean reset) ─────────────────────────────
for p in [target_ward, target_spells]:
    print(f"\nDeleting existing path (if it exists): {p}")
    try:
        dbutils.fs.rm(p, recurse=True)
        print(f"  -> Deleted {p}")
    except Exception as e:
        print(f"  -> Could not delete {p} (may not exist yet): {e}")

# ─── WRITE FRESH DELTA TABLES ───────────────────────────────────────────────

# MH_Ward_Stays staging
(
    wardAtAdmission.write
    .format("delta")
    .mode("overwrite")               # overwrite is now just belt-and-braces
    .option("overwriteSchema", "true")
    .save(target_ward)
)
print("\n✅ Written MH_Ward_Stays to:", target_ward)

# Final MH_Spells table
(
    spells_df.write
    .format("delta")
    .mode("overwrite")               # overwrite on a clean folder
    .option("overwriteSchema", "true")
    .save(target_spells)
)
print("✅ Written MH_Spells to:", target_spells)

# COMMAND ----------
# Spot check a few rows
shellDF = spells_df.select(
    "OrgIDProv",
    "Provider_Name",
    "Provider_Region_Name",
    "SiteIDOfTreat",
    "Admission_Site_Name"
).filter(col("Provider_Region_Name")=="London")


shellDF.display()

