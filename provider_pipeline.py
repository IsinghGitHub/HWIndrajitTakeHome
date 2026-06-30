"""
Provider Network Pipeline — unified replacement for provider_count_assignment.py and
provider_download_assignment.py.

Architecture:
  - DuckDB does all analytical work: parquet pushdown, SQL joins, window functions,
    GROUP BY aggregations.
  - Single read+clean+score-join pass shared between count and download outputs.
  - Polars writes output files.

Fixes vs originals:
  1. Dead empty-guard: load_compare_files assigned to base_ind_df on empty — fixed.
  2. NameError if compareorgid_list empty — guarded with early return.
  3. Swallowed exceptions — now calls emit_error + traceback.
  4. groupby.apply Python lambda in score categorization — replaced with SQL FILTER agg.
  5. Serial parquet reads — DuckDB reads all files in one vectorised scan with pushdown.
  6. Duplicate file reads across two scripts — eliminated; data loaded once.
"""

import os
import sys
import json
import time
import traceback
import warnings

import duckdb
import numpy as np
import pandas as pd
import polars as pl

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
file_path = "./sample_data/"
additionalFilesPath = "./sample_data/"
outputfile_path = "./sample_data/output/Individual/"
outputfile_path_hosp = "./sample_data/output/Organization/"
output_dir = "./sample_data/output"

# ---------------------------------------------------------------------------
# Specialty-code parquet files to EXCLUDE from individual reads
# ---------------------------------------------------------------------------
SPEC_CODES: set = {str(s) + ".parquet" for s in [
    25270,25277,25271,25281,25282,25272,25273,25274,25283,25269,25275,25278,25276,25280,25279,
    26285,26284,26288,26292,26286,26287,26290,26293,26289,26291,26294,19066,27295,27296,27297,
    27298,28309,28304,28300,28308,28302,29311,29312,29313,29310,30314,30315,30317,30316,31320,
    31318,31327,31321,31325,31319,31326,31322,31324,18064,18065,32330,32331,32333,32332,32328,
    32329,32334,38382,39383,33335,33337,33341,33347,33343,33344,33345,33346,33349,33338,33351,
    33350,33340,33336,33339,33352,33342,33348,33353,34360,34354,34361,34355,34358,34362,34357,
    34359,34363,34364,34356,10006,10005,10002,10001,10011,10004,10003,12018,12020,12016,12021,
    12014,12017,12013,12015,12019,14029,14028,14027,14026,15031,31323,16033,16035,16034,16032,
    37372,37371,37373,37375,37378,37380,37381,37379,37377,37376,37374,17041,17056,17047,17062,
    17037,17040,17053,17058,17055,17059,17048,17063,17051,17045,17036,17061,17038,17046,17057,
    17060,40384,17049,17050,17054,36370,36367,36368,22249,22225,22231,22238,22226,22246,22239,
    22228,22241,22233,22227,22229,22230,22242,22240,22245,22243,22236,22247,22248,22237,23254,
    23251,24268,24255,24266,24256,24261,24263,24257,24262,24264,24258,24260,24265,24267,24259,
    17043,17044,17042,
]}

# Networks where hospital counts are intentionally suppressed (magic number, kept for compatibility)
HOSP_NAN_NETWORKS: set = {208546}


# ---------------------------------------------------------------------------
# Emit helpers
# ---------------------------------------------------------------------------
def emit_progress(percent, message, extra=None):
    payload = {"type": "progress", "percent": percent, "message": message}
    if extra is not None:
        payload["data"] = extra
    print(json.dumps(payload))
    sys.stdout.flush()


def emit_log(message):
    print(json.dumps({"type": "log", "message": message}))
    sys.stdout.flush()


def emit_error(message):
    print(json.dumps({"type": "error", "message": message}))
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# DuckDB data loaders
# ---------------------------------------------------------------------------
def _ind_paths(file_path: str, filespath: str, net_ids: list) -> list:
    paths = []
    for net in net_ids:
        d = f"{file_path}{filespath}/network_files/Individual/{net}"
        paths += [f"{d}/{f}" for f in sorted(os.listdir(d)) if f not in SPEC_CODES]
    return paths


def _load_ind_table(con, file_path, filespath, net_ids, table_name, network_id_str, network_name, county_sql):
    paths = _ind_paths(file_path, filespath, net_ids)
    if not paths:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT NULL::BIGINT AS npi, NULL::VARCHAR AS type,
                   NULL::VARCHAR AS presentation_name, NULL::VARCHAR AS first_name,
                   NULL::VARCHAR AS last_name, NULL::VARCHAR AS network_id,
                   NULL::VARCHAR AS network_name, NULL::VARCHAR AS "Address First Line",
                   NULL::VARCHAR AS parent_org, NULL::VARCHAR AS location_confidence,
                   NULL::VARCHAR AS location_confidence_address_level,
                   NULL::VARCHAR AS specialty_category, NULL::VARCHAR AS specialty_name,
                   NULL::VARCHAR AS specialty_subspecialty, NULL::VARCHAR AS specialty_code,
                   NULL::VARCHAR AS "Specialty Flag", NULL::VARCHAR AS "New Flag",
                   NULL::VARCHAR AS city, NULL::VARCHAR AS state,
                   NULL::VARCHAR AS zip_code, NULL::VARCHAR AS county_name,
                   NULL::BIGINT  AS fips, NULL::VARCHAR AS "Specialty Description",
                   NULL::VARCHAR AS "Is_Dual_Provider",
                   NULL::VARCHAR AS group_tin, NULL::VARCHAR AS group_name
            WHERE false
        """)
        return

    paths_sql = "[" + ",".join(f"'{p}'" for p in paths) + "]"
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT npi::BIGINT                                    AS npi,
               type::VARCHAR                                  AS type,
               presentation_name::VARCHAR                     AS presentation_name,
               first_name::VARCHAR                            AS first_name,
               last_name::VARCHAR                             AS last_name,
               '{network_id_str}'::VARCHAR                    AS network_id,
               '{network_name}'::VARCHAR                      AS network_name,
               "Address First Line"::VARCHAR                  AS "Address First Line",
               "Parent_Organization"::VARCHAR                 AS parent_org,
               location_confidence::VARCHAR                   AS location_confidence,
               location_confidence_address_level::VARCHAR     AS location_confidence_address_level,
               specialty_category::VARCHAR                    AS specialty_category,
               specialty_name::VARCHAR                        AS specialty_name,
               specialty_subspecialty::VARCHAR                AS specialty_subspecialty,
               specialty_code::VARCHAR                        AS specialty_code,
               "Specialty Flag"::VARCHAR                      AS "Specialty Flag",
               "New Flag"::VARCHAR                            AS "New Flag",
               city::VARCHAR                                  AS city,
               state::VARCHAR                                 AS state,
               zip_code::VARCHAR                              AS zip_code,
               county_name::VARCHAR                           AS county_name,
               "FIPS State County Code"::BIGINT               AS fips,
               "Specialty Description"::VARCHAR               AS "Specialty Description",
               "Is_Dual_Provider"::VARCHAR                    AS "Is_Dual_Provider",
               "Affiliation ID"::VARCHAR                      AS group_tin,
               "Affiliation Presentation Name"::VARCHAR       AS group_name
        FROM read_parquet({paths_sql}, union_by_name=true)
        WHERE "FIPS State County Code"::BIGINT IN ({county_sql})
          AND location_confidence IN ('Low','Medium','High','LOCATION CONFIDENCE NOT AVAILABLE')
          AND "New Flag" IN ('PCP','Physician Specialists','Other Providers')
    """)


def _load_org_table(con, file_path, filespath, net_ids, table_name, network_id_str, network_name, county_sql):
    parts = []
    for net in net_ids:
        org_file = f"{file_path}{filespath}/network_files/Organization/{net}.csv"
        if not os.path.exists(org_file):
            continue
        parts.append(f"""
            SELECT npi::BIGINT                       AS npi,
                   type::VARCHAR                     AS type,
                   presentation_name::VARCHAR        AS presentation_name,
                   first_name::VARCHAR               AS first_name,
                   last_name::VARCHAR                AS last_name,
                   '{network_id_str}'::VARCHAR       AS network_id,
                   '{network_name}'::VARCHAR         AS network_name,
                   carrier_name::VARCHAR             AS carrier_name,
                   specialty_category::VARCHAR       AS specialty_category,
                   specialty_name::VARCHAR           AS specialty_name,
                   specialty_subspecialty::VARCHAR   AS specialty_subspecialty,
                   specialty_code::VARCHAR           AS specialty_code,
                   "New Flag"::VARCHAR               AS "New Flag",
                   city::VARCHAR                     AS city,
                   state::VARCHAR                    AS state,
                   zip_code::VARCHAR                 AS zip_code,
                   county_name::VARCHAR              AS county_name,
                   "FIPS State County Code"::BIGINT  AS fips,
                   "Address First Line"::VARCHAR     AS "Address First Line",
                   Status::VARCHAR                   AS Status,
                   specialty_name::VARCHAR           AS "Specialty Description",
                   carrier_name::VARCHAR             AS parent_org,
                   NULL::VARCHAR                     AS group_tin,
                   NULL::VARCHAR                     AS group_name
            FROM read_csv('{org_file}', auto_detect=true, null_padding=true)
            WHERE UPPER(COALESCE(Status,'')) = 'ACTIVE'
              AND "FIPS State County Code"::BIGINT IN ({county_sql})
              AND COALESCE("New Flag",'') != 'None'
        """)

    if parts:
        union_sql = " UNION ALL ".join(f"({p})" for p in parts)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {union_sql}")
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT NULL::BIGINT AS npi, NULL::VARCHAR AS type,
                   NULL::VARCHAR AS presentation_name, NULL::VARCHAR AS first_name,
                   NULL::VARCHAR AS last_name, NULL::VARCHAR AS network_id,
                   NULL::VARCHAR AS network_name, NULL::VARCHAR AS carrier_name,
                   NULL::VARCHAR AS specialty_category, NULL::VARCHAR AS specialty_name,
                   NULL::VARCHAR AS specialty_subspecialty, NULL::VARCHAR AS specialty_code,
                   NULL::VARCHAR AS "New Flag", NULL::VARCHAR AS city, NULL::VARCHAR AS state,
                   NULL::VARCHAR AS zip_code, NULL::VARCHAR AS county_name,
                   NULL::BIGINT  AS fips, NULL::VARCHAR AS "Address First Line",
                   NULL::VARCHAR AS Status, NULL::VARCHAR AS "Specialty Description",
                   NULL::VARCHAR AS parent_org,
                   NULL::VARCHAR AS group_tin, NULL::VARCHAR AS group_name
            WHERE false
        """)


# ---------------------------------------------------------------------------
# Score file loaders
# ---------------------------------------------------------------------------
def _load_score_tables(con, filespath, county_sql):
    score_ind_path = f"{additionalFilesPath}{filespath}/Score for Individual - PNC Medicare.csv"
    score_org_path = f"{additionalFilesPath}{filespath}/Scores for Organization - PNC Medicare.csv"

    con.execute(f"""
        CREATE OR REPLACE TABLE score_ind AS
        SELECT NPI::BIGINT                                   AS npi,
               "New Flag"::VARCHAR                           AS "New Flag",
               "County FIPS"::BIGINT                         AS fips,
               TRY_CAST("Cost Score" AS DOUBLE)              AS cost_score,
               TRY_CAST("Quality Score" AS DOUBLE)           AS quality_score,
               Specialty::VARCHAR                            AS specialty_desc,
               TRY_CAST("NPI Score" AS DOUBLE)               AS npi_score,
               TRY_CAST("MA Utilization Score" AS DOUBLE)    AS ma_util_score,
               TRY_CAST("FFS Utilization Score" AS DOUBLE)   AS ffs_util_score,
               "Quality color indicator"::VARCHAR            AS quality_color
        FROM read_csv('{score_ind_path}', auto_detect=true, ignore_errors=true)
        WHERE "County FIPS"::BIGINT IN ({county_sql})
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY NPI::BIGINT, "New Flag", "County FIPS"::BIGINT, Specialty
            ORDER BY 1
        ) = 1
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE score_org AS
        SELECT npi::BIGINT                                         AS npi,
               UPPER(TRIM("Address First Line"::VARCHAR))          AS address,
               state::VARCHAR                                      AS state,
               "FIPS State County Code"::BIGINT                    AS fips,
               TRY_CAST("Quality Score" AS DOUBLE)                 AS quality_score,
               TRY_CAST("Cost Score" AS DOUBLE)                    AS cost_score,
               TRY_CAST("FFS Utilization Score" AS DOUBLE)         AS ffs_util_score,
               TRY_CAST("MA Utilization Score" AS DOUBLE)          AS ma_util_score
        FROM read_csv('{score_org_path}', auto_detect=true, ignore_errors=true)
        WHERE "FIPS State County Code"::BIGINT IN ({county_sql})
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY npi::BIGINT, "Address First Line", state, "FIPS State County Code"::BIGINT
            ORDER BY 1
        ) = 1
    """)


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------
def run_pipeline(filespath, base, compare, baseParentOrgId, compareParentOrgId,
                 planTypeId, baseParentOrg, compareParentOrg, planType,
                 id_parameter, county, clientId, userId):
    try:
        # Guard: empty compare list causes NameError after the loop in original scripts
        compareorgid_list = [i.strip() + "_" + planTypeId for i in compareParentOrgId.split(",")]
        if not compareorgid_list:
            emit_error("compareParentOrgId produced an empty list — nothing to process.")
            return None

        baseorg = baseParentOrg + " " + planType
        compareorg = [i.strip() + " " + planType for i in compareParentOrg.split("|")]
        base_compareorgid = baseParentOrgId + "_" + planTypeId
        county_sql = ", ".join(str(c) for c in county)

        con = duckdb.connect()

        # ----------------------------------------------------------------
        # Load score tables ONCE (shared by count + download)
        # ----------------------------------------------------------------
        _load_score_tables(con, filespath, county_sql)

        # ----------------------------------------------------------------
        # Load base network data ONCE
        # ----------------------------------------------------------------
        net1_list = list(set(int(n.strip()) for n in base[baseParentOrgId]["networkIds"].split(",")))
        _load_ind_table(con, file_path, filespath, net1_list,
                        "ind_base", base_compareorgid, baseorg, county_sql)
        _load_org_table(con, file_path, filespath, net1_list,
                        "org_base", base_compareorgid, baseorg, county_sql)

        # ----------------------------------------------------------------
        # HSD specialty list (for bar-graph counts)
        # ----------------------------------------------------------------
        hsd_path = f"{additionalFilesPath}{filespath}/HSD_specialty.csv"
        hsd_df = pd.read_csv(hsd_path)
        hsd_df["Specialty Description"] = hsd_df["Specialty Description"].str.strip()
        hsd_list_upper = set(hsd_df["Specialty Description"].str.upper())

        # ----------------------------------------------------------------
        # Hospital name mapping
        # ----------------------------------------------------------------
        hosp_map_path = f"{additionalFilesPath}{filespath}/Hospital_name_mapping.csv"
        hosp_map_df = pd.read_csv(hosp_map_path)
        hosp_map_df["Presentation Name"] = hosp_map_df["Presentation Name"].str.strip().str.upper()
        hosp_map_df["AHD_Facility Name"] = hosp_map_df["AHD_Facility Name"].str.strip()

        # ----------------------------------------------------------------
        # Market performance baseline
        # ----------------------------------------------------------------
        mkt_ind_path = f"{additionalFilesPath}{filespath}/network_performance_countywise.csv"
        mkt_org_path = f"{additionalFilesPath}{filespath}/network_performance_countywise_hospital.csv"
        mkt_ind = pd.read_csv(mkt_ind_path)
        mkt_org = pd.read_csv(mkt_org_path)
        mkt_org.rename(columns={"FIPS County Code": "FIPS State County Code"}, inplace=True)
        mkt_ind = mkt_ind[mkt_ind["FIPS State County Code"].isin(county)]
        mkt_org = mkt_org[mkt_org["FIPS State County Code"].isin(county)]
        mkt_ind = mkt_ind[mkt_ind["Scoring Category"] != "Not Available"]
        mkt_org = mkt_org[mkt_org["Scoring Category"] != "Not Available"]
        mkt_ind.rename(columns={"MA Utilization Score": "Utilization Score"}, inplace=True)
        mkt_org.rename(columns={"MA Utilization Score": "Utilization Score"}, inplace=True)
        mkt_all = pd.concat([mkt_ind, mkt_org], ignore_index=True)
        mkt_all = mkt_all.drop(columns=["state", "county_name", "FIPS State County Code",
                                          "FFS Utilization Score"], errors="ignore")
        mkt_market = (
            mkt_all.groupby(["Scoring Category", "New Flag"], as_index=False)
            .agg({"Utilization Score": "sum", "Cost Score": "sum", "Quality Score": "sum"})
        )
        mkt_market["network_name"] = "Common Counties"
        mkt_market["network_id"] = ""

        # Accumulators
        finaltable = pd.DataFrame()
        finaltable_spec = pd.DataFrame()
        final_NetPerIndi = pd.DataFrame()

        # ----------------------------------------------------------------
        # Per-compare iteration
        # ----------------------------------------------------------------
        for idx, (compareId, compareid_name) in enumerate(zip(compareorgid_list, compareorg), 1):
            compare_net = compareId.split("_")[0]

            # Bug fix #1: original assigned to base_ind_df in empty-guard
            net2_list = list(set(int(n) for n in compare[compare_net]["networkIds"].split(",")))
            _load_ind_table(con, file_path, filespath, net2_list,
                            "ind_cmp", compareId, compareid_name, county_sql)
            _load_org_table(con, file_path, filespath, net2_list,
                            "org_cmp", compareId, compareid_name, county_sql)

            # Combined IND + ORG for this pair
            con.execute("""
                CREATE OR REPLACE TABLE combined_ind AS
                SELECT * FROM ind_base
                UNION ALL
                SELECT * FROM ind_cmp
            """)
            con.execute("""
                CREATE OR REPLACE TABLE combined_org AS
                SELECT * FROM org_base
                UNION ALL
                SELECT * FROM org_cmp
            """)

            # ---- Score joins ----
            con.execute("""
                CREATE OR REPLACE TABLE scored_ind AS
                SELECT i.*,
                       s.cost_score, s.quality_score, s.npi_score,
                       s.ma_util_score, s.ffs_util_score, s.quality_color
                FROM combined_ind i
                LEFT JOIN score_ind s
                  ON i.npi = s.npi
                 AND i."New Flag" = s."New Flag"
                 AND i.fips = s.fips
                 AND (CASE WHEN i."New Flag" = 'PCP' THEN 'PCP'
                           ELSE i."Specialty Description" END) = s.specialty_desc
            """)

            con.execute("""
                CREATE OR REPLACE TABLE scored_org AS
                SELECT o.*,
                       s.quality_score, s.cost_score, s.ffs_util_score, s.ma_util_score,
                       NULL::DOUBLE AS npi_score,
                       NULL::VARCHAR AS quality_color
                FROM combined_org o
                LEFT JOIN score_org s
                  ON o.npi = s.npi
                 AND UPPER(TRIM(o."Address First Line")) = s.address
                 AND o.state = s.state
                 AND o.fips = s.fips
            """)

            # ================================================================
            # COUNT OUTPUT BUILDER
            # ================================================================

            # Common/unique window flag for IND
            con.execute(f"""
                CREATE OR REPLACE TABLE cnt_ind_flagged AS
                SELECT *,
                  CASE WHEN COUNT(DISTINCT network_id) OVER (
                         PARTITION BY npi, state, fips, "New Flag"
                       ) > 1 THEN 'common' ELSE 'unique' END AS common_unique
                FROM combined_ind
            """)
            # org
            con.execute(f"""
                CREATE OR REPLACE TABLE cnt_org_flagged AS
                SELECT *,
                  CASE WHEN COUNT(DISTINCT network_id) OVER (
                         PARTITION BY npi, state, fips, "Address First Line", "New Flag", specialty_code
                       ) > 1 THEN 'common' ELSE 'unique' END AS common_unique,
                  npi::VARCHAR || "Address First Line" || specialty_code AS npi_x_addr
                FROM combined_org
            """)
            # specialty subset
            con.execute(f"""
                CREATE OR REPLACE TABLE cnt_ind_spec AS
                SELECT *,
                  CASE WHEN COUNT(DISTINCT network_id) OVER (
                         PARTITION BY npi, state, fips, "New Flag"
                       ) > 1 THEN 'common' ELSE 'unique' END AS common_unique
                FROM combined_ind
                WHERE UPPER("Specialty Description") IN ({
                    ", ".join(f"'{s}'" for s in hsd_list_upper)
                })
            """)

            # Aggregate counts: IND
            grp_ind = con.execute("""
                SELECT network_id AS "Network ID", network_name,
                       "New Flag" AS "Provider Type", common_unique AS "Unique/Common",
                       state, fips AS "FIPS State County Code",
                       COUNT(DISTINCT npi) AS NPI
                FROM cnt_ind_flagged
                GROUP BY network_id, network_name, "New Flag", common_unique, state, fips
            """).df()
            grp_ind.rename(columns={"Network ID": "network_id", "Provider Type": "flag",
                                    "Unique/Common": "common_unique", "NPI": "npi_count"}, inplace=True)

            # Aggregate counts: ORG
            grp_org = con.execute("""
                SELECT network_id AS "Network ID", network_name,
                       "New Flag" AS "Provider Type", common_unique AS "Unique/Common",
                       state, fips AS "FIPS State County Code",
                       COUNT(DISTINCT npi_x_addr) AS NPI
                FROM cnt_org_flagged
                GROUP BY network_id, network_name, "New Flag", common_unique, state, fips
            """).df()
            grp_org.rename(columns={"Network ID": "network_id", "Provider Type": "flag",
                                    "Unique/Common": "common_unique", "NPI": "npi_count"}, inplace=True)

            # Aggregate counts: Specialty bar graph
            grp_spec = con.execute("""
                SELECT network_id AS "Network ID", network_name,
                       "Specialty Description" AS "Provider Type", common_unique AS "Unique/Common",
                       state, fips AS "FIPS State County Code",
                       COUNT(DISTINCT npi) AS NPI
                FROM cnt_ind_spec
                GROUP BY network_id, network_name, "Specialty Description", common_unique, state, fips
            """).df()
            grp_spec.rename(columns={"Network ID": "network_id", "Provider Type": "flag",
                                     "Unique/Common": "common_unique", "NPI": "npi_count"}, inplace=True)

            group_df = pd.concat([grp_ind, grp_org], ignore_index=True)
            group_df = (
                group_df
                .groupby(["network_id", "network_name", "flag", "common_unique"], as_index=False)
                .agg({"npi_count": "sum"})
            )

            # HOSP_NAN_NETWORKS suppression
            if set(net1_list) & HOSP_NAN_NETWORKS:
                group_df = pd.concat([group_df, pd.DataFrame({
                    "network_id": [base_compareorgid], "network_name": [baseorg],
                    "flag": ["Hospital"], "npi_count": [" "]
                })], ignore_index=True)
            if set(net2_list) & HOSP_NAN_NETWORKS:
                group_df = pd.concat([group_df, pd.DataFrame({
                    "network_id": [compareId], "network_name": [compareid_name],
                    "flag": ["Hospital"], "npi_count": [" "]
                })], ignore_index=True)

            group_spec_df = (
                grp_spec
                .groupby(["network_id", "network_name", "flag", "common_unique"], as_index=False)
                .agg({"npi_count": "sum"})
            )

            group_df["base_compare"] = "Compare_" + str(idx)
            group_spec_df["base_compare"] = "Compare_" + str(idx)

            finaltable = pd.concat([finaltable, group_df], ignore_index=True)
            finaltable_spec = pd.concat([finaltable_spec, group_spec_df], ignore_index=True)

            # ---- Network performance (score categorization via SQL FILTER — no groupby.apply) ----
            for tbl, flag_col in [("scored_ind", "New Flag"), ("scored_org", "New Flag")]:
                perf = con.execute(f"""
                    WITH cats AS (
                        SELECT network_id, network_name, "{flag_col}",
                          CASE WHEN quality_score >= 4 THEN 'High'
                               WHEN quality_score > 2  THEN 'Medium'
                               WHEN quality_score <= 2 THEN 'Low'
                               ELSE 'Not Available' END AS quality_cat,
                          CASE WHEN cost_score >= 4 THEN 'High'
                               WHEN cost_score > 2  THEN 'Medium'
                               WHEN cost_score <= 2 THEN 'Low'
                               ELSE 'Not Available' END AS cost_cat,
                          CASE WHEN ma_util_score >= 80 THEN 'High'
                               WHEN ma_util_score <= 40 THEN 'Low'
                               WHEN ma_util_score > 40  THEN 'Medium'
                               ELSE 'Not Available' END AS util_cat
                        FROM {tbl}
                    ),
                    scored AS (
                        SELECT network_id, network_name, "{flag_col}" AS "New Flag",
                          COUNT(*) FILTER (WHERE quality_cat='High')   AS qs_high,
                          COUNT(*) FILTER (WHERE quality_cat='Medium') AS qs_med,
                          COUNT(*) FILTER (WHERE quality_cat='Low')    AS qs_low,
                          COUNT(*) FILTER (WHERE cost_cat='High')      AS cs_high,
                          COUNT(*) FILTER (WHERE cost_cat='Medium')    AS cs_med,
                          COUNT(*) FILTER (WHERE cost_cat='Low')       AS cs_low,
                          COUNT(*) FILTER (WHERE util_cat='High')      AS us_high,
                          COUNT(*) FILTER (WHERE util_cat='Medium')    AS us_med,
                          COUNT(*) FILTER (WHERE util_cat='Low')       AS us_low
                        FROM cats
                        WHERE quality_cat != 'Not Available'
                           OR cost_cat    != 'Not Available'
                           OR util_cat    != 'Not Available'
                        GROUP BY network_id, network_name, "{flag_col}"
                    )
                    SELECT network_id, network_name, "New Flag",
                           qs_high AS "Quality Score", cs_high AS "Cost Score",
                           us_high AS "Utilization Score", 'High' AS "Scoring Category"
                    FROM scored
                    UNION ALL
                    SELECT network_id, network_name, "New Flag",
                           qs_med, cs_med, us_med, 'Medium'
                    FROM scored
                    UNION ALL
                    SELECT network_id, network_name, "New Flag",
                           qs_low, cs_low, us_low, 'Low'
                    FROM scored
                """).df()
                final_NetPerIndi = pd.concat([final_NetPerIndi, perf], ignore_index=True)

            # ================================================================
            # DOWNLOAD OUTPUT BUILDER — Y/N membership flags
            # ================================================================
            con.execute(f"""
                CREATE OR REPLACE TABLE dl_ind AS
                SELECT *,
                  CASE WHEN COUNT(DISTINCT network_id) OVER (
                         PARTITION BY npi, state, county_name, "New Flag"
                       ) > 1 OR network_id = '{base_compareorgid}'
                       THEN 'Y' ELSE 'N' END AS base_flag,
                  CASE WHEN COUNT(DISTINCT network_id) OVER (
                         PARTITION BY npi, state, county_name, "New Flag"
                       ) > 1 OR network_id = '{compareId}'
                       THEN 'Y' ELSE 'N' END AS compare_flag
                FROM scored_ind
            """)
            con.execute(f"""
                CREATE OR REPLACE TABLE dl_org AS
                SELECT *,
                  CASE WHEN COUNT(DISTINCT network_id) OVER (
                         PARTITION BY npi, state, county_name, "Address First Line",
                                      "New Flag", specialty_code
                       ) > 1 OR network_id = '{base_compareorgid}'
                       THEN 'Y' ELSE 'N' END AS base_flag,
                  CASE WHEN COUNT(DISTINCT network_id) OVER (
                         PARTITION BY npi, state, county_name, "Address First Line",
                                      "New Flag", specialty_code
                       ) > 1 OR network_id = '{compareId}'
                       THEN 'Y' ELSE 'N' END AS compare_flag
                FROM scored_org
            """)

        # end for compareId loop

        # ----------------------------------------------------------------
        # Finalise count outputs
        # ----------------------------------------------------------------
        finaltable["network_id"] = finaltable["network_id"].astype(str)
        finaltable["base_compare"] = np.where(
            finaltable["network_id"] == base_compareorgid,
            "base_" + finaltable["base_compare"],
            finaltable["base_compare"],
        )
        finaltable["req_id"] = id_parameter

        finaltable_spec["network_id"] = finaltable_spec["network_id"].astype(str)
        finaltable_spec["base_compare"] = np.where(
            finaltable_spec["network_id"] == base_compareorgid,
            "base_" + finaltable_spec["base_compare"],
            finaltable_spec["base_compare"],
        )
        finaltable_spec["req_id"] = id_parameter

        networkPerf = pd.concat([final_NetPerIndi, mkt_market], ignore_index=True)
        networkPerf["req_id"] = id_parameter
        networkPerf = networkPerf[
            ["req_id", "network_id", "network_name",
             "Quality Score", "Cost Score", "Utilization Score", "New Flag", "Scoring Category"]
        ]

        os.makedirs(output_dir, exist_ok=True)
        finaltable[["req_id", "network_id", "network_name", "common_unique",
                    "base_compare", "flag", "npi_count"]].to_csv(
            f"{output_dir}/NI+Improved_Results_Table.csv", index=False)
        finaltable_spec[["req_id", "network_id", "network_name", "common_unique",
                          "base_compare", "flag", "npi_count"]].to_csv(
            f"{output_dir}/NI+Improved_Results_BarGraph.csv", index=False)
        networkPerf.to_csv(f"{output_dir}/NI+Improved_Results_PerformIndicators.csv", index=False)

        # ----------------------------------------------------------------
        # Finalise download outputs — pull last iteration's dl_ind / dl_org
        # ----------------------------------------------------------------
        dl_ind_df = con.execute("SELECT * FROM dl_ind").df()
        dl_org_df = con.execute("SELECT * FROM dl_org").df()

        # Rename base/compare flag columns to payer plan names
        dl_ind_df.rename(columns={"base_flag": baseorg, "compare_flag": compareid_name}, inplace=True)
        dl_org_df.rename(columns={"base_flag": baseorg, "compare_flag": compareid_name}, inplace=True)

        payer_cols = [baseorg, compareid_name]

        # Standard column rename map
        col_rename = {
            "New Flag": "Provider Type", "county_name": "County",
            "npi": "NPI", "type": "Type", "presentation_name": "Presentation Name",
            "city": "City", "state": "State", "zip_code": "Zip Code",
            "Specialty Description": "Specialty Name", "first_name": "First Name",
            "location_confidence": "Location Confidence County Level",
            "location_confidence_address_level": "Location Confidence Address level",
            "last_name": "Last Name", "group_name": "Group Name", "group_tin": "Group TIN",
            "fips": "FIPS State County Code", "specialty_name": "Specialty Name (raw)",
            "carrier_name": "carrier_name",
            "quality_score": "Quality Score", "cost_score": "Cost Score",
            "npi_score": "NPI Score", "ma_util_score": "MA Utilization Score",
            "ffs_util_score": "FFS Utilization Score", "quality_color": "Quality Score Confidence",
        }

        dl_ind_df.rename(columns=col_rename, inplace=True)
        dl_org_df.rename(columns=col_rename, inplace=True)

        # Score columns: stringify + fill NA
        # "Quality Score Confidence" (quality_color) is a category (e.g. Green/Yellow/Red),
        # not a number — running it through to_numeric would blank out every row.
        numeric_score_cols = ["NPI Score", "Quality Score", "Cost Score",
                               "FFS Utilization Score", "MA Utilization Score"]
        score_cols = numeric_score_cols + ["Quality Score Confidence"]
        for c in numeric_score_cols:
            for df_ref in [dl_ind_df, dl_org_df]:
                if c in df_ref.columns:
                    df_ref[c] = pd.to_numeric(df_ref[c], errors="coerce")

        for c in score_cols:
            for df_ref in [dl_ind_df, dl_org_df]:
                if c in df_ref.columns:
                    df_ref[c] = df_ref[c].astype("string").fillna("NA")

        # Hospital name mapping
        hosp_df = dl_org_df[dl_org_df["Provider Type"] == "Hospital"].copy()
        hosp_df["Presentation Name"] = hosp_df["Presentation Name"].str.strip().str.upper()
        hosp_df = hosp_df.merge(hosp_map_df, on="Presentation Name", how="left")
        hosp_df["Presentation Name"] = hosp_df["AHD_Facility Name"].fillna(
            hosp_df["Presentation Name"].str.title()
        )
        hosp_df.drop(columns=["AHD_Facility Name"], inplace=True, errors="ignore")

        # clientId column selection
        if clientId in [45]:
            hosp_score = ["Cost Score", "Quality Score", "FFS Utilization Score",
                          "MA Utilization Score", "Quality Score Confidence"]
            ind_score  = ["NPI Score", "Cost Score", "Quality Score", "FFS Utilization Score",
                          "MA Utilization Score", "Quality Score Confidence"]
            rest_hosp  = ["Type", "NPI", "Presentation Name", "Address First Line", "Zip Code",
                          "City", "State", "County", "FIPS State County Code",
                          "Provider Type", "Specialty Name", "Status"]
            rest_ind   = ["Type", "NPI", "First Name", "Last Name", "Presentation Name",
                          "Zip Code", "City", "State", "County", "FIPS State County Code",
                          "Location Confidence County Level", "Provider Type", "Specialty Name",
                          "Specialty Flag", "Status"]
            hosp_df["Location Confidence County Level"] = "High"
            hosp_df_out = hosp_df.drop(columns=["Address First Line"], errors="ignore")

        elif clientId == 8:
            hosp_score = ["Cost Score", "Quality Score", "FFS Utilization Score",
                          "MA Utilization Score", "Quality Score Confidence"]
            ind_score  = ["NPI Score", "Cost Score", "Quality Score", "FFS Utilization Score",
                          "MA Utilization Score", "Quality Score Confidence"]
            rest_hosp  = ["Type", "NPI", "Presentation Name", "Address First Line", "Zip Code",
                          "City", "State", "County", "FIPS State County Code",
                          "Provider Type", "Specialty Name", "Status"]
            rest_ind   = ["Type", "NPI", "First Name", "Last Name", "Presentation Name",
                          "Address First Line", "Zip Code", "City", "State", "County",
                          "FIPS State County Code", "Location Confidence Address level",
                          "Location Confidence County Level", "Provider Type", "Specialty Name",
                          "Specialty Flag", "Group TIN", "Group Name", "Status"]
            hosp_df["Location Confidence Address level"] = "High"
            hosp_df["Location Confidence County Level"] = "High"
            hosp_df_out = hosp_df

        elif clientId == 5:
            hosp_score = ["Cost Score", "Quality Score", "FFS Utilization Score",
                          "MA Utilization Score", "Quality Score Confidence"]
            ind_score  = ["NPI Score", "Cost Score", "Quality Score", "FFS Utilization Score",
                          "MA Utilization Score", "Quality Score Confidence"]
            rest_hosp  = ["Type", "NPI", "Presentation Name", "Address First Line", "Zip Code",
                          "City", "State", "County", "FIPS State County Code",
                          "Provider Type", "Specialty Name", "Status"]
            rest_ind   = ["Type", "NPI", "First Name", "Last Name", "Presentation Name",
                          "Zip Code", "City", "State", "County", "FIPS State County Code",
                          "Location Confidence County Level", "Provider Type", "Specialty Name",
                          "Specialty Flag", "Group TIN", "Group Name", "Status"]
            hosp_df["Location Confidence County Level"] = "High"
            hosp_df_out = hosp_df.drop(columns=["Address First Line"], errors="ignore")

        else:
            hosp_score = ["Cost Score", "Quality Score", "Quality Score Confidence"]
            ind_score  = ["NPI Score", "Quality Score Confidence"]
            rest_hosp  = ["Type", "NPI", "Presentation Name", "Address First Line", "Zip Code",
                          "City", "State", "County", "FIPS State County Code",
                          "Provider Type", "Specialty Name", "Status"]
            rest_ind   = ["Type", "NPI", "First Name", "Last Name", "Presentation Name",
                          "Zip Code", "City", "State", "County", "FIPS State County Code",
                          "Provider Type", "Specialty Name", "Status"]
            hosp_df_out = hosp_df.drop(columns=["Address First Line"], errors="ignore")

        col_seq_hosp = rest_hosp + payer_cols + hosp_score
        col_seq_ind  = rest_ind  + payer_cols + ind_score

        # ---- Hospital download file ----
        hosp_df_out = hosp_df_out.reindex(columns=col_seq_hosp)
        for col in ["Address First Line", "County", "Status"]:
            if col in hosp_df_out.columns:
                hosp_df_out[col] = hosp_df_out[col].astype(str).str.title()
        hosp_df_out.drop(columns=["Status"], inplace=True, errors="ignore")
        hosp_df_out.rename(columns={"Quality Score Confidence": "NPI Score Confidence"}, inplace=True)
        hosp_df_out["NPI"] = pd.to_numeric(hosp_df_out["NPI"], errors="coerce").astype("Int64")

        pl.from_pandas(hosp_df_out).write_csv(outputfile_path_hosp + str(id_parameter) + ".csv")
        pl.from_pandas(hosp_df_out).write_parquet(outputfile_path_hosp + str(id_parameter) + ".parquet")

        # ---- Individual download file ----
        ind_only = dl_ind_df[dl_ind_df["Provider Type"] != "Hospital"].copy()
        ind_only = ind_only.reindex(columns=col_seq_ind)

        for col in ["First Name", "Last Name", "Presentation Name", "County",
                    "Address First Line", "Location Confidence Address level", "Status"]:
            if col in ind_only.columns:
                ind_only[col] = ind_only[col].astype(str).str.title()

        ind_only.rename(columns={"Quality Score Confidence": "NPI Score Confidence"}, inplace=True)

        npi_to_npi_df = pd.concat([ind_only, hosp_df_out], ignore_index=True)

        if "Group TIN" in npi_to_npi_df.columns:
            npi_to_npi_df["Group TIN"] = (
                pd.to_numeric(npi_to_npi_df["Group TIN"], errors="coerce")
                .astype("Int64").astype("string").fillna("NA")
            )
        if "Group Name" in npi_to_npi_df.columns:
            npi_to_npi_df["Group Name"] = (
                npi_to_npi_df["Group Name"].astype(str).str.strip()
                .replace({"": "NA", "nan": "NA", "None": "NA", "NaN": "NA"})
            )
        if "Location Confidence County Level" in npi_to_npi_df.columns:
            npi_to_npi_df["Location Confidence County Level"] = (
                npi_to_npi_df["Location Confidence County Level"].str.title()
            )
        if "Specialty Flag" in npi_to_npi_df.columns:
            npi_to_npi_df["Specialty Flag"] = npi_to_npi_df["Specialty Flag"].fillna("Not Applicable")

        npi_to_npi_df = npi_to_npi_df.drop_duplicates()
        npi_to_npi_df.drop(columns=["Status"], inplace=True, errors="ignore")

        if clientId not in [8, 45, 5]:
            npi_to_npi_df.drop(columns=["Cost Score", "Quality Score"], inplace=True, errors="ignore")

        # Mixed-type normalisation before Polars (Zip Code str/int, NaN/str columns)
        for col in ["Zip Code", "First Name", "Last Name", "Specialty Flag", "Group Name", "Status"]:
            if col in npi_to_npi_df.columns:
                npi_to_npi_df[col] = (
                    npi_to_npi_df[col].astype(str)
                    .replace({"nan": "NA", "None": "NA", "": "NA"})
                )

        for col in ["NPI Score", "Cost Score", "Quality Score", "FFS Utilization Score",
                    "MA Utilization Score", "NPI Score Confidence"]:
            if col in npi_to_npi_df.columns:
                npi_to_npi_df[col] = npi_to_npi_df[col].fillna("NA").astype("string")

        npi_to_npi_df["NPI"] = pd.to_numeric(npi_to_npi_df["NPI"], errors="coerce").astype("Int64")
        npi_to_npi_df = npi_to_npi_df.sort_values("NPI", ascending=True)

        pl.from_pandas(npi_to_npi_df).write_csv(outputfile_path + str(id_parameter) + ".csv")
        pl.from_pandas(npi_to_npi_df).write_parquet(outputfile_path + str(id_parameter) + ".parquet")

        con.close()
        return 1

    except Exception:
        emit_error(traceback.format_exc())
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    if len(sys.argv) < 14:
        print(
            f"Usage: python provider_pipeline.py "
            "<countyId> <id_param> <filespath> <clientID> <userID> "
            "<base_json> <compare_json> <baseParentOrg> <compareParentOrg> "
            "<baseParentOrgId> <compareParentOrgId> <planType> <planTypeId>\n"
            f"Got {len(sys.argv) - 1} args, expected 13."
        )
        sys.exit(1)

    county_raw    = sys.argv[1]
    id_param      = sys.argv[2]
    filespath     = sys.argv[3]
    clientID      = int(sys.argv[4])
    userID        = sys.argv[5]
    base          = json.loads(sys.argv[6])
    compare       = json.loads(sys.argv[7])
    baseParentOrg = sys.argv[8]
    compareParentOrg  = sys.argv[9]
    baseParentOrgId   = sys.argv[10]
    compareParentOrgId = sys.argv[11]
    planType      = sys.argv[12]
    planTypeId    = sys.argv[13]

    county = [int(c.strip()) for c in county_raw.split(",")]

    status = run_pipeline(
        filespath=filespath, base=base, compare=compare,
        baseParentOrgId=baseParentOrgId, compareParentOrgId=compareParentOrgId,
        planTypeId=planTypeId, baseParentOrg=baseParentOrg, compareParentOrg=compareParentOrg,
        planType=planType, id_parameter=id_param,
        county=county, clientId=clientID, userId=userID,
    )

    if status == 1:
        print("Success")
    else:
        print("Failed")


if __name__ == "__main__":
    main()
