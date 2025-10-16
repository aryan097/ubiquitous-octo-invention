#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
C86 Client360 – Product Appropriateness (Python port of SAS job)
- Pulls source data from Teradata
- Reproduces volatile table, enrich joins, tool-usage link, AOT link
- Builds "autocomplete" and "detail" outputs (Excel), and a pivot-friendly workbook
- Creates all required directories on first run
- INI_RUN is forced to "N" for testing (rolling, week-based windows)
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, date, timedelta

import pandas as pd

# =========================
# 0) CONFIG / PATHS / LOGS
# =========================

# ---- Environment-style roots (portable defaults) ----
HOME = Path.home()
# Equivalent of SAS &regpath scaffolding:
#   <regpath>/c86/{log,out}/product_appropriateness/client360
REGPATH = Path(os.environ.get("REGPATH", HOME / "RSD" / "REG_DEV")).resolve()

LOGPATH = REGPATH / "c86" / "log" / "product_appropriateness" / "client360"
OUTPATH = REGPATH / "c86" / "out" / "product_appropriateness" / "client360"

# Create directories on a fresh machine
for p in [LOGPATH, OUTPATH]:
    p.mkdir(parents=True, exist_ok=True)

# Dated subfolder (SAS &runday)
RUNDAY = date.today().strftime("%Y%m%d")
OUTPATH_DATED = OUTPATH / RUNDAY
OUTPATH_DATED.mkdir(parents=True, exist_ok=True)

# Logging (file + console)
LOGFILE = LOGPATH / f"c86_pa_client360_{RUNDAY}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("c86_client360")

logger.info("=== C86 Client360 PA job: START ===")
logger.info("User ID: %s", os.environ.get("USER") or os.environ.get("USERNAME") or "unknown")
logger.info("Host: %s", os.uname().nodename if hasattr(os, "uname") else "unknown")
logger.info("REGPATH: %s", REGPATH)
logger.info("LOGPATH: %s", LOGPATH)
logger.info("OUTPATH: %s", OUTPATH)

# =========================
# 1) DB CONNECTION HELPER
# =========================

# Your provided helper (kept verbatim, with imports guarded)
from pathlib import Path as _Path
def get_teradata_conn(config_path: str = "TeradataConnection_T.json"):
    try:
        import teradatasql  # pip install teradatasql
    except Exception:
        logging.warning("teradatasql not available; returning None. Install with: pip install teradatasql")
        return None

    cfg = _Path(config_path)
    if not cfg.exists():
        logging.warning("Config file %s not found. Skipping DB fetch.", config_path)
        return None

    with open(cfg) as f:
        creds = json.load(f)

    try:
        conn = teradatasql.connect(
            host=creds["url"],
            user=creds["user"],
            password=creds["password"],
            logmech="LDAP",
        )
        return conn
    except Exception as e:
        logging.error("Teradata connection failed: %s", e)
        return None

# =========================
# 2) DATE WINDOWS (INI_RUN)
# =========================

# In SAS, ini_run='Y' uses fixed launch dates; ini_run='N' uses the rolling/current week.
# For testing: INI_RUN = 'N'
INI_RUN = "N"

today = date.today()

# SAS computes:
#   week_start      = intnx('week.4', today, 0, 'b') - 11;
#   week_end        = intnx('week.4', today, 0, 'b') - 5;
# Here we emulate Monday-based 'week.4' boundaries then apply offsets.
# Step 1: current Monday (beginning of the ISO week)
week_monday = today - timedelta(days=(today.weekday()))  # Monday = 0
week_start = week_monday - timedelta(days=11)            # minus 11 days
week_end   = week_monday - timedelta(days=5)             # minus 5 days

if INI_RUN.upper() == "Y":
    # Launch constants from SAS:
    # launch_dt='07MAY2023'd; launch_dt_min14='23APR2023'd
    wk_start = date(2023, 5, 7)
    wk_start_min14 = date(2023, 4, 23)
    wk_end = week_end
else:
    # Rolling
    wk_start = week_start
    wk_start_min14 = week_start - timedelta(days=14)
    wk_end = week_end

logger.info("Date window (INI_RUN=%s): wk_start=%s | wk_start_min14=%s | wk_end=%s",
            INI_RUN, wk_start, wk_start_min14, wk_end)

# =========================
# 3) SQL QUERIES
# =========================

# Notes:
# - We mirror the SAS flow:
#   1) tracking_all (DDWV01.EVNT_PROD_TRACK_LOG, filter Advice Tool & EVNT_DT > wk_start-90)
#   2) tracking distincts and counts (in pandas)
#   3) c360_short volatile table (emp_id, snap_dt from ddwv01.evnt_prod_oppor)
#   4) C360_detail_pre: join evnt_prod_oppor + emp + empl_reltn via snap date ranges
#   5) Join tool_used, AOT (ddwv01.evnt_prod_aot)
#   6) Transform to autocomplete (two RDE flavours) and detail outputs

def fetch_df(conn, sql, params=None):
    """Run SQL and return DataFrame."""
    import pandas as _pd
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    return _pd.DataFrame.from_records(rows, columns=cols)

def run_pipeline(config_json="TeradataConnection_T.json"):
    conn = get_teradata_conn(config_json)
    if conn is None:
        logger.warning("Connection unavailable. Running in 'no-db' mode; outputs will be empty schemas.")
        # Return empty frames with expected columns so downstream code doesn’t crash:
        return (pd.DataFrame(), pd.DataFrame())

    try:
        # 3.1 tracking_all
        logger.info("Pulling tracking_all ...")
        sql_tracking = f"""
            SELECT *
            FROM DDWV01.EVNT_PROD_TRACK_LOG
            WHERE ADVC_SALT_TYP = 'Advice Tool'
              AND EVNT_DT > DATE '{(wk_start - timedelta(days=90)).isoformat()}'
        """
        tracking_all = fetch_df(conn, sql_tracking)

        # tracking_tool_use_distinct
        tracking_tool_use_distinct = (
            tracking_all
            .loc[tracking_all["OPPOR_ID"].notna() & tracking_all["ADVC_TOOL_NM"].notna(), ["OPPOR_ID", "ADVC_TOOL_NM"]]
            .assign(ADVC_TOOL_NM=lambda df: df["ADVC_TOOL_NM"].str.upper())
            .drop_duplicates()
        )

        # tracking_count_tool_use_pre2
        tracking_count_tool_use_pre2 = (
            tracking_all
            .loc[tracking_all["OPPOR_ID"].notna(), ["OPPOR_ID", "ADVC_TOOL_NM"]]
            .assign(ADVC_TOOL_NM=lambda df: df["ADVC_TOOL_NM"].str.upper())
            .groupby("OPPOR_ID", as_index=False)["ADVC_TOOL_NM"].nunique()
            .rename(columns={"ADVC_TOOL_NM": "count_unique_tool_used"})
            .sort_values("count_unique_tool_used", ascending=False)
        )

        # tracking_tool_use
        tracking_tool_use = tracking_count_tool_use_pre2[["OPPOR_ID", "count_unique_tool_used"]].assign(
            TOOL_USED=lambda df: df["count_unique_tool_used"].gt(0).map({True: "Tool Used", False: None})
        )[["OPPOR_ID", "TOOL_USED"]]

        # 3.2 Build c360_short as a volatile table, then C360_detail_pre via joins
        logger.info("Creating volatile table c360_short ...")
        with conn.cursor() as cur:
            cur.execute("DELETE VOLATILE TABLE c360_short ALL;")  # ignore error if first run
            # Create and populate volatile table
            cur.execute(f"""
                CREATE MULTISET VOLATILE TABLE c360_short AS
                (
                  SELECT
                    evnt_id,
                    CAST(rbc_oppor_own_id AS INTEGER) AS emp_id,
                    evnt_dt AS snap_dt
                  FROM ddwv01.evnt_prod_oppor
                  WHERE rbc_oppor_own_id IS NOT NULL
                    AND evnt_dt IS NOT NULL
                    AND evnt_id IS NOT NULL
                    AND evnt_dt BETWEEN DATE '{wk_start.isoformat()}' AND DATE '{wk_end.isoformat()}'
                )
                WITH DATA PRIMARY INDEX (emp_id, snap_dt) ON COMMIT PRESERVE ROWS;
            """)
            cur.execute("COLLECT STATISTICS COLUMN (emp_id, snap_dt) ON c360_short;")

        # C360_detail_pre: emulating the SAS join with interval containment
        logger.info("Pulling C360_detail_pre via range joins ...")
        sql_detail = f"""
            SELECT
              c360.*,
              e1.org_unt_no,
              e1.hr_posn_titl_en,
              e2.posn_strt_dt,
              e2.posn_end_dt,
              e1.occpt_job_cd
            FROM ddwv01.evnt_prod_oppor AS c360
            LEFT JOIN
            (
                SELECT c3.evnt_id,
                       e1.org_unt_no, e1.hr_posn_titl_en, e2.posn_strt_dt, e2.posn_end_dt, e1.occpt_job_cd
                FROM c360_short AS c3
                INNER JOIN ddwv01.emp AS e1
                    ON c3.emp_id = e1.emp_id
                   AND c3.snap_dt >= e1.captr_dt
                   AND c3.snap_dt  < e1.chg_dt
                INNER JOIN ddwv01.empl_reltn AS e2
                    ON c3.emp_id = e2.emp_id
                   AND c3.snap_dt >= e2.captr_dt
                   AND c3.snap_dt  < e2.chg_dt
            ) AS emp
              ON emp.evnt_id = c360.evnt_id
            WHERE c360.evnt_id IS NOT NULL
              AND c360.evnt_dt BETWEEN DATE '{wk_start.isoformat()}' AND DATE '{wk_end.isoformat()}'
        """
        c360_detail_pre = fetch_df(conn, sql_detail)

        # 3.3 Bring in TOOL_USED flag
        logger.info("Joining tool_used ...")
        c360_detail = (
            c360_detail_pre
            .merge(tracking_tool_use, how="left", on="OPPOR_ID")
            .assign(TOOL_USED=lambda df: df["TOOL_USED"].fillna("Tool Not Used"))
        )

        # 3.4 AOT linkage set
        logger.info("Pulling AOT counts ...")
        sql_aot = f"""
            SELECT oppor_id, COUNT(*) AS count_aot
            FROM ddwv01.evnt_prod_aot
            WHERE ess_src_evnt_dt BETWEEN DATE '{wk_start_min14.isoformat()}'
                                     AND DATE '{wk_end.isoformat()}'
              AND oppor_id IS NOT NULL
            GROUP BY 1
        """
        aot_all_oppor = fetch_df(conn, sql_aot)
        aot_unique = aot_all_oppor[["OPPOR_ID"]].drop_duplicates()

        c360_detail_link_aot = (
            c360_detail
            .merge(aot_unique.rename(columns={"OPPOR_ID": "aot_oppor_id"}), how="left", left_on="OPPOR_ID", right_on="aot_oppor_id")
        )
        c360_detail_link_aot["C360_PDA_link_AOT"] = (
            (c360_detail_link_aot["PROD_CATG_NM"] == "Personal Accounts") &
            (c360_detail_link_aot["aot_oppor_id"].notna())
        ).astype(int)

        # 3.5 Filters and level_oppor equivalent (remove dup by OPPOR_ID keeping first)
        tmp0 = c360_detail_link_aot.sort_values(["OPPOR_ID"]).copy()
        tmp0["level_oppor"] = tmp0.groupby("OPPOR_ID").cumcount() + 1

        tmp_pa_c360_4ac = tmp0.loc[tmp0["level_oppor"] == 1].copy()

        # 3.6 PA rationale validity (mirror SAS logic)
        def pa_validity(row):
            txt = row.get("CLNT_RTNL_TXT")
            if pd.isna(txt):
                txt = ""
            x = str(txt)
            # Normalize whitespace similar to SAS translate/compress:
            x = " ".join(x.split())  # uniform spaces
            up = x.strip().upper()

            # (1) > 5 characters
            cond1 = len(up) > 5

            # (2) not single repeated char
            # Remove all occurrences of the first char; if anything remains, it's not all repeated
            if up:
                x2 = up.replace(up[0], "")
            else:
                x2 = up
            cond2 = (x2 != "")

            # (3) at least 2 alnum
            alnum = "".join(ch for ch in up if ch.isalnum())
            cond3 = len(alnum) >= 2

            return "Valid" if (cond1 and cond2 and cond3) else "Invalid"

        # For detail & autocomplete we need segment fields and dates
        def week_ending_sunday(d):
            # SAS: intnx('week.7', evnt_dt, 0, 'e') – week ending Sunday
            if pd.isna(d):
                return pd.NaT
            d = pd.to_datetime(d).normalize()
            # move to end of week Sunday
            return (d.to_period('W-SUN').end_time.normalize())

        # Prepare tmp_pa_c360_4ac_count_pre (joining distinct tool names)
        tmp_pa_c360_4ac_count_pre = (
            tmp_pa_c360_4ac
            .merge(tracking_tool_use_distinct.assign(ADVC_TOOL_NM=lambda df: df["ADVC_TOOL_NM"].str.upper()),
                   how="left", on="OPPOR_ID")
        )

        # Common transforms (autocomplete COUNT_Tool and Tool_Use variants)
        for df in [tmp_pa_c360_4ac, tmp_pa_c360_4ac_count_pre]:
            df["RegulatoryName"] = "C86"
            df["LOB"] = "Retail"
            df["ReportName"] = "C86 Client360 Product Appropriateness"
            df["ControlRisk"] = "Completeness"
            df["TestType"] = "Anomaly"
            df["TestPeriod"] = "Origination"
            df["ProductType"] = df.get("PROD_CATG_NM")
            df["segment"] = "Account Open"
            df["segment2"] = df.get("ASCT_PROD_FMLY_NM")
            df["segment3"] = df.get("PROD_SRVC_NM")
            df["segment6"] = df.get("OPPOR_STAGE_NM")
            df["segment7"] = df.get("TOOL_USED")
            # Month "yymmn6." → YYYYMM
            df["segment10"] = pd.to_datetime(df["EVNT_DT"]).dt.strftime("%Y%m")
            df["CommentCode"] = "COM13"
            df["Comments"] = "Population Distribution"
            df["HoldoutFlag"] = "N"
            df["SnapDate"] = pd.to_datetime(df["EVNT_DT"]).apply(week_ending_sunday)
            df["DateCompleted"] = pd.Timestamp.today().normalize()
            # PA rationale validity only for "Not Appropriate - Rationale"
            mask_rationale = df.get("IS_PROD_APRP_FOR_CLNT") == "Not Appropriate - Rationale"
            df["prod_not_aprp_rtnl_txt_cat"] = None
            df.loc[mask_rationale, "prod_not_aprp_rtnl_txt_cat"] = df.loc[mask_rationale].apply(pa_validity, axis=1)

        # Build "autocomplete – Tool" (COUNT_Tool)
        tmp_pa_c360_ac_count_assessment = (
            tmp_pa_c360_4ac_count_pre
            .assign(
                segment4=lambda d: d["IS_PROD_APRP_FOR_CLNT"].map({
                    "Not Appropriate - Rationale": "Product Not Appropriate",
                    "Client declined product appropriateness assessment": "Client declined product appropriateness assessment",
                    "Product Appropriate": "Product Appropriate",
                    "Product Appropriateness assessed outside Client 360": "Product Appropriateness assessed outside of Client360",
                }).fillna("Missing"),
                RDE="PA003_Client360_Completeness_Tool",
                segment8=lambda d: d["ADVC_TOOL_NM"]
            )
        )

        # Grouped (sum Volume)
        group_cols_tool = [
            "RegulatoryName","LOB","ReportName","ControlRisk","TestType","TestPeriod",
            "ProductType","RDE","segment","segment2","segment3","segment4","prod_not_aprp_rtnl_txt_cat",
            "segment6","segment7","segment8","segment10","HoldoutFlag","CommentCode","Comments","DateCompleted","SnapDate"
        ]
        pa_c360_autocomplete_Count_Tool = (
            tmp_pa_c360_ac_count_assessment
            .groupby(group_cols_tool, dropna=False, as_index=False)
            .agg(Volume=("EVNT_ID", "size"))
            .assign(Amount=pd.NA)
        )

        # Build "autocomplete – Use" (no segment8)
        tmp_pa_c360_ac_use = (
            tmp_pa_c360_4ac
            .assign(
                segment4=lambda d: d["IS_PROD_APRP_FOR_CLNT"].map({
                    "Product Appropriateness assessed outside Client 360": "Product Appropriateness assessed outside of Client360",
                    "Not Appropriate - Rationale": "Product Not Appropriate",
                    "Client declined product appropriateness assessment": "Client declined product appropriateness assessment",
                    "Product Appropriate": "Product Appropriate",
                }).fillna("Missing"),
                RDE="PA003_Client360_Completeness_RDE"
            )
        )
        group_cols_use = [
            "RegulatoryName","LOB","ReportName","ControlRisk","TestType","TestPeriod",
            "ProductType","RDE","segment","segment2","segment3","segment4","prod_not_aprp_rtnl_txt_cat",
            "segment6","segment7","segment10","HoldoutFlag","CommentCode","Comments","DateCompleted","SnapDate"
        ]
        pa_c360_autocomplete_tool_use = (
            tmp_pa_c360_ac_use
            .groupby(group_cols_use, dropna=False, as_index=False)
            .agg(Volume=("EVNT_ID", "size"))
            .assign(Amount=pd.NA)
        )

        # Combine autocomplete variants (like SAS set …)
        autocomplete = pd.concat(
            [pa_c360_autocomplete_Count_Tool, pa_c360_autocomplete_tool_use],
            ignore_index=True
        )

        # Detail file (subset of columns, filtered on PA result)
        def map_pa_result(val: str) -> str:
            mapping = {
                "Product Appropriateness assessed outside Client 360": "Product Appropriateness assessed outside Client 360",
                "Not Appropriate - Rationale": "Product Not Appropriate",
                "Client declined product appropriateness assessment": "Client declined product appropriateness assessment",
                "Product Appropriate": "Product Appropriate",
            }
            return mapping.get(val, "Missing")

        detail_pre = tmp_pa_c360_4ac_count_pre.copy()
        detail_pre["PA_result"] = detail_pre["IS_PROD_APRP_FOR_CLNT"].map(map_pa_result)
        detail_pre["PA_rationale_validity"] = detail_pre["prod_not_aprp_rtnl_txt_cat"]

        detail = detail_pre.loc[
            detail_pre["PA_result"].isin(
                ["Product Not Appropriate", "Missing", "Product Appropriateness assessed outside Client 360"]
            ),
            :
        ].copy()

        # Final detail column set and formatting (approximate SAS formats)
        detail_out = pd.DataFrame({
            "event_month": pd.to_datetime(detail["EVNT_DT"]).dt.strftime("%Y%m"),
            "reporting_date": pd.Timestamp.today().strftime("%m/%d/%Y"),
            "event_week_ending": detail["EVNT_DT"].apply(week_ending_sunday).dt.strftime("%m/%d/%Y"),
            "event_date": pd.to_datetime(detail["EVNT_DT"]).dt.strftime("%m/%d/%Y"),
            "event_timestamp": pd.to_datetime(detail["EVNT_TMSTMP"]).dt.strftime("%m/%d/%Y %I:%M:%S %p"),
            "opportunity_id": detail["OPPOR_ID"],
            "opportunity_type": detail.get("OPPOR_REC_TYP"),
            "product_code": detail.get("PROD_CD"),
            "product_category_name": detail.get("PROD_CATG_NM"),
            "product_family_name": detail.get("ASCT_PROD_FMLY_NM"),
            "product_name": detail.get("PROD_SRVC_NM"),
            "oppor_stage_nm": detail.get("OPPOR_STAGE_NM"),
            "tool_used": detail.get("TOOL_USED"),
            "tool_nm": detail.get("ADVC_TOOL_NM"),
            "PA_result": detail["PA_result"],
            "PA_rationale": detail.get("CLNT_RTNL_TXT"),
            "PA_rationale_validity": detail.get("PA_rationale_validity"),
            "employee_id": detail.get("RBC_OPPOR_OWN_ID"),
            "job_code": detail.get("OCCPT_JOB_CD"),
            "position_title": detail.get("HR_POSN_TITL_EN"),
            "employee_transit": detail.get("ORG_UNT_NO"),
            "position_start_date": pd.to_datetime(detail.get("POSN_STRT_DT")).dt.strftime("%m/%d/%Y"),
        })

        # 4) SAVE OUTPUTS
        auto_xlsx = OUTPATH / "pa_client360_autocomplete.xlsx"   # SAS also writes a non-dated AC
        detail_xlsx = OUTPATH_DATED / f"pa_client360_detail_{RUNDAY}.xlsx"
        pivot_xlsx = OUTPATH / "PA_Client360_Pivot.xlsx"

        logger.info("Writing: %s", auto_xlsx)
        with pd.ExcelWriter(auto_xlsx, engine="xlsxwriter") as w:
            autocomplete.to_excel(w, sheet_name="autocomplete", index=False)

        logger.info("Writing: %s", detail_xlsx)
        with pd.ExcelWriter(detail_xlsx, engine="xlsxwriter") as w:
            detail_out.to_excel(w, sheet_name="detail", index=False)

        # For pivot-friendly workbook, mirror SAS behavior: write AC again as 'Autocomplete'
        logger.info("Writing: %s", pivot_xlsx)
        with pd.ExcelWriter(pivot_xlsx, engine="xlsxwriter") as w:
            autocomplete.to_excel(w, sheet_name="Autocomplete", index=False)

        return (autocomplete, detail_out)

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        ac_df, detail_df = run_pipeline("TeradataConnection_T.json")
        logger.info("Rows -> autocomplete: %s | detail: %s", len(ac_df), len(detail_df))
        logger.info("=== C86 Client360 PA job: END ===")
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        sys.exit(2)
