import logging
import os

import pandas as pd
from sqlalchemy import create_engine

from runtime_support import load_project_dotenv, resolve_repo_path

logger = logging.getLogger(__name__)

load_project_dotenv()

DEFAULT_SOLRIS_LOOKUP_CSV = resolve_repo_path("data/solris_lookup.csv")
DEFAULT_WATER_LOOKUP_CSV = resolve_repo_path("data/water_filtration_lookup.csv")


def supabase_engine(required: bool = False):
    supabase_url = os.getenv("SUPABASE_URL")
    if not supabase_url:
        if required:
            raise RuntimeError("SUPABASE_URL is not set in .env")
        return None
    conn_str = supabase_url.replace("postgres://", "postgresql://", 1)
    return create_engine(conn_str)


def normalize_water_filtration_df(wf_df: pd.DataFrame) -> pd.DataFrame:
    return wf_df.rename(
        columns={"wetland_type": "solris_class", "value": "wf_value_per_ha"}
    )


def load_lookup_tables(
    prefer_supabase: bool = True,
    require_supabase: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    if prefer_supabase:
        engine = supabase_engine(required=require_supabase)
        if engine is not None:
            try:
                solris_df = pd.read_sql("SELECT * FROM solris_lookup", engine)
                wf_df = pd.read_sql("SELECT * FROM water_filtration_lookup", engine)
                return solris_df, normalize_water_filtration_df(wf_df), "Supabase"
            except Exception as exc:
                if require_supabase:
                    raise
                logger.warning(
                    "Failed to load lookup tables from Supabase (%s); falling back to local CSV files.",
                    exc,
                )
            finally:
                engine.dispose()

    if not DEFAULT_SOLRIS_LOOKUP_CSV.exists():
        raise FileNotFoundError(
            f"SOLRIS lookup CSV not found: {DEFAULT_SOLRIS_LOOKUP_CSV}"
        )
    if not DEFAULT_WATER_LOOKUP_CSV.exists():
        raise FileNotFoundError(
            f"Water filtration lookup CSV not found: {DEFAULT_WATER_LOOKUP_CSV}"
        )

    solris_df = pd.read_csv(DEFAULT_SOLRIS_LOOKUP_CSV)
    wf_df = normalize_water_filtration_df(pd.read_csv(DEFAULT_WATER_LOOKUP_CSV))
    return solris_df, wf_df, "local CSV files"


def build_lookup_dict(solris_df: pd.DataFrame, wf_df: pd.DataFrame) -> dict:
    solris_df = solris_df.copy()
    solris_df = solris_df.dropna(subset=["solris_code"])
    solris_df["solris_code"] = solris_df["solris_code"].astype(int)

    carbon_cols = ("agc_tc_ha", "bgc_tc_ha", "soc_tc_ha", "deoc_tc_ha")
    for col in carbon_cols:
        if col not in solris_df.columns:
            solris_df[col] = 0.0
        solris_df[col] = pd.to_numeric(solris_df[col], errors="coerce").fillna(0.0)

    solris_df["total_c_per_ha"] = solris_df[list(carbon_cols)].sum(axis=1)

    wf_df = normalize_water_filtration_df(wf_df.copy())
    if "solris_class" in wf_df.columns:
        if "wf_value_per_ha" not in wf_df.columns:
            wf_df["wf_value_per_ha"] = 0.0
        wf_df["wf_value_per_ha"] = pd.to_numeric(
            wf_df["wf_value_per_ha"], errors="coerce"
        ).fillna(0.0)
        solris_df = solris_df.merge(
            wf_df[["solris_class", "wf_value_per_ha"]],
            on="solris_class",
            how="left",
        )
    else:
        solris_df["wf_value_per_ha"] = 0.0

    solris_df["wf_value_per_ha"] = solris_df["wf_value_per_ha"].fillna(0.0)

    def _coerce(val):
        if pd.isna(val):
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return val

    lookup = {}
    for _, row in solris_df.iterrows():
        code = int(row["solris_code"])
        lookup[code] = {
            col: _coerce(row[col]) for col in solris_df.columns if col != "solris_code"
        }
    return lookup


def load_lookup_dict(
    prefer_supabase: bool = True,
    require_supabase: bool = False,
) -> tuple[dict, str]:
    solris_df, wf_df, source = load_lookup_tables(
        prefer_supabase=prefer_supabase,
        require_supabase=require_supabase,
    )
    return build_lookup_dict(solris_df, wf_df), source
