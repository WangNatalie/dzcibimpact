#!/usr/bin/env python3
"""
Compute sum(field * area / 10000) AND total area from a Supabase table
using the Postgres Session Pooler.

Usage:

    export SUPABASE_URL="postgres://user:pass@host:port/dbname"

    python compute_weighted_sum_pool.py \
        --table my_table \
        --field my_value_field \
        --area-field area_m2
"""

import os
import argparse
from psycopg import connect
from psycopg import sql
from dotenv import load_dotenv

load_dotenv()


def compute_weighted_sum_and_area(
    conninfo: str,
    table_name: str,
    value_field: str,
    area_field: str,
) -> tuple[float, float]:
    with connect(conninfo) as conn:
        with conn.cursor() as cur:
            query = sql.SQL(
                """
                SELECT
                    SUM({value} * {area} / 10000.0) AS weighted_sum,
                    SUM({area}) AS total_area
                FROM {table}
                """
            ).format(
                value=sql.Identifier(value_field),
                area=sql.Identifier(area_field),
                table=sql.Identifier(table_name),
            )

            cur.execute(query)
            result = cur.fetchone()

            weighted_sum = result[0] if result and result[0] is not None else 0.0
            total_area = result[1] if result and result[1] is not None else 0.0

    return weighted_sum, total_area


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Query supabase tables"
        )
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Name of the table.",
    )
    parser.add_argument(
        "--field",
        required=True,
        help="Name of the numeric field to multiply by area.",
    )
    parser.add_argument(
        "--area-field",
        default="area_m2",
        help="Name of the area field (default: 'area_m2').",
    )

    parser.add_argument(
        "--conversion-factor",
        default=1,
        help="Conversion factor for the value field (default: 0.0001).",
    )

    args = parser.parse_args()

    # Using SUPABASE_URL here because that's what you're loading from .env
    conninfo = os.getenv("SUPABASE_URL")
    if not conninfo:
        raise RuntimeError("SUPABASE_URL environment variable is not set.")

    weighted_total, total_area = compute_weighted_sum_and_area(
        conninfo=conninfo,
        table_name=args.table,
        value_field=args.field,
        area_field=args.area_field,
    )

    # First: the weighted money total
    print(
        f"Sum of {args.field} over {total_area/10000:.2f} hectares of opportunity is {weighted_total * args.conversion_factor:.2f}"
    )

if __name__ == "__main__":
    main()
