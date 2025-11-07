"""
EPL Standings ETL Pipeline
Fetches EPL standings from API and loads into PostgreSQL
"""

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import UniqueConstraint
from dotenv import load_dotenv
import os
import json
import requests
import pandas as pd
from datetime import datetime


def run_etl():
    """Main ETL function to fetch standings and load to PostgreSQL"""

    print(f"[{datetime.now()}] Starting ETL pipeline...")

    # Load environment variables
    load_dotenv()

    API_KEY = os.getenv("API_KEY")
    API_HOST = os.getenv("API_HOST")
    SEASON = 2023
    LEAGUE = 39

    # ============================================
    # 1. EXTRACT: Fetch data from API
    # ============================================
    print("Step 1: Extracting data from API...")

    url = f"{API_HOST}/standings"
    headers = {"x-apisports-key": API_KEY}
    params = {"league": "39", "season": SEASON}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        standings = data["response"][0]["league"]["standings"][0]
        print(f"Successfully fetched data for {len(standings)} teams")

    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return False

    # ============================================
    # 2. TRANSFORM: Parse response into DataFrame
    # ============================================
    print("Step 2: Transforming data...")

    rows = []
    columns = [
        "season",
        "position",
        "team_id",
        "team_name",
        "team_logo",
        "played",
        "won",
        "draw",
        "lose",
        "goals_for",
        "goals_against",
        "goal_difference",
        "points",
        "form",
        "description",
    ]

    for club in standings:
        try:
            season = SEASON
            position = club["rank"]
            team_id = club["team"]["id"]
            team_name = club["team"]["name"]
            team_logo = club["team"]["logo"]
            played = club["all"]["played"]
            won = club["all"]["win"]
            draw = club["all"]["draw"]
            lose = club["all"]["lose"]
            goals_for = club["all"]["goals"]["for"]
            goals_against = club["all"]["goals"]["against"]
            goal_difference = club["goalsDiff"]
            points = club["points"]
            form = club["form"]
            description = club["description"]

            active_rows = (
                season,
                position,
                team_id,
                team_name,
                team_logo,
                played,
                won,
                draw,
                lose,
                goals_for,
                goals_against,
                goal_difference,
                points,
                form,
                description,
            )
            rows.append(active_rows)

        except KeyError as e:
            print(f"Missing key in data structure: {e}")
            print(f"Problem record: {club}")

    df = pd.DataFrame(rows, columns=columns)
    df["description"] = df["description"].fillna("EPL: Next Season")

    print(f"Transformed {len(df)} rows successfully")

    # ============================================
    # 3. LOAD: Upload data into PostgreSQL
    # ============================================
    print("Step 3: Loading data to PostgreSQL...")

    # DB configuration
    DB_CONFIG = {
        "host": os.getenv("HOST"),
        "port": os.getenv("PORT"),
        "database": os.getenv("DB"),
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
    }

    try:
        # Connect to PostgreSQL server
        admin_engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}",
            isolation_level="AUTOCOMMIT",
        )

        # Create DB if it doesn't exist
        with admin_engine.connect() as conn:
            try:
                conn.execute(text(f"CREATE DATABASE {DB_CONFIG['database']}"))
                print(f"Database {DB_CONFIG['database']} created successfully.")
            except ProgrammingError as e:
                if "already exists" in str(e):
                    print(f"Database {DB_CONFIG['database']} already exists.")
                else:
                    raise e

        # Connect to the target DB
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
            echo=False,
            pool_timeout=10,
        )

        # Define table metadata
        metadata = MetaData()

        standings_table = Table(
            "standings",
            metadata,
            Column("season", Integer, primary_key=True, nullable=False),
            Column("position", Integer, nullable=False),
            Column("team_id", Integer, primary_key=True, nullable=False),
            Column("team_name", String(100), nullable=False),
            Column("team_logo", String(100), nullable=False),
            Column("played", Integer, nullable=False),
            Column("won", Integer, nullable=False),
            Column("draw", Integer, nullable=False),
            Column("lose", Integer, nullable=False),
            Column("goals_for", Integer, nullable=False),
            Column("goals_against", Integer, nullable=False),
            Column("goal_difference", Integer, nullable=False),
            Column("points", Integer, nullable=False),
            Column("form", String(5), nullable=False),
            Column(
                "description", String(100), default="EPL: Next Season", nullable=False
            ),
            UniqueConstraint("season", "position", name="uix_season_position"),
        )

        # Create table if it doesn't exist
        metadata.create_all(engine)
        print("Table 'standings' created/verified successfully.")

        # Convert DataFrame to list of dictionaries
        df_rows = df.to_dict(orient="records")

        stmt = insert(standings_table)

        # Upsert statement
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["season", "team_id"],
            set_={
                c.name: c for c in stmt.excluded if c.name not in ["season", "team_id"]
            },
        )

        # Upsert data in bulk into the table
        with engine.begin() as conn:
            conn.execute(upsert_stmt, df_rows)
            print("Data upserted successfully.")

        print(f"[{datetime.now()}] ETL pipeline completed successfully!")
        return True

    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        return False


if __name__ == "__main__":
    run_etl()
