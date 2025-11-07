# Premier League Standings ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline that fetches English Premier League standings from a REST API and loads the data into PostgreSQL. Built to demonstrate modern data engineering practices and Python data pipeline development.

## Project Overview

**Data Source**: [API-Football](https://dashboard.api-football.com/) via API Sports
**Tech Stack**: Python, SQLAlchemy, Pandas, PostgreSQL
**Pipeline Type**: RESTful API → Python ETL → Relational Database

```
API-Football → Extract (Python) → Transform (Pandas) → Load (PostgreSQL)
```

---

## The Why

As a data professional, I wanted to build a real-world ETL pipeline that demonstrates:

- **API Integration**: Working with external REST APIs and handling authentication
- **Data Transformation**: Parsing nested JSON and normalizing into structured tables
- **Database Operations**: Creating schemas, implementing upserts, and managing transactions
- **Production Readiness**: Error handling, data quality checks, and idempotent operations

While there are many ways to track football standings, this project focuses on the **technical implementation** of a reliable data pipeline that could scale to support analytics dashboards, mobile apps, or data science workflows.

---

## What This Pipeline Does

1. **Extracts** current Premier League standings from API-Football
2. **Transforms** nested JSON into a clean, normalized pandas DataFrame
3. **Loads** data into PostgreSQL using upsert logic (no duplicates)
4. **Validates** data quality throughout the process

### Key Technical Features

- **Idempotent upserts**: Re-running the pipeline won't create duplicates
- **Composite primary keys**: Season + Team ID ensure data integrity
- **Error handling**: Graceful failures with detailed logging
- **Environment-based config**: Secure credential management with `.env`
- **Schema-first approach**: Strongly-typed database schema with constraints

---

## Pipeline Architecture

### Data Model

The pipeline extracts the following fields for each team:

| Field | Type | Description |
|-------|------|-------------|
| `season` | Integer | Season year (e.g., 2023 = 2023/24) |
| `position` | Integer | Current league position (1-20) |
| `team_id` | Integer | Unique team identifier |
| `team_name` | String | Team name |
| `played` | Integer | Matches played |
| `won/draw/lose` | Integer | Match outcomes |
| `goals_for/against` | Integer | Goals scored/conceded |
| `goal_difference` | Integer | Goal differential |
| `points` | Integer | Total points |
| `form` | String | Last 5 matches (e.g., "WWDLW") |
| `description` | String | Qualification status |

### Database Schema

```sql
CREATE TABLE standings (
    season INT NOT NULL,
    team_id INT NOT NULL,
    position INT NOT NULL,
    team_name VARCHAR(100) NOT NULL,
    -- [other columns...]
    PRIMARY KEY (season, team_id),
    UNIQUE (season, position)
);
```

---

## Setup Instructions

### Prerequisites

- Python 3.8+
- PostgreSQL 12+
- API-Football key from [API-Sports](https://dashboard.api-football.com/)

### Installation

1. **Clone the repository**

```bash
git clone https://github.com/msomali/epl-standings.git
cd epl-standings
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Configure environment variables**

Create a `.env` file in the project root:

```env
# API Configuration
API_KEY=your_rapidapi_key
API_HOST=https://v3.football.api-sports.io/standings

# Database Configuration
HOST=localhost
PORT=5432
DB=epl_database
USER=your_postgres_user
PASSWORD=your_postgres_password
```

4. **Run the pipeline**

**Option 1: Jupyter Notebook**

```bash
jupyter notebook epl.ipynb
```

**Option 2: Python Script**

```bash
python epl_etl.py
```

---

## Data Quality Checks

The pipeline includes validation at each stage:

- **Extract**: HTTP status validation, response structure checks
- **Transform**:
  - Verify 20 teams are present
  - Check for missing required fields
  - Validate data types
- **Load**:
  - Unique constraint enforcement
  - Transaction rollback on errors
  - Verification queries post-load

---

## Project Structure

```
epl-standings/
├── epl.ipynb                  # Jupyter notebook (exploration)
├── epl_etl.py                 # Production ETL script
├── requirements.txt           # Python dependencies
├── .env                       # Configuration (not in git)
├── .gitignore
├── logs/                      # Pipeline logs
└── README.md                 # This file
```

---

## Future Enhancements

- [ ] Add historical season tracking (multi-season support)
- [ ] Implement data quality monitoring with Great Expectations
- [ ] Create visualization dashboard (Streamlit/Plotly)
- [ ] Add unit tests and integration tests
- [ ] Containerize with Docker
- [ ] Add CI/CD pipeline with GitHub Actions
- [ ] Implement incremental loads vs full refreshes
- [ ] Add alerting for pipeline failures

---

## Technical Skills Demonstrated

- REST API integration and authentication
- JSON data parsing and transformation
- Pandas for data manipulation
- SQLAlchemy for database abstraction
- PostgreSQL schema design and constraints
- Upsert operations (INSERT ... ON CONFLICT)
- Environment-based configuration
- Error handling and logging
- ETL pipeline orchestration

---

## Learning Resources

If you're building something similar, these resources helped me:

- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [API-Football Docs](https://www.api-football.com/documentation-v3)
- [PostgreSQL Upsert (ON CONFLICT)](https://www.postgresql.org/docs/current/sql-insert.html)
- [Pandas DataFrame Guide](https://pandas.pydata.org/docs/user_guide/index.html)

---

## Contributing

This is a learning project, but suggestions are welcome! Feel free to:

- Open an issue for bugs or questions
- Submit a PR for improvements
- Share how you've adapted this for your own projects

---

## License

MIT License - feel free to use this code for your own learning and projects.

---

## Contact

Built by [Your Name]
[GitHub](https://github.com/msomali) • [LinkedIn](https://linkedin.com/in/walidaak)
