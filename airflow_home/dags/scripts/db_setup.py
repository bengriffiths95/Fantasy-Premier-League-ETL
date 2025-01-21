def seed_prod_db(engine):
    with engine.connect() as conn:
        conn.execute(
            f"DROP TABLE IF EXISTS fact_players, dim_players, dim_teams, dim_fixtures"
        )
        conn.execute(
            f"CREATE TABLE fact_players ( player_id int, team_id int, gameweek_id int, fixture_id int, opposition_team_id int, fixture_difficulty_rating int, is_home bool )"
        )
        conn.execute(
            f"CREATE TABLE dim_players ( first_name varchar(255), second_name varchar(255), web_name varchar(255), player_id int, team_id int )"
        )
        conn.execute(
            f"CREATE TABLE dim_teams ( team_id int, team_name varchar(255), team_name_short varchar(255) )"
        )
        conn.execute(
            f"CREATE TABLE dim_fixtures ( fixture_id int, gameweek_id int, fixture_date DATE, fixture_time TIME, match_finished bool, home_team_id int, away_team_id int, home_team_score int, away_team_score int, home_team_difficulty int, away_team_difficulty int )"
        )


def seed_test_db(engine):
    with engine.connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS test_table")
        conn.execute(
            f"CREATE TABLE test_table ( player_id int, team_id int, gameweek_id int, fixture_id int, opposition_team_id int, fixture_difficulty_rating int, is_home bool )"
        )
