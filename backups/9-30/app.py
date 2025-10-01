from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_cors import CORS
import sqlite3
import os
import json
# Try to import PostgreSQL adapter for production
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None


app = Flask(__name__, static_folder='frontend', static_url_path='', template_folder='frontend')
# Enable CORS for frontend integration, including file:// protocol
CORS(app, origins=['http://localhost:5000', 'http://127.0.0.1:5000', 'null'], 
     supports_credentials=True, 
     allow_headers=['Content-Type', 'Authorization'],
     methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'])

# --- TEAM BATTING AND PITCHING ENDPOINTS ---

def get_db_connection():
    """Create a database connection - PostgreSQL in production, SQLite locally"""
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url and psycopg2:
        # Production PostgreSQL
        conn = psycopg2.connect(database_url)
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        return conn
    else:
        # Local SQLite
        conn = sqlite3.connect('yakyuu.db')
        conn.row_factory = sqlite3.Row
        return conn

def format_innings_pitched(ip_decimal):
    """Convert decimal innings pitched to baseball standard format (e.g., 123.33 -> 123.1)"""
    if ip_decimal is None:
        return '0.0'
    
    # Convert to float if it's not already
    ip_float = float(ip_decimal)
    
    # Get whole innings
    whole_innings = int(ip_float)
    
    # Get fractional part and convert to outs
    fractional_part = ip_float - whole_innings
    outs = round(fractional_part * 3)
    
    # Handle case where rounding gives us 3 outs (should be next inning)
    if outs >= 3:
        whole_innings += 1
        outs = 0
    
    # Format as baseball standard
    if outs == 0:
        return f"{whole_innings}.0"
    else:
        return f"{whole_innings}.{outs}"

@app.route('/api/teams/<team_id>/batting')
def get_team_batting_stats(team_id):
    """Get comprehensive batting statistics for a team with filtering"""
    game_types = request.args.getlist('game_types[]')
    splits = request.args.getlist('splits[]')
    if not splits:
        splits = [request.args.get('split', 'overall')]
    if not game_types:
        game_types = None
    conn = get_db_connection()
    try:
        base_query = """
            FROM batting b
            JOIN games g ON b.game_id = g.game_id
            WHERE b.team = ?
        """
        params = [team_id]
        if game_types and len(game_types) > 0:
            placeholders = ','.join(['?' for _ in game_types])
            base_query += f" AND g.gametype IN ({placeholders})"
            params.extend(game_types)
        if splits and 'overall' not in [s.lower() for s in splits]:
            for split in splits:
                split_lower = split.lower()
                if split_lower in ['home', 'road']:
                    if split_lower == 'home':
                        base_query += " AND b.team = g.home_team_id"
                    else:
                        base_query += " AND b.team = g.away_team_id"
                elif split_lower in ['wins', 'losses']:
                    if split_lower == 'wins':
                        base_query += " AND b.team = g.winning_team_id"
                    else:
                        base_query += " AND b.team = g.losing_team_id"
        # Use only columns that exist in your schema
        career_query = f"""
            SELECT 
                COUNT(DISTINCT b.game_id) as g,
                SUM(b.pa) as pa,
                SUM(b.ab) as ab,
                SUM(b.b_h) as h,
                SUM(b.b_r) as r,
                SUM(b.b_2b) as doubles,
                SUM(b.b_3b) as triples,
                SUM(b.b_hr) as hr,
                SUM(b.b_rbi) as rbi,
                SUM(b.b_k) as so,
                SUM(b.b_bb) as bb,
                SUM(b.b_hbp) as hbp,
                SUM(b.b_sac) as sac,
                SUM(b.b_gdp) as gidp,
                SUM(b.b_roe) as roe,
                ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
                ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) + CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as ops,
                SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) as tb,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
                ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip
            {base_query}
        """
        season_query = f"""
            SELECT 
                g.season,
                COUNT(DISTINCT b.game_id) as g,
                SUM(b.pa) as pa,
                SUM(b.ab) as ab,
                SUM(b.b_h) as h,
                SUM(b.b_r) as r,
                SUM(b.b_2b) as doubles,
                SUM(b.b_3b) as triples,
                SUM(b.b_hr) as hr,
                SUM(b.b_rbi) as rbi,
                SUM(b.b_k) as so,
                SUM(b.b_bb) as bb,
                SUM(b.b_hbp) as hbp,
                SUM(b.b_sac) as sac,
                SUM(b.b_gdp) as gidp,
                SUM(b.b_roe) as roe,
                ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
                ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) + CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as ops,
                SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) as tb,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
                ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip
            {base_query}
            GROUP BY g.season
            ORDER BY g.season ASC
        """
        cur = conn.execute(career_query, params)
        career = cur.fetchone()
        cur = conn.execute(season_query, params)
        seasons = [dict(row) for row in cur.fetchall()]
        return jsonify({
            'seasons': seasons,
            'career': dict(career) if career else {}
        })
    except Exception as e:
        print(f"Team batting stats error: {e}")
        return jsonify({'error': 'Failed to get team batting stats'}), 500
    finally:
        conn.close()


@app.route('/api/teams/<team_id>/pitching')
def get_team_pitching_stats(team_id):
    """Get comprehensive pitching statistics for a team with filtering"""
    game_types = request.args.getlist('game_types[]')
    splits = request.args.getlist('splits[]')
    if not splits:
        splits = [request.args.get('split', 'overall')]
    if not game_types:
        game_types = None
    conn = get_db_connection()
    try:
        base_query = """
            FROM pitching p
            JOIN games g ON p.game_id = g.game_id
            WHERE p.team = ?
        """
        params = [team_id]
        if game_types and len(game_types) > 0:
            placeholders = ','.join(['?' for _ in game_types])
            base_query += f" AND g.gametype IN ({placeholders})"
            params.extend(game_types)
        if splits and 'overall' not in [s.lower() for s in splits]:
            for split in splits:
                split_lower = split.lower()
                if split_lower in ['home', 'road']:
                    if split_lower == 'home':
                        base_query += " AND p.team = g.home_team_id"
                    else:
                        base_query += " AND p.team = g.away_team_id"
                elif split_lower in ['wins', 'losses']:
                    if split_lower == 'wins':
                        base_query += " AND p.team = g.winning_team_id"
                    else:
                        base_query += " AND p.team = g.losing_team_id"
        career_query = f"""
            SELECT 
                COUNT(DISTINCT p.game_id) as g,
                COUNT(*) as app,
                SUM(p.win) as w,
                SUM(p.loss) as l,
                SUM(p.save) as sv,
                SUM(p.hold) as hld,
                SUM(p.start) as gs,
                SUM(p.finish) as cg,
                SUM(p.ip) as ip,
                SUM(p.pitches_thrown) as pitches,
                SUM(p.batters_faced) as bf,
                SUM(p.r) as r,
                SUM(p.er) as er,
                SUM(p.p_h) as h,
                SUM(p.p_hr) as hr,
                SUM(p.p_k) as k,
                SUM(p.p_bb) as bb,
                SUM(p.p_hbp) as hbp,
                SUM(p.p_2b) as doubles,
                SUM(p.p_3b) as triples,
                SUM(p.p_gb) as gb,
                SUM(p.p_fb) as fb,
                SUM(p.wild_pitch) as wp,
                SUM(p.balk) as bk,
                SUM(p.p_roe) as roe,
                SUM(p.p_gdp) as gidp,
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
                ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,
                ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as k9,
                ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as bb9,
                ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as hr9,
                ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as fo_pct,
                ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as go_pct,
                ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gb), 0), 3) as gidp_pct,
                ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
                ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
                0 as era_plus,
                ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_hr) - SUM(p.p_bb) - SUM(p.p_hbp), 0), 3) as babip
            {base_query}
        """
        season_query = f"""
            SELECT 
                g.season,
                COUNT(DISTINCT p.game_id) as g,
                COUNT(*) as app,
                SUM(p.win) as w,
                SUM(p.loss) as l,
                SUM(p.save) as sv,
                SUM(p.hold) as hld,
                SUM(p.start) as gs,
                SUM(p.finish) as cg,
                SUM(p.ip) as ip,
                SUM(p.pitches_thrown) as pitches,
                SUM(p.batters_faced) as bf,
                SUM(p.r) as r,
                SUM(p.er) as er,
                SUM(p.p_h) as h,
                SUM(p.p_hr) as hr,
                SUM(p.p_k) as k,
                SUM(p.p_bb) as bb,
                SUM(p.p_hbp) as hbp,
                SUM(p.p_2b) as doubles,
                SUM(p.p_3b) as triples,
                SUM(p.p_gb) as gb,
                SUM(p.p_fb) as fb,
                SUM(p.wild_pitch) as wp,
                SUM(p.balk) as bk,
                SUM(p.p_roe) as roe,
                SUM(p.p_gdp) as gidp,
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
                ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,
                ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as k9,
                ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as bb9,
                ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as hr9,
                ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as fo_pct,
                ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as go_pct,
                ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gb), 0), 3) as gidp_pct,
                ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
                ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
                0 as era_plus,
                ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_hr) - SUM(p.p_bb) - SUM(p.p_hbp), 0), 3) as babip
            {base_query}
            GROUP BY g.season
            ORDER BY g.season ASC
        """
        cur = conn.execute(career_query, params)
        career = cur.fetchone()
        cur = conn.execute(season_query, params)
        seasons = [dict(row) for row in cur.fetchall()]
        
        # Add placeholder values for FIP and ERA+ and format IP
        for season in seasons:
            season['fip'] = season.get('raw_fip', 0.00) if season.get('raw_fip') else 0.00
            season['era_plus'] = 100
            season['ip'] = format_innings_pitched(season.get('ip'))
        
        # Handle career data
        if career:
            career_dict = dict(career)
            career_dict['fip'] = career_dict.get('raw_fip', 0.00) if career_dict.get('raw_fip') else 0.00
            career_dict['era_plus'] = 100
            career_dict['ip'] = format_innings_pitched(career_dict.get('ip'))
        else:
            career_dict = {}
        
        return jsonify({
            'seasons': seasons,
            'career': career_dict
        })
    except Exception as e:
        print(f"Team pitching stats error: {e}")
        return jsonify({'error': 'Failed to get team pitching stats'}), 500
    finally:
        conn.close()
# Database path
DB_PATH = 'yakyuu.db'

def build_event_filter_query(player_id, stat_type, game_types=None, split='overall'):
    """
    Build SQL query for event-based statistics with filtering
    
    Args:
        player_id: Player ID to filter for
        stat_type: 'batting' or 'pitching' - determines which player_id column to use
        game_types: List of game types to include (e.g., ['regular', 'allstar'])
        split: Split type ('overall', 'vs LHP', 'vs RHP', 'vs LHB', 'vs RHB', 'vs SHB', 'Home', 'Road', 'Wins', 'Losses')
    
    Returns:
        Tuple of (SQL query, parameters)
    """
    # Base query - start with event table
    base_query = """
        FROM event e
        JOIN games g ON e.game_id = g.game_id
    """
    
    # Add player filter based on stat type
    if stat_type == 'batting':
        base_query += " WHERE e.batter_player_id = ?"
        params = [player_id]
    else:  # pitching
        base_query += " WHERE e.pitcher_player_id = ?"
        params = [player_id]
    
    # Add game type filter
    if game_types and len(game_types) > 0:
        placeholders = ','.join(['?' for _ in game_types])
        base_query += f" AND g.gametype IN ({placeholders})"
        params.extend(game_types)
    
    # Add split filters
    if split != 'overall':
        if split in ['vs LHP', 'vs RHP']:
            # Filter by pitcher's throwing hand
            throw_hand = 'L' if split == 'vs LHP' else 'R'
            base_query += " AND EXISTS (SELECT 1 FROM players p WHERE p.player_id = e.pitcher_player_id AND p.throw = ?)"
            params.append(throw_hand)
            
        elif split in ['vs LHB', 'vs RHB', 'vs SHB']:
            # Filter by batter's batting hand
            bat_hand = 'L' if split == 'vs LHB' else ('R' if split == 'vs RHB' else 'S')
            base_query += " AND EXISTS (SELECT 1 FROM players p WHERE p.player_id = e.batter_player_id AND p.bat = ?)"
            params.append(bat_hand)
            
        elif split in ['Home', 'Road']:
            # Filter by home/road based on team position in game
            if stat_type == 'batting':
                # For batting: team = home_team_id means home, team = away_team_id means road
                if split == 'Home':
                    base_query += " AND e.team = g.home_team_id"
                else:  # Road
                    base_query += " AND e.team = g.away_team_id"
            else:  # pitching
                # For pitching: opposite logic since pitcher is on opposing team
                if split == 'Home':
                    base_query += " AND e.team = g.away_team_id"
                else:  # Road
                    base_query += " AND e.team = g.home_team_id"
                    
        elif split in ['Wins', 'Losses']:
            if stat_type == 'batting':
                # For batting: team aligns with winning/losing team
                if split == 'Wins':
                    base_query += " AND e.team = g.winning_team_id"
                else:  # Losses
                    base_query += " AND e.team = g.losing_team_id"
            else:  # pitching
                # For pitching: opposite logic
                if split == 'Wins':
                    base_query += " AND e.team = g.losing_team_id"
                else:  # Losses
                    base_query += " AND e.team = g.winning_team_id"
    
    return base_query, params

def get_batting_stats_from_events(player_id, game_types=None, splits=['overall']):
    """Get batting statistics from batting table with filtering"""
    # Build base query for batting table
    base_query = """
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        WHERE b.player_id = ?
    """
    params = [player_id]
    
    # Add game type filter
    if game_types and len(game_types) > 0:
        placeholders = ','.join(['?' for _ in game_types])
        base_query += f" AND g.gametype IN ({placeholders})"
        params.extend(game_types)
    
    # Add split filters
    if splits and 'overall' not in [s.lower() for s in splits]:
        for split in splits:
            split_lower = split.lower()
            if split_lower in ['home', 'road']:
                # Filter by home/road based on team position in game
                if split_lower == 'home':
                    base_query += " AND b.team = g.home_team_id"
                else:  # road
                    base_query += " AND b.team = g.away_team_id"
                    
            elif split_lower in ['wins', 'losses']:
                # Filter by wins/losses based on team alignment with winning/losing team
                if split_lower == 'wins':
                    base_query += " AND b.team = g.winning_team_id"
                else:  # losses
                    base_query += " AND b.team = g.losing_team_id"
    
    # Career stats query
    career_query = f"""
        SELECT 
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            SUM(b.b_h) as h,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_r) as r,
            SUM(b.b_bb) as bb,
            SUM(b.b_k) as so,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) as tb,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_fb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as fo_pct,
            ROUND(CAST(SUM(b.b_gb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as go_pct,
            ROUND(CAST(SUM(b.b_2b) + SUM(b.b_3b) + SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) * 100, 1) as xbh_pct,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso
        {base_query}
    """
    
    # Season stats query
    season_query = f"""
        SELECT 
            g.season,
            0.0 as ywar,
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            SUM(b.b_h) as h,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_r) as r,
            SUM(b.b_bb) as bb,
            SUM(b.b_k) as so,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) as tb,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_fb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as fo_pct,
            ROUND(CAST(SUM(b.b_gb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as go_pct,
            ROUND(CAST(SUM(b.b_2b) + SUM(b.b_3b) + SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) * 100, 1) as xbh_pct,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso
        {base_query}
        GROUP BY g.season
        ORDER BY g.season ASC
    """
    
    return career_query, season_query, params

def get_league_era_and_fip_constants():
    """Get league ERA and FIP constants by season for regular season games"""
    conn = sqlite3.connect('yakyuu.db')
    cursor = conn.cursor()
    
    query = """
        SELECT 
            g.season,
            ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as league_era,
            ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip
        FROM pitching p
        JOIN games g ON p.game_id = g.game_id
        WHERE g.gametype = '公式戦'
        GROUP BY g.season
        ORDER BY g.season
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    
    # Convert to dictionaries for easy lookup
    league_eras = {}
    fip_constants = {}
    
    for season, league_era, raw_fip in results:
        league_eras[season] = league_era
        # FIP constant = League ERA - Raw FIP (to make league FIP = league ERA)
        fip_constants[season] = round(league_era - raw_fip, 2)
    
    return league_eras, fip_constants

def get_league_wobas_and_scale():
    """Get league wOBA and wOBA scale by season for regular season games"""
    conn = sqlite3.connect('yakyuu.db')
    cursor = conn.cursor()
    
    query = """
        SELECT 
            g.season,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as league_woba
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        WHERE g.gametype = '公式戦'
        GROUP BY g.season
        ORDER BY g.season
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    
    # Convert to dictionary for easy lookup
    league_wobas = {}
    
    for season, league_woba in results:
        league_wobas[season] = league_woba
    
    # wOBA scale factor - calibrated for NPB
    # MLB uses 1.15, but NPB needs 0.16 due to using MLB wOBA weights in NPB context
    # This scale makes wRC+ comparable to OPS+ for NPB players
    woba_scale = 0.16
    
    return league_wobas, woba_scale

def get_park_factor(ballpark_name, stat_type='runs'):
    """Get park factor for a specific ballpark and stat type"""
    if not ballpark_name:
        return 1.0  # Neutral park factor if no ballpark specified
    
    conn = sqlite3.connect('yakyuu.db')
    cursor = conn.cursor()
    
    # Map stat types to column names
    pf_columns = {
        'runs': 'pf_runs',
        'hr': 'pf_hr',
        'hits': 'pf_h',
        'bb': 'pf_bb'
    }
    
    column = pf_columns.get(stat_type, 'pf_runs')
    
    cursor.execute(f'SELECT {column} FROM ballparks WHERE park_name = ?', (ballpark_name,))
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0] is not None:
        return result[0]
    else:
        return 1.0  # Neutral park factor if not found

def get_weighted_park_factors(player_id, seasons=None, stat_type='batting'):
    """Get weighted park factors for a player based on games played at each ballpark"""
    conn = sqlite3.connect('yakyuu.db')
    cursor = conn.cursor()
    
    # Build query based on stat type
    if stat_type == 'batting':
        table = 'batting'
        weight_column = 'pa'  # Weight by plate appearances
    else:  # pitching
        table = 'pitching'
        weight_column = 'ip'  # Weight by innings pitched
    
    # Base query to get ballpark usage
    query = f"""
        SELECT 
            g.ballpark,
            SUM(s.{weight_column}) as weight,
            bp.pf_runs
        FROM {table} s
        JOIN games g ON s.game_id = g.game_id
        LEFT JOIN ballparks bp ON g.ballpark = bp.park_name
        WHERE s.player_id = ? AND g.gametype = '公式戦'
    """
    
    params = [player_id]
    
    # Add season filter if specified
    if seasons:
        season_placeholders = ','.join(['?' for _ in seasons])
        query += f" AND g.season IN ({season_placeholders})"
        params.extend(seasons)
    
    query += " GROUP BY g.ballpark, bp.pf_runs"
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()
    
    if not results:
        return 1.0  # Neutral if no data
    
    # Calculate weighted average park factor
    total_weight = 0
    weighted_pf = 0
    
    for ballpark, weight, pf_runs in results:
        if weight and pf_runs is not None:
            total_weight += weight
            weighted_pf += pf_runs * weight
    
    if total_weight > 0:
        return weighted_pf / total_weight
    else:
        return 1.0  # Neutral if no valid data
    
def convert_duration_to_minutes(duration_str):
    """Convert H:MM duration format to total minutes"""
    if not duration_str:
        return 0
    try:
        if ':' in duration_str:
            hours, minutes = duration_str.split(':')
            return int(hours) * 60 + int(minutes)
        else:
            # If it's just a number, assume it's already in minutes
            return int(duration_str)
    except (ValueError, AttributeError):
        return 0



def get_pitching_stats_from_events(player_id, game_types=None, splits=['overall']):
    """Get pitching statistics from pitching table with filtering"""
    # Build base query for pitching table
    base_query = """
        FROM pitching p
        JOIN games g ON p.game_id = g.game_id
        WHERE p.player_id = ?
    """
    params = [player_id]
    
    # Add game type filter
    if game_types and len(game_types) > 0:
        placeholders = ','.join(['?' for _ in game_types])
        base_query += f" AND g.gametype IN ({placeholders})"
        params.extend(game_types)
    
    # Add split filters
    if splits and 'overall' not in [s.lower() for s in splits]:
        for split in splits:
            split_lower = split.lower()
            if split_lower in ['home', 'road']:
                # Filter by home/road based on team position in game
                if split_lower == 'home':
                    base_query += " AND p.team = g.home_team_id"
                else:  # road
                    base_query += " AND p.team = g.away_team_id"
                    
            elif split_lower in ['wins', 'losses']:
                # Filter by wins/losses based on team alignment with winning/losing team
                if split_lower == 'wins':
                    base_query += " AND p.team = g.winning_team_id"
                else:  # losses
                    base_query += " AND p.team = g.losing_team_id"
    
    # Career stats query
    career_query = f"""
        SELECT 
            COUNT(DISTINCT p.game_id) as g,
            COUNT(*) as app,
            SUM(p.start) as gs,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 THEN 0 ELSE p.finish END) as gf,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 THEN 1 ELSE 0 END) as cg,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 AND p.r = 0 THEN 1 ELSE 0 END) as sho,
            SUM(p.win) as w,
            SUM(p.loss) as l,
            SUM(p.save) as sv,
            SUM(p.hold) as hld,
            SUM(p.ip) as ip,
            SUM(p.batters_faced) as bf,
            SUM(p.r) as r,
            SUM(p.er) as er,
            SUM(p.p_h) as h,
            SUM(p.p_hr) as hr,
            SUM(p.p_k) as k,
            SUM(p.p_bb) as bb,
            SUM(p.p_hbp) as hbp,
            SUM(p.wild_pitch) as wp,
            SUM(p.balk) as bk,
            SUM(p.p_roe) as roe,
            ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
            ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,

            ROUND(CAST(SUM(p.win) AS FLOAT) / NULLIF(SUM(p.win) + SUM(p.loss), 0), 3) as w_pct,
            SUM(p.p_2b) as doubles,
            SUM(p.p_3b) as triples,
            ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 1) as hr9,
            ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 1) as k9,
            ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 1) as bb9,
            ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as fo_pct,
            ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as go_pct,
            ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gdp) + SUM(p.p_gb), 0) * 100, 1) as gidp_pct,
            ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
            ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
            0 as era_plus,
            ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_hr), 0), 3) as babip
        {base_query}
    """
    
    # Season stats query
    season_query = f"""
        SELECT 
            g.season,
            0.0 as ywar,
            COUNT(DISTINCT p.game_id) as g,
            COUNT(*) as app,
            SUM(p.start) as gs,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 THEN 0 ELSE p.finish END) as gf,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 THEN 1 ELSE 0 END) as cg,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 AND p.r = 0 THEN 1 ELSE 0 END) as sho,
            SUM(p.win) as w,
            SUM(p.loss) as l,
            SUM(p.save) as sv,
            SUM(p.hold) as hld,
            SUM(p.ip) as ip,
            SUM(p.batters_faced) as bf,
            SUM(p.r) as r,
            SUM(p.er) as er,
            SUM(p.p_h) as h,
            SUM(p.p_hr) as hr,
            SUM(p.p_k) as k,
            SUM(p.p_bb) as bb,
            SUM(p.p_hbp) as hbp,
            SUM(p.wild_pitch) as wp,
            SUM(p.balk) as bk,
            SUM(p.p_roe) as roe,
            ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
            ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,

            ROUND(CAST(SUM(p.win) AS FLOAT) / NULLIF(SUM(p.win) + SUM(p.loss), 0), 3) as w_pct,
            SUM(p.p_2b) as doubles,
            SUM(p.p_3b) as triples,
            ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 1) as hr9,
            ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 1) as k9,
            ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 1) as bb9,
            ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as fo_pct,
            ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as go_pct,
            ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gdp) + SUM(p.p_gb), 0) * 100, 1) as gidp_pct,
            ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
            ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
            0 as era_plus,
            ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_hr), 0), 3) as babip
        {base_query}
        GROUP BY g.season
        ORDER BY g.season ASC
    """
    
    return career_query, season_query, params

@app.route('/')
def index():
    """Serve the main index page"""
    return app.send_static_file('index.html')

@app.route('/players')
def players_homepage():
    """Serve the players homepage"""
    return app.send_static_file('players.html')

@app.route('/players/<player_id>')
def player_page(player_id):
    """Serve individual player page"""
    return app.send_static_file('player_template.html')

@app.route('/teams')
def teams_homepage():
    """Serve the teams/standings homepage"""
    return app.send_static_file('standings.html')

@app.route('/teams/<team_id>')
def team_page(team_id):
    """Serve individual team page"""
    return app.send_static_file('teams.html')

@app.route('/games')
def games_homepage():
    """Serve the games homepage"""
    return app.send_static_file('games.html')

@app.route('/games/<path:game_id>')
def game_page(game_id):
    """Serve individual game page"""
    return app.send_static_file('game_manifesto.html')

@app.route('/api/search')
def search():
    """Search for players, teams, games, or ballparks"""
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'results': []})
    
    conn = get_db_connection()
    results = []
    
    try:
        # Search players
        cursor = conn.execute("""
            SELECT player_id, player_name, player_name_en, bat, throw 
            FROM players 
            WHERE player_name LIKE ? OR player_name_en LIKE ?
            LIMIT 10
        """, (f'%{query}%', f'%{query}%'))
        
        for row in cursor.fetchall():
            results.append({
                'type': 'player',
                'id': row['player_id'],
                'name': row['player_name'],
                'name_en': row['player_name_en'],
                'bat': row['bat'],
                'throw': row['throw']
            })
        
        # Search games (by date or teams)
        cursor = conn.execute("""
            SELECT game_id, date, home_team_id, away_team_id, home_runs, visitor_runs
            FROM games 
            WHERE date LIKE ? OR home_team_id LIKE ? OR away_team_id LIKE ?
            ORDER BY date DESC
            LIMIT 10
        """, (f'%{query}%', f'%{query}%', f'%{query}%'))
        
        for row in cursor.fetchall():
            results.append({
                'type': 'game',
                'id': row['game_id'],
                'date': row['date'],
                'home_team': row['home_team_id'],
                'away_team': row['away_team_id'],
                'score': f"{row['home_runs']}-{row['visitor_runs']}"
            })
        
        # Search ballparks
        cursor = conn.execute("""
            SELECT park_name, home_team, city
            FROM ballparks 
            WHERE park_name LIKE ? OR home_team LIKE ? OR city LIKE ?
            LIMIT 10
        """, (f'%{query}%', f'%{query}%', f'%{query}%'))
        
        for row in cursor.fetchall():
            results.append({
                'type': 'ballpark',
                'id': row['park_name'],
                'name': row['park_name'],
                'home_team': row['home_team'],
                'city': row['city']
            })
            
    except Exception as e:
        print(f"Search error: {e}")
        return jsonify({'error': 'Search failed'}), 500
    finally:
        conn.close()
    
    return jsonify({'results': results[:20]})  # Limit to 20 total results

@app.route('/api/players/<player_id>')
def get_player(player_id):
    """Get player data and basic stats"""
    conn = get_db_connection()
    
    try:
        # Get player metadata
        cursor = conn.execute("""
            SELECT * FROM players WHERE player_id = ?
        """, (player_id,))
        player = cursor.fetchone()
        
        if not player:
            return jsonify({'error': 'Player not found'}), 404
        
        # Get basic career stats from batting table
        cursor = conn.execute("""
            SELECT 
                COUNT(DISTINCT game_id) as games,
                SUM(pa) as pa,
                SUM(ab) as ab,
                SUM(b_h) as hits,
                SUM(b_hr) as hr,
                SUM(b_rbi) as rbi,
                SUM(b_r) as runs,
                SUM(b_bb) as bb,
                SUM(b_k) as so
            FROM batting 
            WHERE player_id = ?
        """, (player_id,))
        batting_stats = cursor.fetchone()
        
        # Get basic career stats from pitching table
        cursor = conn.execute("""
            SELECT 
                COUNT(DISTINCT game_id) as games,
                COUNT(*) as appearances,
                SUM(win) as wins,
                SUM(loss) as losses,
                SUM(save) as saves,
                SUM(ip) as innings,
                SUM(p_k) as strikeouts,
                SUM(p_bb) as walks
            FROM pitching 
            WHERE player_id = ?
        """, (player_id,))
        pitching_stats = cursor.fetchone()
        
        # Get player's current/most recent team
        cursor = conn.execute("""
            SELECT t.team_id, t.team_name, t.team_name_en
            FROM (
                SELECT team, MAX(date) as latest_date
                FROM (
                    SELECT team, g.date
                    FROM batting b
                    JOIN games g ON b.game_id = g.game_id
                    WHERE b.player_id = ?
                    UNION ALL
                    SELECT team, g.date
                    FROM pitching p
                    JOIN games g ON p.game_id = g.game_id
                    WHERE p.player_id = ?
                )
                GROUP BY team
                ORDER BY latest_date DESC
                LIMIT 1
            ) latest_team
            JOIN teams t ON latest_team.team = t.team_id
        """, (player_id, player_id))
        team_info = cursor.fetchone()
        
        # Get player's position from recent log entries
        # First check if player has any recent pitching appearances (if so, they're a pitcher)
        cursor = conn.execute("""
            SELECT 1
            FROM pitching p
            JOIN games g ON p.game_id = g.game_id
            WHERE p.player_id = ?
            ORDER BY g.date DESC, g.game_id DESC
            LIMIT 1
        """, (player_id,))
        has_pitching = cursor.fetchone()
        
        if has_pitching:
            # Player has pitching appearances, so they're a pitcher
            detected_position = 'P'
        else:
            # Check batting logs for position
            cursor = conn.execute("""
                SELECT b.position
                FROM batting b
                JOIN games g ON b.game_id = g.game_id
                WHERE b.player_id = ? AND b.position IS NOT NULL AND b.position != ''
                ORDER BY g.date DESC, g.game_id DESC
                LIMIT 1
            """, (player_id,))
            batting_position = cursor.fetchone()
            detected_position = batting_position[0] if batting_position else player['position']
        
        player_data = {
            'player_id': player['player_id'],
            'name': player['player_name'],
            'name_en': player['player_name_en'],
            'bat': player['bat'],
            'throw': player['throw'],
            'height': player['height'],
            'weight': player['weight'],
            'birthdate': player['birthdate'],
            'position': detected_position,
            'team_id': team_info['team_id'] if team_info else None,
            'team_name': team_info['team_name'] if team_info else None,
            'team_name_en': team_info['team_name_en'] if team_info else None,
            'batting_stats': dict(batting_stats) if batting_stats else {},
            'pitching_stats': dict(pitching_stats) if pitching_stats else {}
        }
        
        return jsonify(player_data)
        
    except Exception as e:
        print(f"Player API error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Failed to get player data'}), 500
    finally:
        conn.close()

@app.route('/api/games/<path:game_id>')
def get_game(game_id):
    """Get comprehensive game data"""
    conn = get_db_connection()
    
    try:
        # Get game metadata with team names, pitcher English names, and ballpark English name
        cursor = conn.execute("""
            SELECT g.*, 
                   ht.team_name as home_team_name, ht.team_name_en as home_team_name_en,
                   at.team_name as away_team_name, at.team_name_en as away_team_name_en,
                   wp.player_name as winning_pitcher_name, wp.player_name_en as winning_pitcher_name_en,
                   lp.player_name as losing_pitcher_name, lp.player_name_en as losing_pitcher_name_en,
                   sp.player_name as save_pitcher_name, sp.player_name_en as save_pitcher_name_en,
                   bp.park_name_en as ballpark_en
            FROM games g
            LEFT JOIN teams ht ON g.home_team_id = ht.team_id
            LEFT JOIN teams at ON g.away_team_id = at.team_id
            LEFT JOIN players wp ON g.winning_pitcher_id = wp.player_id
            LEFT JOIN players lp ON g.losing_pitcher_id = lp.player_id
            LEFT JOIN players sp ON g.save_pitcher_id = sp.player_id
            LEFT JOIN ballparks bp ON g.ballpark = bp.park_name
            WHERE g.game_id = ?
        """, (game_id,))
        game = cursor.fetchone()
        
        if not game:
            return jsonify({'error': 'Game not found'}), 404
        
        # Build line score data
        line_score = {
            'visitor': [],
            'home': []
        }
        
        # Get visitor innings (1-12)
        for i in range(1, 13):
            inning_key = f'visitor_inn{i}'
            if game[inning_key] is not None:
                line_score['visitor'].append(game[inning_key])
        
        # Get home innings (1-12)
        for i in range(1, 13):
            inning_key = f'home_inn{i}'
            if game[inning_key] is not None:
                line_score['home'].append(game[inning_key])
        
        game_data = {
            'game_id': game['game_id'],
            'date': game['date'],
            'season': game['season'],
            'game_number': game['game_number'],
            'start_time': game['start_time'],
            'game_duration': game['game_duration'],
            'attendance': game['attendance'],
            'ballpark': game['ballpark'],
            'ballpark_en': game['ballpark_en'],
            'gametype': game['gametype'],
            
            # Team information
            'home_team_id': game['home_team_id'],
            'away_team_id': game['away_team_id'],
            'home_team_name': game['home_team_name'],
            'home_team_name_en': game['home_team_name_en'],
            'away_team_name': game['away_team_name'],
            'away_team_name_en': game['away_team_name_en'],
            
            # Score information
            'home_runs': game['home_runs'],
            'visitor_runs': game['visitor_runs'],
            'home_hits': game['home_hits'],
            'visitor_hits': game['visitor_hits'],
            'home_errors': game['home_errors'],
            'visitor_errors': game['visitor_errors'],
            
            # Winning/losing information
            'winning_team_id': game['winning_team_id'],
            'losing_team_id': game['losing_team_id'],
            'winning_pitcher_id': game['winning_pitcher_id'],
            'losing_pitcher_id': game['losing_pitcher_id'],
            'save_pitcher_id': game['save_pitcher_id'],
            'winning_pitcher_name': game['winning_pitcher_name'],
            'winning_pitcher_name_en': game['winning_pitcher_name_en'],
            'losing_pitcher_name': game['losing_pitcher_name'],
            'losing_pitcher_name_en': game['losing_pitcher_name_en'],
            'save_pitcher_name': game['save_pitcher_name'],
            'save_pitcher_name_en': game['save_pitcher_name_en'],
            
            # Line score
            'line_score': line_score
        }
        
        return jsonify(game_data)
        
    except Exception as e:
        print(f"Game API error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Failed to get game data'}), 500
    finally:
        conn.close()

@app.route('/api/games/<path:game_id>/batting')
def get_game_batting(game_id):
    """Get batting stats for a specific game"""
    conn = get_db_connection()
    
    try:
        # Get batting stats with player names (Japanese and English)
        # Use natural database order which matches NPB.jp batting order
        cursor = conn.execute("""
            SELECT b.*, p.player_name, p.player_name_en
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            WHERE b.game_id = ?
            ORDER BY b.team
        """, (game_id,))
        
        batting_data = [dict(row) for row in cursor.fetchall()]
        
        # Separate by team
        away_batting = []
        home_batting = []
        
        # Get game info to determine home/away teams
        cursor = conn.execute("""
            SELECT home_team_id, away_team_id FROM games WHERE game_id = ?
        """, (game_id,))
        game_info = cursor.fetchone()
        
        if game_info:
            home_team = game_info['home_team_id']
            away_team = game_info['away_team_id']
            
            for player in batting_data:
                if player['team'] == home_team:
                    home_batting.append(player)
                else:
                    away_batting.append(player)
        
        return jsonify({
            'home': home_batting,
            'away': away_batting
        })
        
    except Exception as e:
        print(f"Game batting API error: {e}")
        return jsonify({'error': 'Failed to get batting data'}), 500
    finally:
        conn.close()

@app.route('/api/games/<path:game_id>/pitching')
def get_game_pitching(game_id):
    """Get pitching stats for a specific game"""
    conn = get_db_connection()
    
    try:
        # Get pitching stats with player names (Japanese and English)
        # Use natural database order which matches NPB.jp pitching order
        cursor = conn.execute("""
            SELECT p.*, pl.player_name, pl.player_name_en
            FROM pitching p
            JOIN players pl ON p.player_id = pl.player_id
            WHERE p.game_id = ?
            ORDER BY p.team
        """, (game_id,))
        
        pitching_data = [dict(row) for row in cursor.fetchall()]
        
        # Separate by team
        away_pitching = []
        home_pitching = []
        
        # Get game info to determine home/away teams
        cursor = conn.execute("""
            SELECT home_team_id, away_team_id FROM games WHERE game_id = ?
        """, (game_id,))
        game_info = cursor.fetchone()
        
        if game_info:
            home_team = game_info['home_team_id']
            away_team = game_info['away_team_id']
            
            for player in pitching_data:
                if player['team'] == home_team:
                    home_pitching.append(player)
                else:
                    away_pitching.append(player)
        
        return jsonify({
            'home': home_pitching,
            'away': away_pitching
        })
        
    except Exception as e:
        print(f"Game pitching API error: {e}")
        return jsonify({'error': 'Failed to get pitching data'}), 500
    finally:
        conn.close()

@app.route('/api/games/<path:game_id>/events')
def get_game_events(game_id):
    """Get play-by-play events for a specific game organized by half-inning"""
    conn = get_db_connection()
    
    try:
        # Get events with player names (both Japanese and English)
        cursor = conn.execute("""
            SELECT e.*, 
                   bp.player_name as batter_name, bp.player_name_en as batter_name_en,
                   pp.player_name as pitcher_name, pp.player_name_en as pitcher_name_en
            FROM event e
            LEFT JOIN players bp ON e.batter_player_id = bp.player_id
            LEFT JOIN players pp ON e.pitcher_player_id = pp.player_id
            WHERE e.game_id = ?
            ORDER BY 
                CAST(SUBSTR(e.inning, 1, LENGTH(e.inning)-1) AS INTEGER),
                CASE WHEN SUBSTR(e.inning, -1) = 'T' THEN 0 ELSE 1 END,
                e.ROWID
        """, (game_id,))
        
        events = []
        previous_pitcher = None
        
        for row in cursor.fetchall():
            # Calculate result from stat columns
            result = calculate_event_result(row)
            
            # Parse inning format (e.g., "1T" -> inning=1, half="T")
            inning_str = row['inning']
            inning_num = inning_str[:-1]  # Remove last character (T/B)
            half = inning_str[-1]  # Get last character (T/B)
            
            # Check for pitching change
            current_pitcher = row['pitcher_player_id']
            if previous_pitcher and previous_pitcher != current_pitcher and events:
                # Same inning/half but different pitcher = pitching change
                last_event = events[-1] if events else None
                if (last_event and 
                    last_event['inning'] == inning_num and 
                    last_event['half'] == half):
                    
                    # Add pitching change row
                    events.append({
                        'type': 'pitching_change',
                        'inning': inning_num,
                        'half': half,
                        'team': row['team'],
                        'new_pitcher_name': row['pitcher_name'],
                        'new_pitcher_name_en': row['pitcher_name_en'],
                        'new_pitcher_id': row['pitcher_player_id']
                    })
            
            # Add the actual event
            events.append({
                'type': 'event',
                'inning': inning_num,
                'half': half,
                'team': row['team'],
                'batter_name': row['batter_name'],
                'batter_name_en': row['batter_name_en'],
                'batter_id': row['batter_player_id'],
                'pitcher_name': row['pitcher_name'],
                'pitcher_name_en': row['pitcher_name_en'],
                'pitcher_id': row['pitcher_player_id'],
                'outs': row['out'],
                'count': row['count'],
                'on_base': row['on_base'],
                'result': result,
                'stats': {
                    'h': row['h'],
                    'rbi': row['rbi'],
                    '1b': row['1b'],
                    '2b': row['2b'],
                    '3b': row['3b'],
                    'hr': row['hr'],
                    'gb': row['gb'],
                    'fb': row['fb'],
                    'k': row['k'],
                    'roe': row['roe'],
                    'bb': row['bb'],
                    'hbp': row['hbp'],
                    'gdp': row['gdp'],
                    'sac': row['sac']
                }
            })
            
            previous_pitcher = current_pitcher
        
        return jsonify(events)
        
    except Exception as e:
        print(f"Game events API error: {e}")
        return jsonify({'error': 'Failed to get events data'}), 500
    finally:
        conn.close()


def calculate_event_result(row):
    """Calculate the result description from event statistics"""
    # Priority order for determining result
    if row['hr'] == 1:
        return "Home Run"
    elif row['3b'] == 1:
        return "Triple"
    elif row['2b'] == 1:
        return "Double"
    elif row['1b'] == 1:
        return "Single"
    elif row['k'] == 1:
        return "Strikeout"
    elif row['bb'] == 1:
        return "Walk"
    elif row['hbp'] == 1:
        return "Hit by Pitch"
    elif row['sac'] == 1:
        return "Sacrifice"
    elif row['gdp'] == 1:
        return "Ground into Double Play"
    elif row['roe'] == 1:
        return "Reached on Error"
    elif row['gb'] == 1:
        return "Ground Ball Out"
    elif row['fb'] == 1:
        return "Fly Ball Out"
    elif row['h'] == 1:
        return "Hit"
    else:
        return "Out"

@app.route('/api/recent-games')
def get_recent_games():
    """Get games from a specific date or the most recent game date"""
    conn = get_db_connection()
    
    try:
        # Get date parameter, if provided
        requested_date = request.args.get('date')
        
        if requested_date:
            # Use the requested date
            target_date = requested_date
        else:
            # Get the most recent game date
            cursor = conn.execute("""
                SELECT MAX(date) as latest_date
                FROM games
            """)
            latest_date_row = cursor.fetchone()
            
            if not latest_date_row or not latest_date_row['latest_date']:
                return jsonify({
                    'games': [],
                    'has_more': False,
                    'date': None
                })
            
            target_date = latest_date_row['latest_date']
        
        # Get all games from the target date
        base_query = """
            SELECT g.game_id, g.date, g.home_team_id, g.away_team_id, 
                   g.home_runs, g.visitor_runs, g.home_hits, g.visitor_hits,
                   g.home_errors, g.visitor_errors, g.ballpark, g.gametype, g.game_number,
                   ht.team_name as home_team_name, ht.team_name_en as home_team_name_en,
                   at.team_name as away_team_name, at.team_name_en as away_team_name_en,
                   bp.park_name_en as ballpark_en
            FROM games g
            LEFT JOIN teams ht ON g.home_team_id = ht.team_id
            LEFT JOIN teams at ON g.away_team_id = at.team_id
            LEFT JOIN ballparks bp ON g.ballpark = bp.park_name OR g.ballpark = bp.park_name_en
            WHERE g.date = ?
            ORDER BY g.game_id ASC
        """
        
        cursor = conn.execute(base_query, [target_date])
        rows = cursor.fetchall()
        
        games = []
        for row in rows:
            # Use English team names only
            home_team = row['home_team_name_en'] or row['home_team_name'] or row['home_team_id']
            away_team = row['away_team_name_en'] or row['away_team_name'] or row['away_team_id']
            
            games.append({
                'game_id': row['game_id'],
                'date': row['date'],
                'home_team': home_team,
                'away_team': away_team,
                'home_team_id': row['home_team_id'],
                'away_team_id': row['away_team_id'],
                'score': f"{row['visitor_runs']}-{row['home_runs']}",  # Away-Home format
                'home_runs': row['home_runs'],
                'visitor_runs': row['visitor_runs'],
                'home_hits': row['home_hits'] or 0,
                'visitor_hits': row['visitor_hits'] or 0,
                'home_errors': row['home_errors'] or 0,
                'visitor_errors': row['visitor_errors'] or 0,
                'ballpark': row['ballpark'],
                'ballpark_en': row['ballpark_en'],
                'game_number': row['game_number'],
                'gametype': row['gametype']
            })
        
        return jsonify({
            'games': games,
            'has_more': False,  # No pagination needed for single day
            'date': target_date
        })
        
    except Exception as e:
        print(f"Recent games error: {e}")
        return jsonify({'error': 'Failed to get recent games'}), 500
    finally:
        conn.close()

@app.route('/api/league-leaders')
def get_league_leaders():
    """Get league leaders for current season with dynamic qualifiers"""
    conn = get_db_connection()
    
    try:
        # Get current season
        cursor = conn.execute("""
            SELECT MAX(season) as current_season
            FROM games
        """)
        current_season = cursor.fetchone()['current_season']
        
        # Get current season average qualifiers (exclude All-Star teams)
        cursor = conn.execute("""
            SELECT AVG(b_qualifier) as avg_b_qualifier,
                   AVG(p_qualifier) as avg_p_qualifier
            FROM (
                SELECT b_qualifier, p_qualifier
                FROM teams 
                WHERE b_qualifier > 0 AND p_qualifier > 50  -- Exclude All-Star teams
                ORDER BY p_qualifier DESC
                LIMIT 12  -- Top 12 teams (main NPB teams)
            )
        """)
        
        qualifiers = cursor.fetchone()
        b_qualifier = qualifiers['avg_b_qualifier'] or 300.0
        p_qualifier = qualifiers['avg_p_qualifier'] or 200.0
        
        print(f"Using dynamic qualifiers for {current_season}: {b_qualifier:.1f} PA, {p_qualifier:.1f} IP")
        
        # Batting Average leaders (regular season only)
        cursor = conn.execute("""
            SELECT p.player_name, p.player_name_en, p.player_id, 
                   ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            JOIN games g ON b.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY b.player_id, p.player_name, p.player_name_en
            HAVING SUM(b.pa) >= ? AND SUM(b.ab) > 0  -- Dynamic PA qualifier
            ORDER BY avg DESC
            LIMIT 5
        """, (current_season, b_qualifier))
        
        avg_leaders = []
        for row in cursor.fetchall():
            avg_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': f"{row['avg']:.3f}"
            })
        
        # Hits leaders (regular season only)
        cursor = conn.execute("""
            SELECT p.player_name, p.player_name_en, p.player_id, SUM(b.b_h) as hits
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            JOIN games g ON b.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY b.player_id, p.player_name, p.player_name_en
            HAVING SUM(b.pa) >= ?  -- Dynamic PA qualifier
            ORDER BY hits DESC
            LIMIT 5
        """, (current_season, b_qualifier))
        
        hits_leaders = []
        for row in cursor.fetchall():
            hits_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': str(row['hits'])
            })
        
        # Home Run leaders (regular season only)
        cursor = conn.execute("""
            SELECT p.player_name, p.player_name_en, p.player_id, SUM(b.b_hr) as hr
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            JOIN games g ON b.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY b.player_id, p.player_name, p.player_name_en
            HAVING SUM(b.pa) >= ?  -- Dynamic PA qualifier
            ORDER BY hr DESC
            LIMIT 5
        """, (current_season, b_qualifier))
        
        hr_leaders = []
        for row in cursor.fetchall():
            hr_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': str(row['hr'])
            })
        
        # RBI leaders (regular season only)
        cursor = conn.execute("""
            SELECT p.player_name, p.player_name_en, p.player_id, SUM(b.b_rbi) as rbi
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            JOIN games g ON b.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY b.player_id, p.player_name, p.player_name_en
            HAVING SUM(b.pa) >= ?  -- Dynamic PA qualifier
            ORDER BY rbi DESC
            LIMIT 5
        """, (current_season, b_qualifier))
        
        rbi_leaders = []
        for row in cursor.fetchall():
            rbi_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': str(row['rbi'])
            })
        
        # ERA leaders (regular season only)
        cursor = conn.execute("""
            SELECT p2.player_name, p2.player_name_en, p.player_id,
                   ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / CAST(SUM(p.ip) AS FLOAT), 2) as era
            FROM pitching p
            JOIN players p2 ON p.player_id = p2.player_id
            JOIN games g ON p.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY p.player_id, p2.player_name, p2.player_name_en
            HAVING SUM(p.ip) >= ?  -- Dynamic IP qualifier
            ORDER BY era ASC
            LIMIT 5
        """, (current_season, p_qualifier))
        
        era_leaders = []
        for row in cursor.fetchall():
            era_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': f"{row['era']:.2f}"
            })
        
        # WHIP leaders (regular season only)
        cursor = conn.execute("""
            SELECT p2.player_name, p2.player_name_en, p.player_id,
                   ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / CAST(SUM(p.ip) AS FLOAT), 2) as whip
            FROM pitching p
            JOIN players p2 ON p.player_id = p2.player_id
            JOIN games g ON p.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY p.player_id, p2.player_name, p2.player_name_en
            HAVING SUM(p.ip) >= ?  -- Dynamic IP qualifier
            ORDER BY whip ASC
            LIMIT 5
        """, (current_season, p_qualifier))
        
        whip_leaders = []
        for row in cursor.fetchall():
            whip_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': f"{row['whip']:.2f}"
            })
        
        # Strikeout leaders (regular season only)
        cursor = conn.execute("""
            SELECT p2.player_name, p2.player_name_en, p.player_id, SUM(p.p_k) as strikeouts
            FROM pitching p
            JOIN players p2 ON p.player_id = p2.player_id
            JOIN games g ON p.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY p.player_id, p2.player_name, p2.player_name_en
            HAVING SUM(p.ip) >= ?  -- Dynamic IP qualifier
            ORDER BY strikeouts DESC
            LIMIT 5
        """, (current_season, p_qualifier))
        
        k_leaders = []
        for row in cursor.fetchall():
            k_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': str(row['strikeouts'])
            })
        
        # Innings Pitched leaders (regular season only)
        cursor = conn.execute("""
            SELECT p2.player_name, p2.player_name_en, p.player_id, SUM(p.ip) as ip
            FROM pitching p
            JOIN players p2 ON p.player_id = p2.player_id
            JOIN games g ON p.game_id = g.game_id
            WHERE g.season = ? AND g.gametype = '公式戦'
            GROUP BY p.player_id, p2.player_name, p2.player_name_en
            HAVING SUM(p.ip) >= ?  -- Dynamic IP qualifier
            ORDER BY ip DESC
            LIMIT 5
        """, (current_season, p_qualifier))
        
        ip_leaders = []
        for row in cursor.fetchall():
            ip_leaders.append({
                'name': row['player_name_en'] or row['player_name'],
                'player_id': row['player_id'],
                'stat': format_innings_pitched(row['ip'])
            })
        
        return jsonify({
            'batting_leaders': {
                'avg': avg_leaders,
                'hits': hits_leaders,
                'hr': hr_leaders,
                'rbi': rbi_leaders
            },
            'pitching_leaders': {
                'era': era_leaders,
                'whip': whip_leaders,
                'k': k_leaders,
                'ip': ip_leaders
            }
        })
        
    except Exception as e:
        print(f"League leaders error: {e}")
        return jsonify({'error': 'Failed to get league leaders'}), 500
    finally:
        conn.close()

@app.route('/api/team-leaders/<team_id>')
def get_team_leaders(team_id):
    """Get team leaders for batting and pitching (current season)"""
    conn = get_db_connection()
    
    try:
        # Get current season
        cursor = conn.execute("""
            SELECT MAX(season) as current_season
            FROM games
        """)
        current_season = cursor.fetchone()['current_season']
        
        # Get team's current season qualifier
        cursor = conn.execute("""
            SELECT b_qualifier, p_qualifier 
            FROM teams 
            WHERE team_id = ?
        """, (team_id,))
        
        team_qualifiers = cursor.fetchone()
        if not team_qualifiers:
            return jsonify({'error': 'Team not found'}), 404
            
        b_qualifier = team_qualifiers['b_qualifier'] or 300.0
        p_qualifier = team_qualifiers['p_qualifier'] or 200.0
        
        # Simplified team leaders for current season
        batting_leaders = {}
        
        # Hits leaders
        cursor = conn.execute("""
            SELECT p.player_name, p.player_name_en, p.player_id, SUM(b.b_h) as hits
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            JOIN games g ON b.game_id = g.game_id
            WHERE b.team = ? AND g.season = ?
            GROUP BY b.player_id, p.player_name, p.player_name_en
            ORDER BY hits DESC
            LIMIT 1
        """, (team_id, current_season))
        
        hits_leader = cursor.fetchone()
        batting_leaders['hits'] = [{
            'name': hits_leader['player_name_en'] or hits_leader['player_name'],
            'player_id': hits_leader['player_id'],
            'stat': str(hits_leader['hits'])
        }] if hits_leader else []
        
        # HR leaders
        cursor = conn.execute("""
            SELECT p.player_name, p.player_name_en, p.player_id, SUM(b.b_hr) as hr
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            JOIN games g ON b.game_id = g.game_id
            WHERE b.team = ? AND g.season = ?
            GROUP BY b.player_id, p.player_name, p.player_name_en
            ORDER BY hr DESC
            LIMIT 1
        """, (team_id, current_season))
        
        hr_leader = cursor.fetchone()
        batting_leaders['hr'] = [{
            'name': hr_leader['player_name_en'] or hr_leader['player_name'],
            'player_id': hr_leader['player_id'],
            'stat': str(hr_leader['hr'])
        }] if hr_leader else []
        
        # RBI leaders
        cursor = conn.execute("""
            SELECT p.player_name, p.player_name_en, p.player_id, SUM(b.b_rbi) as rbi
            FROM batting b
            JOIN players p ON b.player_id = p.player_id
            JOIN games g ON b.game_id = g.game_id
            WHERE b.team = ? AND g.season = ?
            GROUP BY b.player_id, p.player_name, p.player_name_en
            ORDER BY rbi DESC
            LIMIT 1
        """, (team_id, current_season))
        
        rbi_leader = cursor.fetchone()
        batting_leaders['rbi'] = [{
            'name': rbi_leader['player_name_en'] or rbi_leader['player_name'],
            'player_id': rbi_leader['player_id'],
            'stat': str(rbi_leader['rbi'])
        }] if rbi_leader else []
        
        # Pitching leaders
        pitching_leaders = {}
        
        # ERA leaders
        cursor = conn.execute("""
            SELECT p2.player_name, p2.player_name_en, p.player_id,
                   ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / CAST(SUM(p.ip) AS FLOAT), 2) as era
            FROM pitching p
            JOIN players p2 ON p.player_id = p2.player_id
            JOIN games g ON p.game_id = g.game_id
            WHERE p.team = ? AND g.season = ?
            GROUP BY p.player_id, p2.player_name, p2.player_name_en
            HAVING SUM(p.ip) >= ?
            ORDER BY era ASC
            LIMIT 1
        """, (team_id, current_season, p_qualifier))
        
        era_leader = cursor.fetchone()
        pitching_leaders['era'] = [{
            'name': era_leader['player_name_en'] or era_leader['player_name'],
            'player_id': era_leader['player_id'],
            'stat': f"{era_leader['era']:.2f}"
        }] if era_leader else []
        
        # WHIP leaders
        cursor = conn.execute("""
            SELECT p2.player_name, p2.player_name_en, p.player_id,
                   ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / CAST(SUM(p.ip) AS FLOAT), 2) as whip
            FROM pitching p
            JOIN players p2 ON p.player_id = p2.player_id
            JOIN games g ON p.game_id = g.game_id
            WHERE p.team = ? AND g.season = ?
            GROUP BY p.player_id, p2.player_name, p2.player_name_en
            HAVING SUM(p.ip) >= ?
            ORDER BY whip ASC
            LIMIT 1
        """, (team_id, current_season, p_qualifier))
        
        whip_leader = cursor.fetchone()
        pitching_leaders['whip'] = [{
            'name': whip_leader['player_name_en'] or whip_leader['player_name'],
            'player_id': whip_leader['player_id'],
            'stat': f"{whip_leader['whip']:.2f}"
        }] if whip_leader else []
        
        # Strikeout leaders
        cursor = conn.execute("""
            SELECT p2.player_name, p2.player_name_en, p.player_id, SUM(p.p_k) as strikeouts
            FROM pitching p
            JOIN players p2 ON p.player_id = p2.player_id
            JOIN games g ON p.game_id = g.game_id
            WHERE p.team = ? AND g.season = ?
            GROUP BY p.player_id, p2.player_name, p2.player_name_en
            ORDER BY strikeouts DESC
            LIMIT 1
        """, (team_id, current_season))
        
        k_leader = cursor.fetchone()
        pitching_leaders['k'] = [{
            'name': k_leader['player_name_en'] or k_leader['player_name'],
            'player_id': k_leader['player_id'],
            'stat': str(k_leader['strikeouts'])
        }] if k_leader else []
        
        return jsonify({
            'batting_leaders': batting_leaders,
            'pitching_leaders': pitching_leaders
        })
        
        # OLD COMPLEX CODE BELOW - REMOVE THIS SECTION
        for stat in ['old_code']:
            if stat in ['b_avg', 'b_obp', 'b_slg']:
                # For rate stats, use calculated values
                if stat == 'b_avg':
                    cursor = conn.execute("""
                        SELECT p.player_name, p.player_name_en, p.player_id,
                               ROUND(CAST(SUM(b.b_h) AS FLOAT) / CAST(SUM(b.b_ab) AS FLOAT), 3) as avg
                        FROM batting b
                        JOIN players p ON b.player_id = p.player_id
                        JOIN games g ON b.game_id = g.game_id
                        WHERE b.team = ? AND g.season = ?
                        GROUP BY b.player_id, p.player_name, p.player_name_en
                        HAVING SUM(b.pa) >= ? AND SUM(b.b_ab) > 0
                        ORDER BY avg DESC
                        LIMIT 3
                    """, (team_id, current_season, b_qualifier * 0.5))  # Lower qualifier for team leaders
                elif stat == 'b_obp':
                    cursor = conn.execute("""
                        SELECT p.player_name, p.player_name_en, p.player_id,
                               ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / CAST(SUM(b.pa) AS FLOAT), 3) as obp
                        FROM batting b
                        JOIN players p ON b.player_id = p.player_id
                        WHERE b.team_id = ?
                        GROUP BY b.player_id, p.player_name, p.player_name_en
                        HAVING SUM(b.pa) >= ?
                        ORDER BY obp DESC
                        LIMIT 3
                    """, (team_id, b_qualifier * 0.5))
                else:  # b_slg
                    cursor = conn.execute("""
                        SELECT p.player_name, p.player_name_en, p.player_id,
                               ROUND(CAST(SUM(b.b_h) + SUM(b.b_2b) + 2*SUM(b.b_3b) + 3*SUM(b.b_hr) AS FLOAT) / CAST(SUM(b.b_ab) AS FLOAT), 3) as slg
                        FROM batting b
                        JOIN players p ON b.player_id = p.player_id
                        WHERE b.team_id = ?
                        GROUP BY b.player_id, p.player_name, p.player_name_en
                        HAVING SUM(b.pa) >= ? AND SUM(b.b_ab) > 0
                        ORDER BY slg DESC
                        LIMIT 3
                    """, (team_id, b_qualifier * 0.5))
            else:
                # For counting stats
                query = f"""
                    SELECT p.player_name, p.player_name_en, p.player_id, SUM(b.{stat}) as stat_value
                    FROM batting b
                    JOIN players p ON b.player_id = p.player_id
                    WHERE b.team_id = ?
                    GROUP BY b.player_id, p.player_name, p.player_name_en
                    HAVING SUM(b.pa) >= ?
                    ORDER BY stat_value DESC
                    LIMIT 3
                """
                cursor = conn.execute(query, (team_id, b_qualifier * 0.5))
            
            leaders = []
            for row in cursor.fetchall():
                if stat in ['b_avg', 'b_obp', 'b_slg']:
                    stat_value = row[list(row.keys())[-1]]  # Last column
                    leaders.append({
                        'name': row['player_name_en'] or row['player_name'],
                        'player_id': row['player_id'],
                        'stat': f"{stat_value:.3f}"
                    })
                else:
                    leaders.append({
                        'name': row['player_name_en'] or row['player_name'],
                        'player_id': row['player_id'],
                        'stat': str(row['stat_value'])
                    })
            
            batting_leaders[stat.replace('b_', '')] = leaders
        
        # Pitching leaders for this team
        pitching_stats = ['era', 'whip', 'p_k', 'p_w', 'p_sv']
        pitching_leaders = {}
        
        for stat in pitching_stats:
            if stat == 'era':
                cursor = conn.execute("""
                    SELECT p2.player_name, p2.player_name_en, p.player_id,
                           ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / CAST(SUM(p.ip) AS FLOAT), 2) as era
                    FROM pitching p
                    JOIN players p2 ON p.player_id = p2.player_id
                    WHERE p.team_id = ?
                    GROUP BY p.player_id, p2.player_name, p2.player_name_en
                    HAVING SUM(p.ip) >= ?
                    ORDER BY era ASC
                    LIMIT 3
                """, (team_id, p_qualifier * 0.3))
            elif stat == 'whip':
                cursor = conn.execute("""
                    SELECT p2.player_name, p2.player_name_en, p.player_id,
                           ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / CAST(SUM(p.ip) AS FLOAT), 2) as whip
                    FROM pitching p
                    JOIN players p2 ON p.player_id = p2.player_id
                    WHERE p.team_id = ?
                    GROUP BY p.player_id, p2.player_name, p2.player_name_en
                    HAVING SUM(p.ip) >= ?
                    ORDER BY whip ASC
                    LIMIT 3
                """, (team_id, p_qualifier * 0.3))
            else:
                # For counting stats
                query = f"""
                    SELECT p2.player_name, p2.player_name_en, p.player_id, SUM(p.{stat}) as stat_value
                    FROM pitching p
                    JOIN players p2 ON p.player_id = p2.player_id
                    WHERE p.team_id = ?
                    GROUP BY p.player_id, p2.player_name, p2.player_name_en
                    HAVING SUM(p.ip) >= ?
                    ORDER BY stat_value DESC
                    LIMIT 3
                """
                cursor = conn.execute(query, (team_id, p_qualifier * 0.3))
            
            leaders = []
            for row in cursor.fetchall():
                if stat in ['era', 'whip']:
                    stat_value = row[list(row.keys())[-1]]  # Last column
                    leaders.append({
                        'name': row['player_name_en'] or row['player_name'],
                        'player_id': row['player_id'],
                        'stat': f"{stat_value:.2f}"
                    })
                else:
                    leaders.append({
                        'name': row['player_name_en'] or row['player_name'],
                        'player_id': row['player_id'],
                        'stat': str(row['stat_value'])
                    })
            
            pitching_leaders[stat.replace('p_', '')] = leaders
        
        return jsonify({
            'batting_leaders': batting_leaders,
            'pitching_leaders': pitching_leaders
        })
        
    except Exception as e:
        print(f"Team leaders error: {e}")
        return jsonify({'error': 'Failed to get team leaders'}), 500
    finally:
        conn.close()

@app.route('/api/stats')
def get_database_stats():
    """Get database statistics for homepage hero section"""
    conn = get_db_connection()
    
    try:
        # Count total players
        cursor = conn.execute("SELECT COUNT(*) as player_count FROM players")
        player_count = cursor.fetchone()['player_count']
        
        # Count total games
        cursor = conn.execute("SELECT COUNT(*) as game_count FROM games")
        game_count = cursor.fetchone()['game_count']
        
        # Count total event files (from event table)
        cursor = conn.execute("SELECT COUNT(*) as event_count FROM event")
        event_count = cursor.fetchone()['event_count']
        
        return jsonify({
            'players': player_count,
            'games': game_count,
            'events': event_count
        })
        
    except Exception as e:
        print(f"Database stats error: {e}")
        return jsonify({'error': 'Failed to get database stats'}), 500
    finally:
        conn.close()

@app.route('/api/players/<player_id>/batting')
def get_player_batting_stats(player_id):
    """Get comprehensive batting statistics for a player with filtering"""
    # Get filter parameters from request
    game_types = request.args.getlist('game_types[]')
    splits = request.args.getlist('splits[]')
    if not splits:
        splits = [request.args.get('split', 'overall')]
    
    # Convert empty list to None for default behavior
    if not game_types:
        game_types = None
    
    conn = get_db_connection()
    
    try:
        # Get player info
        cursor = conn.execute("SELECT player_name FROM players WHERE player_id = ?", (player_id,))
        player = cursor.fetchone()
        if not player:
            return jsonify({'error': 'Player not found'}), 404
        
        # Get event-based batting stats
        career_query, season_query, params = get_batting_stats_from_events(player_id, game_types, splits)
        
        # Execute career stats query
        cursor = conn.execute(career_query, params)
        career_stats = cursor.fetchone()
        
        # Execute season stats query
        cursor = conn.execute(season_query, params)
        season_stats = [dict(row) for row in cursor.fetchall()]
        
        # Get league wOBAs and park factors for dynamic wRC+ calculation
        league_wobas, woba_scale = get_league_wobas_and_scale()
        
        # Calculate dynamic wRC+ for season stats with park factors
        for season in season_stats:
            if season.get('woba') is not None:
                league_woba = league_wobas.get(season['season'], 0.320)  # Default to 0.320 if not found
                
                # Get weighted park factors for this season
                park_factor_offense = get_weighted_park_factors(player_id, [season['season']], 'batting')
                
                # Calculate park-adjusted wRC+
                # Formula: ((wOBA - league_wOBA) / wOBA_scale) / (park_factor) * 100 + 100
                # Park factor > 1.0 = hitter-friendly = divide to reduce wRC+
                # Park factor < 1.0 = pitcher-friendly = divide to increase wRC+
                raw_wrc_plus = ((season['woba'] - league_woba) / woba_scale) * 100 + 100
                season['wrc_plus'] = round(raw_wrc_plus / park_factor_offense, 0)
            else:
                season['wrc_plus'] = 100
        
        # Calculate career wRC+ (weighted by plate appearances)
        career_dict = dict(career_stats) if career_stats else {}
        if career_dict.get('woba') is not None and season_stats:
            # Calculate weighted average league wOBA based on plate appearances per season
            total_pa = 0
            weighted_league_woba = 0
            
            for season in season_stats:
                if season.get('pa') and season['season'] in league_wobas:
                    total_pa += season['pa']
                    weighted_league_woba += league_wobas[season['season']] * season['pa']
            
            if total_pa > 0:
                avg_league_woba = weighted_league_woba / total_pa
                # Get career-wide park factor
                all_seasons = [s['season'] for s in season_stats if s.get('season')]
                park_factor_career = get_weighted_park_factors(player_id, all_seasons, 'batting')
                raw_career_wrc_plus = ((career_dict['woba'] - avg_league_woba) / woba_scale) * 100 + 100
                career_dict['wrc_plus'] = round(raw_career_wrc_plus / park_factor_career, 0)
            else:
                career_dict['wrc_plus'] = 100
        else:
            career_dict['wrc_plus'] = 100
        
        return jsonify({
            'player_name': player['player_name'],
            'career': career_dict,
            'seasons': season_stats
        })
        
    except Exception as e:
        print(f"Player batting stats error: {e}")
        return jsonify({'error': 'Failed to get batting stats'}), 500
    finally:
        conn.close()

@app.route('/api/players/<player_id>/pitching')
def get_player_pitching_stats(player_id):
    """Get comprehensive pitching statistics for a player with filtering"""
    # Get filter parameters from request
    game_types = request.args.getlist('game_types[]')
    splits = request.args.getlist('splits[]')
    if not splits:
        splits = [request.args.get('split', 'overall')]
    
    # Convert empty list to None for default behavior
    if not game_types:
        game_types = None
    
    conn = get_db_connection()
    
    try:
        # Get player info
        cursor = conn.execute("SELECT player_name FROM players WHERE player_id = ?", (player_id,))
        player = cursor.fetchone()
        if not player:
            return jsonify({'error': 'Player not found'}), 404
        
        # Get event-based pitching stats
        career_query, season_query, params = get_pitching_stats_from_events(player_id, game_types, splits)
        
        # Execute career stats query
        cursor = conn.execute(career_query, params)
        career_stats = cursor.fetchone()
        
        # Execute season stats query
        cursor = conn.execute(season_query, params)
        season_stats = [dict(row) for row in cursor.fetchall()]
        
        # Get league ERAs and FIP constants for calculations
        league_eras, fip_constants = get_league_era_and_fip_constants()
        
        # Calculate ERA+ and FIP for season stats with park factors
        for season in season_stats:
            # Calculate park-adjusted ERA+
            if season['era'] and season['era'] > 0:
                league_era = league_eras.get(season['season'], 4.00)  # Default to 4.00 if not found
                
                # Get weighted park factors for this season (pitching perspective)
                park_factor_pitching = get_weighted_park_factors(player_id, [season['season']], 'pitching')
                
                # Park-adjusted ERA+ formula: 100 * (league_era / player_era) * park_factor
                # Higher park factor = more hitter-friendly = multiply to increase ERA+
                # Lower park factor = more pitcher-friendly = multiply to decrease ERA+
                raw_era_plus = 100 * (league_era / season['era'])
                season['era_plus'] = round(raw_era_plus * park_factor_pitching)
            else:
                season['era_plus'] = 100
            
            # Calculate FIP with dynamic constant
            if season['raw_fip'] is not None:
                fip_constant = fip_constants.get(season['season'], 3.10)  # Default to 3.10 if not found
                season['fip'] = round(season['raw_fip'] + fip_constant, 2)
            else:
                season['fip'] = None
        
        # Calculate career ERA+ and FIP (weighted averages)
        career_dict = dict(career_stats) if career_stats else {}
        
        # Career ERA+
        if career_dict.get('era') and career_dict['era'] > 0:
            # Calculate weighted average league ERA based on innings pitched per season
            total_ip = 0
            weighted_league_era = 0
            
            for season in season_stats:
                if season.get('ip') and season['season'] in league_eras:
                    total_ip += season['ip']
                    weighted_league_era += league_eras[season['season']] * season['ip']
            
            if total_ip > 0:
                avg_league_era = weighted_league_era / total_ip
                # Get career-wide park factor for pitching
                all_seasons = [s['season'] for s in season_stats if s.get('season')]
                park_factor_career = get_weighted_park_factors(player_id, all_seasons, 'pitching')
                raw_career_era_plus = 100 * (avg_league_era / career_dict['era'])
                career_dict['era_plus'] = round(raw_career_era_plus * park_factor_career)
            else:
                career_dict['era_plus'] = 100
        else:
            career_dict['era_plus'] = 100
        
        # Career FIP
        if career_dict.get('raw_fip') is not None:
            # Calculate weighted average FIP constant based on innings pitched per season
            total_ip = 0
            weighted_fip_constant = 0
            
            for season in season_stats:
                if season.get('ip') and season['season'] in fip_constants:
                    total_ip += season['ip']
                    weighted_fip_constant += fip_constants[season['season']] * season['ip']
            
            if total_ip > 0:
                avg_fip_constant = weighted_fip_constant / total_ip
                career_dict['fip'] = round(career_dict['raw_fip'] + avg_fip_constant, 2)
            else:
                career_dict['fip'] = round(career_dict['raw_fip'] + 3.10, 2)  # Default constant
        else:
            career_dict['fip'] = None
        
        return jsonify({
            'player_name': player['player_name'],
            'career': career_dict,
            'seasons': season_stats
        })
        
    except Exception as e:
        print(f"Player pitching stats error: {e}")
        return jsonify({'error': 'Failed to get pitching stats'}), 500
    finally:
        conn.close()

@app.route('/api/teams/<team_id>')
def get_team_info(team_id):
    """Get team metadata"""
    conn = get_db_connection()
    
    try:
        cursor = conn.execute("""
            SELECT team_id, team_name, team_name_en, first_year, ballpark, league, b_qualifier, p_qualifier
            FROM teams 
            WHERE team_id = ?
        """, (team_id,))
        
        team = cursor.fetchone()
        if not team:
            return jsonify({'error': 'Team not found'}), 404
            
        return jsonify({
            'team_id': team['team_id'],
            'team_name': team['team_name'],
            'team_name_en': team['team_name_en'],
            'first_year': team['first_year'],
            'ballpark': team['ballpark'],
            'league': team['league'],
            'b_qualifier': team['b_qualifier'],
            'p_qualifier': team['p_qualifier']
        })
        
    except Exception as e:
        print(f"Team info error: {e}")
        return jsonify({'error': 'Failed to get team info'}), 500
    finally:
        conn.close()

@app.route('/api/teams/<team_id>/record')
def get_team_record(team_id):
    """Get team's current season record"""
    conn = get_db_connection()
    
    try:
        # Get current season
        cursor = conn.execute("SELECT MAX(season) as current_season FROM games")
        current_season = cursor.fetchone()['current_season']
        
        # Get team record for current season
        cursor = conn.execute("""
            SELECT 
                SUM(CASE 
                    WHEN (home_team_id = ? AND home_runs > visitor_runs) OR 
                         (away_team_id = ? AND visitor_runs > home_runs) 
                    THEN 1 ELSE 0 END) as wins,
                SUM(CASE 
                    WHEN (home_team_id = ? AND home_runs < visitor_runs) OR 
                         (away_team_id = ? AND visitor_runs < home_runs) 
                    THEN 1 ELSE 0 END) as losses,
                SUM(CASE 
                    WHEN (home_team_id = ? OR away_team_id = ?) AND home_runs = visitor_runs 
                    THEN 1 ELSE 0 END) as ties
            FROM games 
            WHERE season = ? AND (home_team_id = ? OR away_team_id = ?)
        """, (team_id, team_id, team_id, team_id, team_id, team_id, current_season, team_id, team_id))
        
        record = cursor.fetchone()
        wins = record['wins'] or 0
        losses = record['losses'] or 0
        ties = record['ties'] or 0
        
        # Calculate win percentage
        total_games = wins + losses
        win_pct = wins / total_games if total_games > 0 else 0.000
        
        return jsonify({
            'wins': wins,
            'losses': losses,
            'ties': ties,
            'win_pct': win_pct,
            'season': current_season
        })
        
    except Exception as e:
        print(f"Team record error: {e}")
        return jsonify({'error': 'Failed to get team record'}), 500
    finally:
        conn.close()

@app.route('/api/teams/<team_id>/recent-games')
def get_team_recent_games(team_id):
    """Get recent games for a team"""
    # Get query parameters
    limit = request.args.get('limit', 10, type=int)
    offset = request.args.get('offset', 0, type=int)
    game_types = request.args.getlist('game_types[]')
    
    # Validate limit
    if limit < 1 or limit > 50:
        limit = 10
    
    conn = get_db_connection()
    
    try:
        # Build base query
        base_query = """
            SELECT 
                g.game_id,
                g.date,
                g.home_team_id,
                g.away_team_id,
                g.home_runs,
                g.visitor_runs,
                g.home_hits,
                g.visitor_hits,
                g.home_errors,
                g.visitor_errors,
                g.ballpark,
                b.park_name,
                b.park_name_en,
                g.gametype,
                g.game_number,
                g.season,
                g.winning_team_id,
                g.losing_team_id,
                CASE 
                    WHEN g.home_team_id = ? THEN 'home'
                    ELSE 'away'
                END as home_away,
                CASE 
                    WHEN g.home_team_id = ? THEN g.away_team_id
                    ELSE g.home_team_id
                END as opponent,
                CASE 
                    WHEN g.home_team_id = ? THEN g.home_runs
                    ELSE g.visitor_runs
                END as team_runs,
                CASE 
                    WHEN g.home_team_id = ? THEN g.visitor_runs
                    ELSE g.home_runs
                END as opponent_runs,
                CASE 
                    WHEN g.home_team_id = ? THEN g.home_hits
                    ELSE g.visitor_hits
                END as team_hits,
                CASE 
                    WHEN g.home_team_id = ? THEN g.visitor_hits
                    ELSE g.home_hits
                END as opponent_hits,
                CASE 
                    WHEN g.home_team_id = ? THEN g.home_errors
                    ELSE g.visitor_errors
                END as team_errors,
                CASE 
                    WHEN g.home_team_id = ? THEN g.visitor_errors
                    ELSE g.home_errors
                END as opponent_errors,
                CASE 
                    WHEN g.winning_team_id = ? THEN 'W'
                    WHEN g.losing_team_id = ? THEN 'L'
                    ELSE 'T'
                END as result
            FROM games g
            LEFT JOIN ballparks b ON g.ballpark = b.park_name
            WHERE (g.home_team_id = ? OR g.away_team_id = ?)
        """
        
        params = [team_id, team_id, team_id, team_id, team_id, team_id, team_id, team_id, team_id, team_id, team_id, team_id]
        
        # Add game type filter if specified
        if game_types and len(game_types) > 0:
            placeholders = ','.join(['?' for _ in game_types])
            base_query += f" AND g.gametype IN ({placeholders})"
            params.extend(game_types)
        
        # Order by date descending and limit with offset
        base_query += " ORDER BY g.date DESC, g.game_id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor = conn.execute(base_query, params)
        games = [dict(row) for row in cursor.fetchall()]
        
        # Get total count for pagination info
        count_query = """
            SELECT COUNT(*) as total
            FROM games g
            WHERE (g.home_team_id = ? OR g.away_team_id = ?)
        """
        count_params = [team_id, team_id]
        
        # Add same game type filter for count
        if game_types and len(game_types) > 0:
            placeholders = ','.join(['?' for _ in game_types])
            count_query += f" AND g.gametype IN ({placeholders})"
            count_params.extend(game_types)
        
        cursor = conn.execute(count_query, count_params)
        total_games = cursor.fetchone()['total']
        
        # Get team names for display
        team_names = {}
        if games:
            all_teams = set()
            for game in games:
                all_teams.add(game['opponent'])
                all_teams.add(team_id)
            
            if all_teams:
                placeholders = ','.join(['?' for _ in all_teams])
                cursor = conn.execute(f"SELECT team_id, team_name, team_name_en FROM teams WHERE team_id IN ({placeholders})", list(all_teams))
                team_names = {row['team_id']: {'name': row['team_name'], 'name_en': row['team_name_en']} for row in cursor.fetchall()}
        
        # Add team names to games
        for game in games:
            opponent_info = team_names.get(game['opponent'], {'name': game['opponent'], 'name_en': game['opponent']})
            team_info = team_names.get(team_id, {'name': team_id, 'name_en': team_id})
            
            game['opponent_name'] = opponent_info['name']
            game['opponent_name_en'] = opponent_info['name_en']
            game['team_name'] = team_info['name']
            game['team_name_en'] = team_info['name_en']
        
        return jsonify({
            'games': games,
            'total': total_games,
            'offset': offset,
            'limit': limit,
            'has_more': offset + len(games) < total_games
        })
        
    except Exception as e:
        print(f"Recent games error: {e}")
        return jsonify({'error': 'Failed to get recent games'}), 500
    finally:
        conn.close()

# --- STANDINGS ENDPOINTS ---

@app.route('/api/standings')
def get_standings():
    """Get team standings organized by league for specified or current season"""
    season = request.args.get('season')
    conn = get_db_connection()
    try:
        # If no season specified, get current season
        if not season:
            cursor = conn.execute("""
                SELECT MAX(season) as current_season
                FROM games
            """)
            season = cursor.fetchone()['current_season']
        
        print(f"Getting standings for season: {season}")
        
        # Get all teams in Central and Pacific leagues
        cursor = conn.execute("""
            SELECT team_id, team_name, team_name_en, league
            FROM teams 
            WHERE league IN ('cl', 'pl')
            ORDER BY league, team_name
        """)
        teams = [dict(row) for row in cursor.fetchall()]
        
        # Calculate record for each team (using same logic as get_team_record)
        for team in teams:
            team_id = team['team_id']
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as games,
                    SUM(CASE 
                        WHEN (home_team_id = ? AND home_runs > visitor_runs) OR 
                             (away_team_id = ? AND visitor_runs > home_runs) 
                        THEN 1 ELSE 0 END) as wins,
                    SUM(CASE 
                        WHEN (home_team_id = ? AND home_runs < visitor_runs) OR 
                             (away_team_id = ? AND visitor_runs < home_runs) 
                        THEN 1 ELSE 0 END) as losses,
                    SUM(CASE 
                        WHEN (home_team_id = ? OR away_team_id = ?) AND home_runs = visitor_runs 
                        THEN 1 ELSE 0 END) as ties
                FROM games 
                WHERE season = ? AND gametype = '公式戦' AND (home_team_id = ? OR away_team_id = ?)
            """, (team_id, team_id, team_id, team_id, team_id, team_id, season, team_id, team_id))
            
            record = cursor.fetchone()
            team['games'] = record['games'] or 0
            team['wins'] = record['wins'] or 0
            team['losses'] = record['losses'] or 0
            team['ties'] = record['ties'] or 0
        
        # Calculate win percentage and organize by league
        central_league = []
        pacific_league = []
        
        for team in teams:
            # Calculate win percentage
            total_games = team['wins'] + team['losses'] + team['ties']
            if total_games > 0:
                team['win_pct'] = round(team['wins'] / (team['wins'] + team['losses']) if (team['wins'] + team['losses']) > 0 else 0, 3)
            else:
                team['win_pct'] = 0.000
            
            # Organize by league
            if team['league'] == 'cl':
                central_league.append(team)
            elif team['league'] == 'pl':
                pacific_league.append(team)
        
        # Sort by win percentage (descending)
        central_league.sort(key=lambda x: x['win_pct'], reverse=True)
        pacific_league.sort(key=lambda x: x['win_pct'], reverse=True)
        
        # Calculate games behind for each league
        def calculate_games_behind(league_teams):
            if not league_teams:
                return league_teams
            
            leader = league_teams[0]
            leader_wins = leader['wins']
            leader_losses = leader['losses']
            
            for i, team in enumerate(league_teams):
                if i == 0:
                    team['games_behind'] = 0.0
                    team['place'] = 1
                else:
                    gb = ((leader_wins - team['wins']) + (team['losses'] - leader_losses)) / 2
                    team['games_behind'] = round(gb, 1)
                    team['place'] = i + 1
            
            return league_teams
        
        central_league = calculate_games_behind(central_league)
        pacific_league = calculate_games_behind(pacific_league)
        
        return jsonify({
            'central_league': central_league,
            'pacific_league': pacific_league,
            'season': season
        })
        
    except Exception as e:
        print(f"Standings error: {e}")
        return jsonify({'error': 'Failed to get standings'}), 500
    finally:
        conn.close()

@app.route('/api/seasons')
def get_seasons():
    """Get all available seasons"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
            SELECT DISTINCT season
            FROM games
            ORDER BY season DESC
        """)
        seasons = [row['season'] for row in cursor.fetchall()]
        
        return jsonify({
            'seasons': seasons
        })
    except Exception as e:
        print(f"Seasons error: {e}")
        return jsonify({'error': 'Failed to get seasons'}), 500
    finally:
        conn.close()

# --- PAGE ROUTES ---

@app.route('/advanced')
def advanced_stats():
    """Serve the advanced stats page"""
    return render_template('advanced.html')


# --- BALLPARK ENDPOINTS ---

@app.route('/ballparks')
def ballparks_listing():
    """Serve the ballparks listing page"""
    return render_template('ballparks.html')

@app.route('/ballparks/<park_name>')
def ballpark_page(park_name):
    """Serve the ballpark page"""
    return render_template('ballpark.html')

@app.route('/api/ballparks/<park_name>')
def get_ballpark(park_name):
    """Get ballpark metadata"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
            SELECT b.*, t.team_name, t.team_name_en 
            FROM ballparks b
            LEFT JOIN teams t ON b.home_team = t.team_id
            WHERE b.park_name = ? OR b.park_name_en = ?
        """, (park_name, park_name))
        
        ballpark = cursor.fetchone()
        if not ballpark:
            return jsonify({'error': 'Ballpark not found'}), 404
            
        return jsonify(dict(ballpark))
        
    except Exception as e:
        print(f"Ballpark error: {e}")
        return jsonify({'error': 'Failed to get ballpark data'}), 500
    finally:
        conn.close()

@app.route('/api/ballparks')
def get_all_ballparks():
    """Get all ballparks"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
            SELECT b.park_name, b.park_name_en, b.city, b.home_team, 
                   b.pf_runs, b.games_sample_size, b.pf_confidence,
                   t.team_name, t.team_name_en
            FROM ballparks b
            LEFT JOIN teams t ON b.home_team = t.team_id
            ORDER BY b.park_name_en
        """)
        
        ballparks = [dict(row) for row in cursor.fetchall()]
        return jsonify({'ballparks': ballparks})
        
    except Exception as e:
        print(f"Ballparks list error: {e}")
        return jsonify({'error': 'Failed to get ballparks'}), 500
    finally:
        conn.close()

@app.route('/api/ballparks/<park_name>/batting')
def get_ballpark_batting_stats(park_name):
    """Get comprehensive batting statistics for a ballpark with filtering"""
    game_types = request.args.getlist('game_types[]')
    splits = request.args.getlist('splits[]')
    if not splits:
        splits = [request.args.get('split', 'overall')]
    if not game_types:
        game_types = None
    
    conn = get_db_connection()
    try:
        base_query = """
            FROM batting b
            JOIN games g ON b.game_id = g.game_id
            WHERE g.ballpark = ? OR g.ballpark = (SELECT park_name FROM ballparks WHERE park_name_en = ?)
        """
        params = [park_name, park_name]
        
        if game_types and len(game_types) > 0:
            placeholders = ','.join(['?' for _ in game_types])
            base_query += f" AND g.gametype IN ({placeholders})"
            params.extend(game_types)
            
        if splits and 'overall' not in [s.lower() for s in splits]:
            for split in splits:
                split_lower = split.lower()
                if split_lower in ['home', 'road']:
                    if split_lower == 'home':
                        base_query += " AND b.team = g.home_team_id"
                    else:
                        base_query += " AND b.team = g.away_team_id"
                elif split_lower in ['wins', 'losses']:
                    if split_lower == 'wins':
                        base_query += " AND b.team = g.winning_team_id"
                    else:
                        base_query += " AND b.team = g.losing_team_id"
        
        career_query = f"""
            SELECT 
                COUNT(DISTINCT b.game_id) as g,
                SUM(b.pa) as pa,
                SUM(b.ab) as ab,
                SUM(b.b_h) as h,
                SUM(b.b_r) as r,
                SUM(b.b_2b) as doubles,
                SUM(b.b_3b) as triples,
                SUM(b.b_hr) as hr,
                SUM(b.b_rbi) as rbi,
                SUM(b.b_k) as so,
                SUM(b.b_bb) as bb,
                SUM(b.b_hbp) as hbp,
                SUM(b.b_sac) as sac,
                SUM(b.b_gdp) as gidp,
                SUM(b.b_roe) as roe,
                ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
                ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) + CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as ops,
                SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) as tb,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
                ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip
            {base_query}
        """
        
        season_query = f"""
            SELECT 
                g.season,
                COUNT(DISTINCT b.game_id) as g,
                SUM(b.pa) as pa,
                SUM(b.ab) as ab,
                SUM(b.b_h) as h,
                SUM(b.b_r) as r,
                SUM(b.b_2b) as doubles,
                SUM(b.b_3b) as triples,
                SUM(b.b_hr) as hr,
                SUM(b.b_rbi) as rbi,
                SUM(b.b_k) as so,
                SUM(b.b_bb) as bb,
                SUM(b.b_hbp) as hbp,
                SUM(b.b_sac) as sac,
                SUM(b.b_gdp) as gidp,
                SUM(b.b_roe) as roe,
                ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
                ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) + CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as ops,
                SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) as tb,
                ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
                ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip
            {base_query}
            GROUP BY g.season
            ORDER BY g.season ASC
        """
        
        cur = conn.execute(career_query, params)
        career = cur.fetchone()
        cur = conn.execute(season_query, params)
        seasons = [dict(row) for row in cur.fetchall()]
        
        return jsonify({
            'seasons': seasons,
            'career': dict(career) if career else {}
        })
        
    except Exception as e:
        print(f"Ballpark batting stats error: {e}")
        return jsonify({'error': 'Failed to get ballpark batting stats'}), 500
    finally:
        conn.close()

@app.route('/api/ballparks/<park_name>/pitching')
def get_ballpark_pitching_stats(park_name):
    """Get comprehensive pitching statistics for a ballpark with filtering"""
    game_types = request.args.getlist('game_types[]')
    splits = request.args.getlist('splits[]')
    if not splits:
        splits = [request.args.get('split', 'overall')]
    if not game_types:
        game_types = None
    
    conn = get_db_connection()
    try:
        base_query = """
            FROM pitching p
            JOIN games g ON p.game_id = g.game_id
            WHERE g.ballpark = ? OR g.ballpark = (SELECT park_name FROM ballparks WHERE park_name_en = ?)
        """
        params = [park_name, park_name]
        
        if game_types and len(game_types) > 0:
            placeholders = ','.join(['?' for _ in game_types])
            base_query += f" AND g.gametype IN ({placeholders})"
            params.extend(game_types)
            
        if splits and 'overall' not in [s.lower() for s in splits]:
            for split in splits:
                split_lower = split.lower()
                if split_lower in ['home', 'road']:
                    if split_lower == 'home':
                        base_query += " AND p.team = g.home_team_id"
                    else:
                        base_query += " AND p.team = g.away_team_id"
                elif split_lower in ['wins', 'losses']:
                    if split_lower == 'wins':
                        base_query += " AND p.team = g.winning_team_id"
                    else:
                        base_query += " AND p.team = g.losing_team_id"
        
        career_query = f"""
            SELECT 
                COUNT(DISTINCT p.game_id) as g,
                COUNT(*) as app,
                SUM(p.win) as w,
                SUM(p.loss) as l,
                SUM(p.save) as sv,
                SUM(p.hold) as hld,
                SUM(p.start) as gs,
                SUM(p.finish) as cg,
                SUM(p.ip) as ip,
                SUM(p.pitches_thrown) as pitches,
                SUM(p.batters_faced) as bf,
                SUM(p.r) as r,
                SUM(p.er) as er,
                SUM(p.p_h) as h,
                SUM(p.p_hr) as hr,
                SUM(p.p_k) as k,
                SUM(p.p_bb) as bb,
                SUM(p.p_hbp) as hbp,
                SUM(p.p_2b) as doubles,
                SUM(p.p_3b) as triples,
                SUM(p.p_gb) as gb,
                SUM(p.p_fb) as fb,
                SUM(p.wild_pitch) as wp,
                SUM(p.balk) as bk,
                SUM(p.p_roe) as roe,
                SUM(p.p_gdp) as gidp,
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
                ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,
                ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as k9,
                ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as bb9,
                ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as hr9,
                ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as fo_pct,
                ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as go_pct,
                ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gb), 0), 3) as gidp_pct,
                ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
                ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
                0 as era_plus,
                ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_hr) - SUM(p.p_bb) - SUM(p.p_hbp), 0), 3) as babip
            {base_query}
        """
        
        season_query = f"""
            SELECT 
                g.season,
                COUNT(DISTINCT p.game_id) as g,
                COUNT(*) as app,
                SUM(p.win) as w,
                SUM(p.loss) as l,
                SUM(p.save) as sv,
                SUM(p.hold) as hld,
                SUM(p.start) as gs,
                SUM(p.finish) as cg,
                SUM(p.ip) as ip,
                SUM(p.pitches_thrown) as pitches,
                SUM(p.batters_faced) as bf,
                SUM(p.r) as r,
                SUM(p.er) as er,
                SUM(p.p_h) as h,
                SUM(p.p_hr) as hr,
                SUM(p.p_k) as k,
                SUM(p.p_bb) as bb,
                SUM(p.p_hbp) as hbp,
                SUM(p.p_2b) as doubles,
                SUM(p.p_3b) as triples,
                SUM(p.p_gb) as gb,
                SUM(p.p_fb) as fb,
                SUM(p.wild_pitch) as wp,
                SUM(p.balk) as bk,
                SUM(p.p_roe) as roe,
                SUM(p.p_gdp) as gidp,
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
                ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,
                ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as k9,
                ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as bb9,
                ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as hr9,
                ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as fo_pct,
                ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.p_gb) + SUM(p.p_fb), 0), 3) as go_pct,
                ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gb), 0), 3) as gidp_pct,
                ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
                ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
                0 as era_plus,
                ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_hr) - SUM(p.p_bb) - SUM(p.p_hbp), 0), 3) as babip
            {base_query}
            GROUP BY g.season
            ORDER BY g.season ASC
        """
        
        cur = conn.execute(career_query, params)
        career = cur.fetchone()
        cur = conn.execute(season_query, params)
        seasons = [dict(row) for row in cur.fetchall()]
        
        # Add placeholder values for FIP and ERA+ and format IP
        for season in seasons:
            season['fip'] = season.get('raw_fip', 0.00) if season.get('raw_fip') else 0.00
            season['era_plus'] = 100
            season['ip'] = format_innings_pitched(season.get('ip'))
        
        # Handle career data
        if career:
            career_dict = dict(career)
            career_dict['fip'] = career_dict.get('raw_fip', 0.00) if career_dict.get('raw_fip') else 0.00
            career_dict['era_plus'] = 100
            career_dict['ip'] = format_innings_pitched(career_dict.get('ip'))
        else:
            career_dict = {}
        
        return jsonify({
            'seasons': seasons,
            'career': career_dict
        })
        
    except Exception as e:
        print(f"Ballpark pitching stats error: {e}")
        return jsonify({'error': 'Failed to get ballpark pitching stats'}), 500
    finally:
        conn.close()

@app.route('/api/ballparks/<park_name>/recent-games')
def get_ballpark_recent_games(park_name):
    """Get recent games played at a ballpark"""
    limit = request.args.get('limit', 10, type=int)
    offset = request.args.get('offset', 0, type=int)
    game_type = request.args.get('game_type', '')
    
    conn = get_db_connection()
    try:
        base_query = """
            SELECT g.game_id, g.date, g.season, g.gametype, g.game_number,
                   g.home_team_id, g.away_team_id, 
                   g.home_runs, g.visitor_runs as away_runs,
                   g.home_hits, g.visitor_hits as away_hits, 
                   g.home_errors, g.visitor_errors as away_errors,
                   g.ballpark, g.attendance, g.winning_team_id,
                   ht.team_name as home_team_name, ht.team_name_en as home_team_name_en,
                   at.team_name as away_team_name, at.team_name_en as away_team_name_en,
                   b.park_name, b.park_name_en
            FROM games g
            LEFT JOIN teams ht ON g.home_team_id = ht.team_id
            LEFT JOIN teams at ON g.away_team_id = at.team_id
            LEFT JOIN ballparks b ON g.ballpark = b.park_name OR g.ballpark = b.park_name_en
            WHERE (g.ballpark = ? OR g.ballpark = (SELECT park_name FROM ballparks WHERE park_name_en = ?))
        """
        params = [park_name, park_name]
        
        if game_type:
            base_query += " AND g.gametype = ?"
            params.append(game_type)
        
        base_query += " ORDER BY g.date DESC, g.game_id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor = conn.execute(base_query, params)
        games = [dict(row) for row in cursor.fetchall()]
        
        return jsonify({'games': games})
        
    except Exception as e:
        print(f"Ballpark recent games error: {e}")
        return jsonify({'error': 'Failed to get recent games'}), 500
    finally:
        conn.close()

@app.route('/api/ballparks/stats')
def get_ballparks_stats():
    """Get combined batting and pitching statistics for all ballparks"""
    season_filter = request.args.get('season', 'all')  # 'current' or 'all'
    
    conn = get_db_connection()
    try:
        # Get current season
        cursor = conn.execute("SELECT MAX(season) as current_season FROM games")
        current_season = cursor.fetchone()['current_season']
        
        # Build season filter
        season_condition = ""
        params = []
        if season_filter == 'current':
            season_condition = "AND g.season = ?"
            params.append(current_season)
        
        # Query to get combined stats for all ballparks
        query = f"""
            SELECT 
                COALESCE(b.park_name_en, b.park_name, g.ballpark) as name,
                COUNT(DISTINCT g.game_id) as g,
                b.pf_runs as pf,
                
                -- Batting stats (from batting table)
                SUM(bat.pa) as pa,
                SUM(bat.ab) as ab,
                SUM(bat.b_h) as h,
                SUM(bat.b_r) as r,
                SUM(bat.b_hr) as hr,
                SUM(bat.b_rbi) as rbi,
                SUM(bat.b_k) as k,
                SUM(bat.b_bb) as bb,
                ROUND(CAST(SUM(bat.b_h) AS FLOAT) / NULLIF(SUM(bat.ab), 0), 3) as avg,
                ROUND(CAST((SUM(bat.b_h) - SUM(bat.b_2b) - SUM(bat.b_3b) - SUM(bat.b_hr)) + 2*SUM(bat.b_2b) + 3*SUM(bat.b_3b) + 4*SUM(bat.b_hr) AS FLOAT) / NULLIF(SUM(bat.ab), 0), 3) as slg,
                ROUND(CAST(SUM(bat.b_h) - SUM(bat.b_hr) AS FLOAT) / NULLIF(SUM(bat.ab) - SUM(bat.b_k) - SUM(bat.b_hr), 0), 3) as babip,
                
                -- Pitching stats (ERA and WHIP from pitching table)
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
                ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,
                SUM(p.ip) as ip
                
            FROM games g
            LEFT JOIN ballparks b ON g.ballpark = b.park_name OR g.ballpark = b.park_name_en
            LEFT JOIN batting bat ON g.game_id = bat.game_id
            LEFT JOIN pitching p ON g.game_id = p.game_id
            WHERE g.ballpark IS NOT NULL
            {season_condition}
            GROUP BY COALESCE(b.park_name_en, b.park_name, g.ballpark), b.pf_runs
            HAVING SUM(bat.pa) > 0 AND SUM(p.ip) > 0
            ORDER BY COUNT(DISTINCT g.game_id) DESC
        """
        
        cursor = conn.execute(query, params)
        ballparks = [dict(row) for row in cursor.fetchall()]
        
        # Format innings pitched for display
        for ballpark in ballparks:
            if ballpark['ip']:
                ballpark['ip'] = format_innings_pitched(ballpark['ip'])
            if ballpark['pf'] is None:
                ballpark['pf'] = 1.000
        
        return jsonify({
            'ballparks': ballparks,
            'season': current_season if season_filter == 'current' else 'All-Time'
        })
        
    except Exception as e:
        print(f"Ballparks stats error: {e}")
        return jsonify({'error': 'Failed to get ballpark stats'}), 500
    finally:
        conn.close()

# --- HOMEPAGE STATISTICS ENDPOINTS ---

@app.route('/api/players/count')
def get_players_count():
    """Get total count of players in database"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT COUNT(DISTINCT player_id) as count FROM players")
        result = cursor.fetchone()
        return jsonify({'count': result['count'] if result else 0})
    except Exception as e:
        print(f"Players count error: {e}")
        return jsonify({'error': 'Failed to get players count'}), 500
    finally:
        conn.close()

@app.route('/api/games/count')
def get_games_count():
    """Get total count of games in database"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT COUNT(DISTINCT game_id) as count FROM games")
        result = cursor.fetchone()
        return jsonify({'count': result['count'] if result else 0})
    except Exception as e:
        print(f"Games count error: {e}")
        return jsonify({'error': 'Failed to get games count'}), 500
    finally:
        conn.close()

@app.route('/api/events/count')
def get_events_count():
    """Get total count of event files in database"""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT COUNT(*) as count FROM event")
        result = cursor.fetchone()
        return jsonify({'count': result['count'] if result else 0})
    except Exception as e:
        print(f"Events count error: {e}")
        return jsonify({'error': 'Failed to get events count'}), 500
    finally:
        conn.close()

@app.route('/api/leaders/batting/<stat>')
def get_batting_leaders(stat):
    """Get batting leaders for a specific stat"""
    limit = request.args.get('limit', 5, type=int)
    season = request.args.get('season', None)
    
    # Map stat names to database columns
    stat_mapping = {
        'hits': 'b_h',
        'hr': 'b_hr', 
        'rbi': 'b_rbi',
        'avg': 'avg',
        'obp': 'obp',
        'slg': 'slg',
        'ops': 'ops'
    }
    
    if stat not in stat_mapping:
        return jsonify({'error': 'Invalid stat'}), 400
    
    conn = get_db_connection()
    try:
        # Build query based on stat type
        if stat in ['hits', 'hr', 'rbi']:
            # For counting stats, sum the values
            query = f"""
                SELECT 
                    p.player_id,
                    p.player_name,
                    SUM(b.{stat_mapping[stat]}) as stat_value
                FROM batting b
                JOIN players p ON b.player_id = p.player_id
                JOIN games g ON b.game_id = g.game_id
                WHERE 1=1
            """
            params = []
            
            if season:
                query += " AND g.season = ?"
                params.append(season)
            
            query += f"""
                GROUP BY p.player_id, p.player_name
                HAVING SUM(b.pa) >= 100
                ORDER BY stat_value DESC
                LIMIT ?
            """
            params.append(limit)
            
        else:
            # For rate stats, calculate them
            if stat == 'avg':
                stat_calc = "ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3)"
            elif stat == 'obp':
                stat_calc = "ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3)"
            elif stat == 'slg':
                stat_calc = "ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3)"
            elif stat == 'ops':
                stat_calc = """
                    ROUND(
                        (CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0)) +
                        (CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0)),
                        3
                    )
                """
            
            query = f"""
                SELECT 
                    p.player_id,
                    p.player_name,
                    {stat_calc} as stat_value
                FROM batting b
                JOIN players p ON b.player_id = p.player_id
                JOIN games g ON b.game_id = g.game_id
                WHERE 1=1
            """
            params = []
            
            if season:
                query += " AND g.season = ?"
                params.append(season)
            
            query += f"""
                GROUP BY p.player_id, p.player_name
                HAVING SUM(b.pa) >= 100
                ORDER BY stat_value DESC
                LIMIT ?
            """
            params.append(limit)
        
        cursor = conn.execute(query, params)
        leaders = []
        for row in cursor.fetchall():
            leaders.append({
                'player_id': row['player_id'],
                'player_name': row['player_name'],
                'stat_value': row['stat_value']
            })
        
        return jsonify(leaders)
        
    except Exception as e:
        print(f"Stat leaders error: {e}")
        return jsonify({'error': 'Failed to get stat leaders'}), 500
    finally:
        conn.close()

@app.route('/api/games/advanced')
def games_advanced():
    """Advanced game lookup with filtering and pagination"""
    try:
        conn = get_db_connection()
        
        # Get query parameters
        sort_by = request.args.get('sort_by', 'date')
        sort_order = request.args.get('sort_order', 'DESC')
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 25))
        
        # Filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        home_team = request.args.get('home_team')
        away_team = request.args.get('away_team')
        ballpark = request.args.get('ballpark')
        game_type = request.args.get('game_type')
        
        # Complex filters (JSON format)
        filters_json = request.args.get('filters')
        complex_filters = []
        if filters_json:
            try:
                complex_filters = json.loads(filters_json)
            except:
                complex_filters = []
        
        # Build base query with team names and English translations
        query = """
            SELECT 
                g.game_id,
                g.date,
                g.home_team_id,
                g.away_team_id,
                COALESCE(ht.team_name_en, ht.team_name, g.home_team_id) as home_team_name,
                COALESCE(at.team_name_en, at.team_name, g.away_team_id) as away_team_name,
                g.home_runs as home_score,
                g.visitor_runs as away_score,
                g.game_number,
                g.ballpark,
                COALESCE(bp.park_name_en, g.ballpark) as ballpark_en,
                g.gametype,
                CASE 
                    WHEN g.gametype = 'R' THEN 'Regular Season'
                    WHEN g.gametype = 'P' THEN 'Playoffs'
                    WHEN g.gametype = 'C' THEN 'Climax Series'
                    WHEN g.gametype = 'J' THEN 'Japan Series'
                    WHEN g.gametype = 'E' THEN 'Exhibition'
                    WHEN g.gametype = 'A' THEN 'All-Star'
                    ELSE g.gametype
                END as gametype_en,
                g.attendance,
                g.home_hits,
                g.visitor_hits as away_hits,
                g.home_errors,
                g.visitor_errors as away_errors,
                g.game_duration,
                g.winning_team_id,
                g.losing_team_id
            FROM games g
            LEFT JOIN teams ht ON g.home_team_id = ht.team_id
            LEFT JOIN teams at ON g.away_team_id = at.team_id
            LEFT JOIN ballparks bp ON g.ballpark = bp.park_name
            WHERE 1=1
        """
        
        params = []
        
        # Add basic filters
        if start_date:
            query += " AND g.date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND g.date <= ?"
            params.append(end_date)
        
        if home_team:
            query += " AND g.home_team_id = ?"
            params.append(home_team)
        
        if away_team:
            query += " AND g.away_team_id = ?"
            params.append(away_team)
        
        if ballpark:
            query += " AND g.ballpark = ?"
            params.append(ballpark)
        
        if game_type:
            query += " AND g.gametype = ?"
            params.append(game_type)
        
        # Add complex filters
        for filter_obj in complex_filters:
            filter_type = filter_obj.get('type')
            operator = filter_obj.get('operator')
            value = filter_obj.get('value')
            team_id = filter_obj.get('team_id')
            inning = filter_obj.get('inning')
            role = filter_obj.get('role')
            
            if not filter_type or not operator:
                continue
            
            # Team filter - handle both specific teams and roles
            if filter_type == 'team':
                if team_id and value:
                    # Specific team with role
                    if value == 'winning' and operator == '=':
                        query += " AND g.winning_team_id = ?"
                        params.append(team_id)
                    elif value == 'losing' and operator == '=':
                        query += " AND g.losing_team_id = ?"
                        params.append(team_id)
                    elif value == 'home' and operator == '=':
                        query += " AND g.home_team_id = ?"
                        params.append(team_id)
                    elif value == 'away' and operator == '=':
                        query += " AND g.away_team_id = ?"
                        params.append(team_id)
                    elif value == 'any' and operator == '=':
                        query += " AND (g.home_team_id = ? OR g.away_team_id = ?)"
                        params.extend([team_id, team_id])
                elif team_id:
                    # Specific team without role (any role)
                    if operator == '=':
                        query += " AND (g.home_team_id = ? OR g.away_team_id = ?)"
                        params.extend([team_id, team_id])
                    elif operator == '!=':
                        query += " AND NOT (g.home_team_id = ? OR g.away_team_id = ?)"
                        params.extend([team_id, team_id])
            
            # Team stat filters - require value, team_id optional for "any team"
            elif filter_type in ['team_hits', 'team_runs', 'team_errors'] and value:
                stat_column = {
                    'team_hits': ('g.home_hits', 'g.visitor_hits'),
                    'team_runs': ('g.home_runs', 'g.visitor_runs'),
                    'team_errors': ('g.home_errors', 'g.visitor_errors')
                }[filter_type]
                
                home_col, away_col = stat_column
                
                if role == 'any' or not role:
                    if not team_id:  # "Any Team" selected - check if either team meets criteria
                        if operator == '>':
                            query += f" AND ({home_col} > ? OR {away_col} > ?)"
                            params.extend([int(value), int(value)])
                        elif operator == '<':
                            query += f" AND ({home_col} < ? OR {away_col} < ?)"
                            params.extend([int(value), int(value)])
                        elif operator == '=':
                            query += f" AND ({home_col} = ? OR {away_col} = ?)"
                            params.extend([int(value), int(value)])
                        elif operator == '>=':
                            query += f" AND ({home_col} >= ? OR {away_col} >= ?)"
                            params.extend([int(value), int(value)])
                        elif operator == '<=':
                            query += f" AND ({home_col} <= ? OR {away_col} <= ?)"
                            params.extend([int(value), int(value)])
                        elif operator == '!=':
                            query += f" AND NOT ({home_col} = ? AND {away_col} = ?)"
                            params.extend([int(value), int(value)])
                    else:  # Specific team selected with "any" role
                        if operator == '>':
                            query += f" AND ((g.home_team_id = ? AND {home_col} > ?) OR (g.away_team_id = ? AND {away_col} > ?))"
                            params.extend([team_id, int(value), team_id, int(value)])
                        elif operator == '<':
                            query += f" AND ((g.home_team_id = ? AND {home_col} < ?) OR (g.away_team_id = ? AND {away_col} < ?))"
                            params.extend([team_id, int(value), team_id, int(value)])
                        elif operator == '=':
                            query += f" AND ((g.home_team_id = ? AND {home_col} = ?) OR (g.away_team_id = ? AND {away_col} = ?))"
                            params.extend([team_id, int(value), team_id, int(value)])
                        elif operator == '>=':
                            query += f" AND ((g.home_team_id = ? AND {home_col} >= ?) OR (g.away_team_id = ? AND {away_col} >= ?))"
                            params.extend([team_id, int(value), team_id, int(value)])
                        elif operator == '<=':
                            query += f" AND ((g.home_team_id = ? AND {home_col} <= ?) OR (g.away_team_id = ? AND {away_col} <= ?))"
                            params.extend([team_id, int(value), team_id, int(value)])
                        elif operator == '!=':
                            query += f" AND NOT ((g.home_team_id = ? AND {home_col} = ?) OR (g.away_team_id = ? AND {away_col} = ?))"
                            params.extend([team_id, int(value), team_id, int(value)])
                elif role == 'winning':
                    if operator == '>':
                        query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} > ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {away_col} > ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '<':
                        query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} < ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {away_col} < ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '=':
                        query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {away_col} = ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '>=':
                        query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} >= ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {away_col} >= ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '<=':
                        query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} <= ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {away_col} <= ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '!=':
                        query += f" AND NOT ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {away_col} = ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                elif role == 'losing':
                    if operator == '>':
                        query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} > ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {away_col} > ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '<':
                        query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} < ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {away_col} < ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '=':
                        query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {away_col} = ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '>=':
                        query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} >= ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {away_col} >= ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '<=':
                        query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} <= ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {away_col} <= ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif operator == '!=':
                        query += f" AND NOT ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {away_col} = ?))"
                        params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                elif role == 'home':
                    if operator == '>':
                        query += f" AND g.home_team_id = ? AND {home_col} > ?"
                        params.extend([team_id, int(value)])
                    elif operator == '<':
                        query += f" AND g.home_team_id = ? AND {home_col} < ?"
                        params.extend([team_id, int(value)])
                    elif operator == '=':
                        query += f" AND g.home_team_id = ? AND {home_col} = ?"
                        params.extend([team_id, int(value)])
                    elif operator == '>=':
                        query += f" AND g.home_team_id = ? AND {home_col} >= ?"
                        params.extend([team_id, int(value)])
                    elif operator == '<=':
                        query += f" AND g.home_team_id = ? AND {home_col} <= ?"
                        params.extend([team_id, int(value)])
                    elif operator == '!=':
                        query += f" AND NOT (g.home_team_id = ? AND {home_col} = ?)"
                        params.extend([team_id, int(value)])
                elif role == 'away':
                    if operator == '>':
                        query += f" AND g.away_team_id = ? AND {away_col} > ?"
                        params.extend([team_id, int(value)])
                    elif operator == '<':
                        query += f" AND g.away_team_id = ? AND {away_col} < ?"
                        params.extend([team_id, int(value)])
                    elif operator == '=':
                        query += f" AND g.away_team_id = ? AND {away_col} = ?"
                        params.extend([team_id, int(value)])
                    elif operator == '>=':
                        query += f" AND g.away_team_id = ? AND {away_col} >= ?"
                        params.extend([team_id, int(value)])
                    elif operator == '<=':
                        query += f" AND g.away_team_id = ? AND {away_col} <= ?"
                        params.extend([team_id, int(value)])
                    elif operator == '!=':
                        query += f" AND NOT (g.away_team_id = ? AND {away_col} = ?)"
                        params.extend([team_id, int(value)])
                        # Inning score filters
            elif filter_type == 'inning_score' and inning and value:
                inning_num = int(inning)
                if 1 <= inning_num <= 12:
                    home_col = f"g.home_inn{inning_num}"
                    visitor_col = f"g.visitor_inn{inning_num}"
                    
                    if role == 'any' or not role:
                        if not team_id:  # "Any Team" selected - check if either team meets criteria
                            if operator == '>':
                                query += f" AND ({home_col} > ? OR {visitor_col} > ?)"
                                params.extend([int(value), int(value)])
                            elif operator == '<':
                                query += f" AND ({home_col} < ? OR {visitor_col} < ?)"
                                params.extend([int(value), int(value)])
                            elif operator == '=':
                                query += f" AND ({home_col} = ? OR {visitor_col} = ?)"
                                params.extend([int(value), int(value)])
                            elif operator == '>=':
                                query += f" AND ({home_col} >= ? OR {visitor_col} >= ?)"
                                params.extend([int(value), int(value)])
                            elif operator == '<=':
                                query += f" AND ({home_col} <= ? OR {visitor_col} <= ?)"
                                params.extend([int(value), int(value)])
                            elif operator == '!=':
                                query += f" AND NOT ({home_col} = ? AND {visitor_col} = ?)"
                                params.extend([int(value), int(value)])
                        else:  # Specific team selected with "any" role
                            if operator == '>':
                                query += f" AND ((g.home_team_id = ? AND {home_col} > ?) OR (g.away_team_id = ? AND {visitor_col} > ?))"
                                params.extend([team_id, int(value), team_id, int(value)])
                            elif operator == '<':
                                query += f" AND ((g.home_team_id = ? AND {home_col} < ?) OR (g.away_team_id = ? AND {visitor_col} < ?))"
                                params.extend([team_id, int(value), team_id, int(value)])
                            elif operator == '=':
                                query += f" AND ((g.home_team_id = ? AND {home_col} = ?) OR (g.away_team_id = ? AND {visitor_col} = ?))"
                                params.extend([team_id, int(value), team_id, int(value)])
                            elif operator == '>=':
                                query += f" AND ((g.home_team_id = ? AND {home_col} >= ?) OR (g.away_team_id = ? AND {visitor_col} >= ?))"
                                params.extend([team_id, int(value), team_id, int(value)])
                            elif operator == '<=':
                                query += f" AND ((g.home_team_id = ? AND {home_col} <= ?) OR (g.away_team_id = ? AND {visitor_col} <= ?))"
                                params.extend([team_id, int(value), team_id, int(value)])
                            elif operator == '!=':
                                query += f" AND NOT ((g.home_team_id = ? AND {home_col} = ?) OR (g.away_team_id = ? AND {visitor_col} = ?))"
                                params.extend([team_id, int(value), team_id, int(value)])
                    elif role == 'home':
                        if operator == '>':
                            query += f" AND g.home_team_id = ? AND {home_col} > ?"
                            params.extend([team_id, int(value)])
                        elif operator == '<':
                            query += f" AND g.home_team_id = ? AND {home_col} < ?"
                            params.extend([team_id, int(value)])
                        elif operator == '=':
                            query += f" AND g.home_team_id = ? AND {home_col} = ?"
                            params.extend([team_id, int(value)])
                        elif operator == '>=':
                            query += f" AND g.home_team_id = ? AND {home_col} >= ?"
                            params.extend([team_id, int(value)])
                        elif operator == '<=':
                            query += f" AND g.home_team_id = ? AND {home_col} <= ?"
                            params.extend([team_id, int(value)])
                        elif operator == '!=':
                            query += f" AND NOT (g.home_team_id = ? AND {home_col} = ?)"
                            params.extend([team_id, int(value)])
                    elif role == 'away':
                        if operator == '>':
                            query += f" AND g.away_team_id = ? AND {visitor_col} > ?"
                            params.extend([team_id, int(value)])
                        elif operator == '<':
                            query += f" AND g.away_team_id = ? AND {visitor_col} < ?"
                            params.extend([team_id, int(value)])
                        elif operator == '=':
                            query += f" AND g.away_team_id = ? AND {visitor_col} = ?"
                            params.extend([team_id, int(value)])
                        elif operator == '>=':
                            query += f" AND g.away_team_id = ? AND {visitor_col} >= ?"
                            params.extend([team_id, int(value)])
                        elif operator == '<=':
                            query += f" AND g.away_team_id = ? AND {visitor_col} <= ?"
                            params.extend([team_id, int(value)])
                        elif operator == '!=':
                            query += f" AND NOT (g.away_team_id = ? AND {visitor_col} = ?)"
                            params.extend([team_id, int(value)])
                    elif role == 'winning':
                        if operator == '>':
                            query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} > ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {visitor_col} > ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '<':
                            query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} < ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {visitor_col} < ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '=':
                            query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {visitor_col} = ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '>=':
                            query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} >= ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {visitor_col} >= ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '<=':
                            query += f" AND ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} <= ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {visitor_col} <= ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '!=':
                            query += f" AND NOT ((g.winning_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.winning_team_id = ? AND g.away_team_id = ? AND {visitor_col} = ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                    elif role == 'losing':
                        if operator == '>':
                            query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} > ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {visitor_col} > ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '<':
                            query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} < ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {visitor_col} < ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '=':
                            query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {visitor_col} = ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '>=':
                            query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} >= ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {visitor_col} >= ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '<=':
                            query += f" AND ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} <= ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {visitor_col} <= ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])
                        elif operator == '!=':
                            query += f" AND NOT ((g.losing_team_id = ? AND g.home_team_id = ? AND {home_col} = ?) OR (g.losing_team_id = ? AND g.away_team_id = ? AND {visitor_col} = ?))"
                            params.extend([team_id, team_id, int(value), team_id, team_id, int(value)])


            
            # Date filters
            elif filter_type == 'date' and value:
                if operator == '=':
                    query += " AND DATE(g.date) = ?"
                    params.append(value)
                elif operator == '>':
                    query += " AND DATE(g.date) > ?"
                    params.append(value)
                elif operator == '<':
                    query += " AND DATE(g.date) < ?"
                    params.append(value)
                elif operator == '>=':
                    query += " AND DATE(g.date) >= ?"
                    params.append(value)
                elif operator == '<=':
                    query += " AND DATE(g.date) <= ?"
                    params.append(value)
                elif operator == '!=':
                    query += " AND DATE(g.date) != ?"
                    params.append(value)

            # Attendance filters
            elif filter_type == 'attendance' and value:
                if operator == '>':
                    query += " AND g.attendance > ?"
                    params.append(int(value))
                elif operator == '<':
                    query += " AND g.attendance < ?"
                    params.append(int(value))
                elif operator == '=':
                    query += " AND g.attendance = ?"
                    params.append(int(value))
                elif operator == '>=':
                    query += " AND g.attendance >= ?"
                    params.append(int(value))
                elif operator == '<=':
                    query += " AND g.attendance <= ?"
                    params.append(int(value))
                elif operator == '!=':
                    query += " AND g.attendance != ?"
                    params.append(int(value))

            
            # Duration filters (convert H:MM to minutes in SQL for numeric comparisons)
            elif filter_type == 'duration' and value:
                duration_minutes_sql = "(CASE WHEN g.game_duration IS NULL OR g.game_duration = '' THEN NULL " \
                                       "WHEN instr(g.game_duration, ':') > 0 " \
                                       "THEN CAST(substr(g.game_duration,1,instr(g.game_duration,':')-1) AS INTEGER)*60 + " \
                                       "CAST(substr(g.game_duration, instr(g.game_duration,':')+1) AS INTEGER) " \
                                       "ELSE CAST(g.game_duration AS INTEGER) END)"
                if operator == '>':
                    query += f" AND {duration_minutes_sql} > ?"
                    params.append(int(value))
                elif operator == '<':
                    query += f" AND {duration_minutes_sql} < ?"
                    params.append(int(value))
                elif operator == '=':
                    query += f" AND {duration_minutes_sql} = ?"
                    params.append(int(value))
                elif operator == '>=':
                    query += f" AND {duration_minutes_sql} >= ?"
                    params.append(int(value))
                elif operator == '<=':
                    query += f" AND {duration_minutes_sql} <= ?"
                    params.append(int(value))
                elif operator == '!=':
                    query += f" AND {duration_minutes_sql} != ?"
                    params.append(int(value))


            
            # Score differential filters
            elif filter_type == 'score_differential' and value:
                if operator == '>':
                    query += " AND ABS(g.home_runs - g.visitor_runs) > ?"
                    params.append(int(value))
                elif operator == '<':
                    query += " AND ABS(g.home_runs - g.visitor_runs) < ?"
                    params.append(int(value))
                elif operator == '=':
                    query += " AND ABS(g.home_runs - g.visitor_runs) = ?"
                    params.append(int(value))
                elif operator == '>=':
                    query += " AND ABS(g.home_runs - g.visitor_runs) >= ?"
                    params.append(int(value))
                elif operator == '<=':
                    query += " AND ABS(g.home_runs - g.visitor_runs) <= ?"
                    params.append(int(value))
                elif operator == '!=':
                    query += " AND ABS(g.home_runs - g.visitor_runs) != ?"
                    params.append(int(value))
            
            # Ballpark filters
            elif filter_type == 'ballpark' and value:
                if operator == '=':
                    query += " AND g.ballpark = ?"
                    params.append(value)
                elif operator == '!=':
                    query += " AND g.ballpark != ?"
                    params.append(value)
            
            # Game type filters
            elif filter_type == 'gametype' and value:
                if operator == '=':
                    query += " AND g.gametype = ?"
                    params.append(value)
                elif operator == '!=':
                    query += " AND g.gametype != ?"
                    params.append(value)

        # Apply filters to query
        if query:
            full_query = query + " ORDER BY g.date DESC, g.game_id DESC"
        else:
            full_query = " ORDER BY g.date DESC, g.game_id DESC"
        
        # Add pagination
        if limit:
            full_query += f" LIMIT {limit}"
            if offset:
                full_query += f" OFFSET {offset}"
        
        cursor = conn.execute(full_query, params)
        games = cursor.fetchall()
        
                # Get total count for pagination
        # Build count query using the same base query and filters
        count_query = """
            SELECT COUNT(*) as total
            FROM games g
            LEFT JOIN teams ht ON g.home_team_id = ht.team_id
            LEFT JOIN teams at ON g.away_team_id = at.team_id
            LEFT JOIN ballparks bp ON g.ballpark = bp.park_name
            WHERE 1=1
        """
        
        # Build count parameters separately to match the count query structure
        count_params = []
        
        # Add the same basic filters that were applied to the main query
        if start_date:
            count_query += " AND g.date >= ?"
            count_params.append(start_date)
        if end_date:
            count_query += " AND g.date <= ?"
            count_params.append(end_date)
        if home_team:
            count_query += " AND g.home_team_id = ?"
            count_params.append(home_team)
        if away_team:
            count_query += " AND g.away_team_id = ?"
            count_params.append(away_team)
        if ballpark:
            count_query += " AND g.ballpark = ?"
            count_params.append(ballpark)
        if game_type:
            count_query += " AND g.gametype = ?"
            count_params.append(game_type)

        
        # Convert to list of dictionaries
        games_list = []
        for game in games:
            game_dict = dict(game)
            # Map API field names to frontend expected names
            game_dict['home_team'] = game_dict.get('home_team_name', game_dict.get('home_team_id'))
            game_dict['away_team'] = game_dict.get('away_team_name', game_dict.get('away_team_id'))
            game_dict['game_type'] = game_dict.get('gametype_en', game_dict.get('gametype'))
            
            # Convert duration from H:MM to total minutes for display
            if game_dict.get('game_duration'):
                game_dict['duration_minutes'] = convert_duration_to_minutes(game_dict['game_duration'])
            else:
                game_dict['duration_minutes'] = 0
                
            games_list.append(game_dict)


        # Add complex filters to count query (same logic as main query)
        for filter_obj in complex_filters:
            filter_type = filter_obj.get('type')
            operator = filter_obj.get('operator')
            value = filter_obj.get('value')
            
            if not filter_type or not operator:
                continue
            
            # Apply the same filter logic as in the main query
            if filter_type == 'attendance' and value:
                if operator == '>':
                    count_query += " AND g.attendance > ?"
                    count_params.append(int(value))
                elif operator == '<':
                    count_query += " AND g.attendance < ?"
                    count_params.append(int(value))
                elif operator == '=':
                    count_query += " AND g.attendance = ?"
                    count_params.append(int(value))
                elif operator == '>=':
                    count_query += " AND g.attendance >= ?"
                    count_params.append(int(value))
                elif operator == '<=':
                    count_query += " AND g.attendance <= ?"
                    count_params.append(int(value))
                elif operator == '!=':
                    count_query += " AND g.attendance != ?"
                    count_params.append(int(value))
            elif filter_type == 'duration' and value:
                if operator == '>':
                    count_query += " AND g.game_duration > ?"
                    count_params.append(int(value))
                elif operator == '<':
                    count_query += " AND g.game_duration < ?"
                    count_params.append(int(value))
                elif operator == '=':
                    count_query += " AND g.game_duration = ?"
                    count_params.append(int(value))
                elif operator == '>=':
                    count_query += " AND g.game_duration >= ?"
                    count_params.append(int(value))
                elif operator == '<=':
                    count_query += " AND g.game_duration <= ?"
                    count_params.append(int(value))
                elif operator == '!=':
                    count_query += " AND g.game_duration != ?"
                    count_params.append(int(value))
            elif filter_type == 'score_differential' and value:
                if operator == '>':
                    count_query += " AND ABS(g.home_runs - g.visitor_runs) > ?"
                    count_params.append(int(value))
                elif operator == '<':
                    count_query += " AND ABS(g.home_runs - g.visitor_runs) < ?"
                    count_params.append(int(value))
                elif operator == '=':
                    count_query += " AND ABS(g.home_runs - g.visitor_runs) = ?"
                    count_params.append(int(value))
                elif operator == '>=':
                    count_query += " AND ABS(g.home_runs - g.visitor_runs) >= ?"
                    count_params.append(int(value))
                elif operator == '<=':
                    count_query += " AND ABS(g.home_runs - g.visitor_runs) <= ?"
                    count_params.append(int(value))
                elif operator == '!=':
                    count_query += " AND ABS(g.home_runs - g.visitor_runs) != ?"
                    count_params.append(int(value))

        
        try:
            cursor = conn.execute(count_query, count_params)
            total_count = cursor.fetchone()['total']
        except Exception as count_error:
            print(f"Count query error: {count_error}")
            total_count = len(games_list)


        
        # Determine if there are more results
        has_more = len(games_list) == limit and (offset + limit) < total_count
        
        return jsonify({
            'games': games_list,
            'total': total_count,
            'has_more': has_more,
            'offset': offset,
            'limit': limit
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/filter-options')
def get_filter_options():
    """Get available filter options for dropdowns"""
    conn = get_db_connection()
    
    try:
        # Get teams - include English names in parentheses
        cursor = conn.execute("SELECT team_id, team_name, team_name_en FROM teams ORDER BY team_name")
        teams = []
        for row in cursor.fetchall():
            team_name = row['team_name']
            team_name_en = row['team_name_en']
            if team_name_en and team_name_en != team_name:
                display_name = f"{team_name} ({team_name_en})"
            else:
                display_name = team_name
            teams.append({'id': row['team_id'], 'name': display_name})
        
        # Get ballparks - include English names in parentheses
        cursor = conn.execute("""
            SELECT DISTINCT g.ballpark, bp.park_name_en 
            FROM games g 
            LEFT JOIN ballparks bp ON g.ballpark = bp.park_name 
            WHERE g.ballpark IS NOT NULL 
            ORDER BY g.ballpark
        """)
        ballparks = []
        for row in cursor.fetchall():
            ballpark_name = row['ballpark']
            ballpark_name_en = row['park_name_en']
            if ballpark_name_en and ballpark_name_en != ballpark_name:
                display_name = f"{ballpark_name} ({ballpark_name_en})"
            else:
                display_name = ballpark_name
            ballparks.append({'id': ballpark_name, 'name': display_name})

        
        # Get game types - format as objects with id and name, with English translations
        cursor = conn.execute("SELECT DISTINCT gametype FROM games WHERE gametype IS NOT NULL ORDER BY gametype")
        game_type_map = {
            '公式戦': 'Regular Season',
            'ファイナルステージ': 'Final Stage', 
            'ファーストステージ': 'First Stage',
            '日本シリーズ': 'Japan Series',
            'オールスターゲーム': 'All-Star Game',
            'R': 'Regular Season',
            'P': 'Playoffs',
            'C': 'Climax Series', 
            'J': 'Japan Series',
            'E': 'Exhibition',
            'A': 'All-Star'
        }
        game_types = []
        for row in cursor.fetchall():
            gametype = row['gametype']
            display_name = game_type_map.get(gametype, gametype)
            game_types.append({'id': gametype, 'name': f"{gametype} ({display_name})" if gametype != display_name else gametype})

        
        # Get date range
        cursor = conn.execute("SELECT MIN(date) as min_date, MAX(date) as max_date FROM games")
        date_range = cursor.fetchone()
        
        return jsonify({
            'teams': teams,
            'ballparks': ballparks,
            'game_types': game_types,  # Changed from 'gametypes' to 'game_types'
            'date_range': {
                'min_date': date_range['min_date'] if date_range else None,
                'max_date': date_range['max_date'] if date_range else None
            }
        })

        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

def format_innings_pitched(ip_decimal):
    """Convert decimal innings pitched to baseball standard format (e.g., 123.33 -> 123.1)"""
    if ip_decimal is None:
        return '0.0'
    
    # Convert to float if it's not already
    ip_float = float(ip_decimal)
    
    # Get whole innings
    whole_innings = int(ip_float)
    
    # Get fractional part and convert to outs
    fractional_part = ip_float - whole_innings
    outs = round(fractional_part * 3)
    
    # Handle case where rounding gives us 3 outs (should be next inning)
    if outs >= 3:
        whole_innings += 1
        outs = 0
    
    # Format as baseball standard
    if outs == 0:
        return f"{whole_innings}.0"
    else:
        return f"{whole_innings}.{outs}"

@app.route('/api/options/game-types')
def get_available_game_types():
    """Get all unique game types from the database"""
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT DISTINCT gametype FROM games WHERE gametype IS NOT NULL ORDER BY gametype")
        game_types = [row[0] for row in cur.fetchall()]
        return jsonify({'game_types': game_types})
    except Exception as e:
        print(f"Game types error: {e}")
        return jsonify({'error': 'Failed to get game types'}), 500
    finally:
        conn.close()

@app.route('/api/options/ballparks')
def get_available_ballparks():
    """Get all unique ballparks from the database"""
    conn = get_db_connection()
    try:
        cur = conn.execute("SELECT DISTINCT ballpark FROM games WHERE ballpark IS NOT NULL ORDER BY ballpark")
        ballparks = [row[0] for row in cur.fetchall()]
        return jsonify({'ballparks': ballparks})
    except Exception as e:
        print(f"Ballparks error: {e}")
        return jsonify({'error': 'Failed to get ballparks'}), 500
    finally:
        conn.close()

# Helper functions for dynamic advanced stats calculations

def get_league_wobas_and_scale():
    """Get league wOBA and wOBA scale by season for regular season games"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            g.season,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as league_woba
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        WHERE g.gametype = '公式戦'
        GROUP BY g.season
        ORDER BY g.season
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    
    # Convert to dictionary for easy lookup
    league_wobas = {}
    for season, league_woba in results:
        league_wobas[season] = league_woba
    
    # wOBA scale factor - calibrated for NPB
    # MLB uses 1.15, but NPB needs 0.16 due to using MLB wOBA weights in NPB context
    woba_scale = 0.16
    
    return league_wobas, woba_scale

def get_league_era_and_fip_constants():
    """Get league ERA and FIP constants by season for regular season games"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            g.season,
            ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as league_era,
            ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip
        FROM pitching p
        JOIN games g ON p.game_id = g.game_id
        WHERE g.gametype = '公式戦'
        GROUP BY g.season
        ORDER BY g.season
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    
    # Convert to dictionaries for easy lookup
    league_eras = {}
    fip_constants = {}
    
    for season, league_era, raw_fip in results:
        league_eras[season] = league_era
        # FIP constant = League ERA - Raw FIP (to make league FIP = league ERA)
        fip_constants[season] = round(league_era - raw_fip, 2)
    
    return league_eras, fip_constants

def get_park_factor(ballpark_name, stat_type='runs'):
    """Get park factor for a specific ballpark and stat type"""
    if not ballpark_name:
        return 1.0  # Neutral park factor if no ballpark specified
    
    conn = get_db_connection()
    
    # Map stat types to column names
    pf_columns = {
        'runs': 'pf_runs',
        'hr': 'pf_hr',
        'hits': 'pf_h',
        'bb': 'pf_bb'
    }
    
    column = pf_columns.get(stat_type, 'pf_runs')
    
    cursor = conn.execute(f'SELECT {column} FROM ballparks WHERE park_name = ?', (ballpark_name,))
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0] is not None:
        return result[0]
    else:
        return 1.0  # Neutral park factor if not found

def get_weighted_park_factors(player_id, seasons=None, stat_type='batting'):
    """Get weighted park factors for a player based on games played at each ballpark"""
    conn = get_db_connection()
    
    # Build query based on stat type
    if stat_type == 'batting':
        table = 'batting'
        weight_column = 'pa'  # Weight by plate appearances
    else:  # pitching
        table = 'pitching'
        weight_column = 'ip'  # Weight by innings pitched
    
    # Base query to get ballpark usage
    query = f"""
        SELECT 
            g.ballpark,
            SUM(s.{weight_column}) as weight,
            bp.pf_runs
        FROM {table} s
        JOIN games g ON s.game_id = g.game_id
        LEFT JOIN ballparks bp ON g.ballpark = bp.park_name
        WHERE s.player_id = ? AND g.gametype = '公式戦'
    """
    
    params = [player_id]
    
    # Add season filter if specified
    if seasons:
        season_placeholders = ','.join(['?' for _ in seasons])
        query += f" AND g.season IN ({season_placeholders})"
        params.extend(seasons)
    
    query += " GROUP BY g.ballpark, bp.pf_runs"
    
    cursor = conn.execute(query, params)
    results = cursor.fetchall()
    conn.close()
    
    if not results:
        return 1.0  # Neutral if no data
    
    # Calculate weighted average park factor
    total_weight = 0
    weighted_pf = 0
    
    for ballpark, weight, pf_runs in results:
        if weight and pf_runs is not None:
            total_weight += weight
            weighted_pf += pf_runs * weight
    
    if total_weight > 0:
        return weighted_pf / total_weight
    else:
        return 1.0  # Neutral if no valid data

def calculate_wrc_plus(player_stats, league_wobas, woba_scale):
    """Calculate wRC+ for a player's stats with park factors"""
    if not player_stats.get('woba') or not player_stats.get('season'):
        return 100
        
    season = player_stats['season'] 
    player_id = player_stats.get('player_id')
    player_woba = player_stats['woba']
    league_woba = league_wobas.get(season, 0.320)  # Default to 0.320 if not found
    
    # Get weighted park factors for this season/player
    if player_id and season != 'Career':
        park_factor_offense = get_weighted_park_factors(player_id, [season], 'batting')
    elif player_id and season == 'Career':
        # For career stats, get park factors across all seasons
        park_factor_offense = get_weighted_park_factors(player_id, None, 'batting')  
    else:
        park_factor_offense = 1.0  # Neutral if no player_id
    
    # Park-adjusted wRC+ formula: ((wOBA - league_wOBA) / wOBA_scale) * 100 + 100 / park_factor
    # Park factor > 1.0 = hitter-friendly = divide to reduce wRC+
    # Park factor < 1.0 = pitcher-friendly = divide to increase wRC+
    raw_wrc_plus = ((player_woba - league_woba) / woba_scale) * 100 + 100
    adjusted_wrc_plus = raw_wrc_plus / park_factor_offense
    return int(round(adjusted_wrc_plus))

def calculate_era_plus(player_stats, league_eras):
    """Calculate ERA+ for a player's stats with park factors"""
    if not player_stats.get('era') or player_stats['era'] == 0 or not player_stats.get('season'):
        return 100
        
    season = player_stats['season']
    player_id = player_stats.get('player_id')
    player_era = player_stats['era']
    league_era = league_eras.get(season, 4.00)  # Default to 4.00 if not found
    
    # Get weighted park factors for this season/player
    if player_id and season != 'Career':
        park_factor_pitching = get_weighted_park_factors(player_id, [season], 'pitching')
    elif player_id and season == 'Career':
        # For career stats, get park factors across all seasons
        park_factor_pitching = get_weighted_park_factors(player_id, None, 'pitching')
    else:
        park_factor_pitching = 1.0  # Neutral if no player_id
    
    # Park-adjusted ERA+ formula: 100 * (league_era / player_era) * park_factor
    # Higher park factor = more hitter-friendly = multiply to increase ERA+
    # Lower park factor = more pitcher-friendly = multiply to decrease ERA+
    raw_era_plus = 100 * (league_era / player_era)
    adjusted_era_plus = raw_era_plus * park_factor_pitching
    return int(round(adjusted_era_plus))

def enhance_batting_stats_with_advanced(results):
    """Add dynamically calculated advanced stats to batting results"""
    league_wobas, woba_scale = get_league_wobas_and_scale()
    
    for result in results:
        # Calculate wRC+ dynamically
        if result.get('season') != 'Career':
            result['wrc_plus'] = calculate_wrc_plus(result, league_wobas, woba_scale)
        else:
            # For career stats, use a weighted average approach
            # This is simplified - ideally would weight by PA across seasons
            result['wrc_plus'] = calculate_wrc_plus(result, league_wobas, woba_scale)
            
    return results

def enhance_pitching_stats_with_advanced(results):
    """Add dynamically calculated advanced stats to pitching results"""
    league_eras, fip_constants = get_league_era_and_fip_constants()
    
    for result in results:
        # Calculate ERA+ dynamically
        if result.get('season') != 'Career':
            result['era_plus'] = calculate_era_plus(result, league_eras)
        else:
            # For career stats, use a weighted average approach
            # This is simplified - ideally would weight by IP across seasons
            result['era_plus'] = calculate_era_plus(result, league_eras)
        
        # Calculate FIP with dynamic constant
        if result.get('raw_fip') is not None and result.get('season') != 'Career':
            fip_constant = fip_constants.get(result['season'], 3.10)  # Default to 3.10 if not found
            result['fip'] = round(result['raw_fip'] + fip_constant, 2)
        elif result.get('raw_fip') is not None and result.get('season') == 'Career':
            # For career, use simple default constant for now
            # This could be improved with weighted average
            result['fip'] = round(result['raw_fip'] + 3.10, 2)
            
    return results

# Advanced Statistics Endpoints

@app.route('/api/advanced-stats/standard', methods=['POST'])
def get_advanced_stats_standard():
    """Get standard advanced statistics for all players - routes to filtered endpoint with default qualifiers"""
    try:
        data = request.get_json() or {}
        stat_type = data.get('stat_type', 'batting').lower()
        
        # Set up default qualifiers for standard endpoint
        qualifiers = {}
        if stat_type == 'batting':
            qualifiers = {'ab': {'min': 100}}
        elif stat_type == 'pitching':
            qualifiers = {'ip': {'min': 10}}
        
        # Create filters for the filtered endpoint with empty filters but proper qualifiers
        filters = {
            'seasons': [],
            'teams': [],
            'positions': [],
            'min_pa': 0,
            'min_ip': 0,
            'aggregate_by_season': data.get('aggregate_by_season', False),
            'sort_by': data.get('sort_by', 'wrc_plus' if stat_type == 'batting' else 'era'),
            'sort_order': data.get('sort_order', 'desc'),
            'limit': data.get('limit', None),
            'game_filters': {},
            'situational_filters': {},
            'qualifiers': qualifiers
        }
        
        conn = get_db_connection()
        
        if stat_type == 'batting':
            return get_batting_stats_filtered(conn, filters)
        else:
            return get_pitching_stats_filtered(conn, filters)
            
    except Exception as e:
        print(f"Advanced stats error: {e}")
        return jsonify({'error': 'Failed to get advanced stats', 'details': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/api/advanced-stats/filtered', methods=['POST'])
def get_advanced_stats_filtered():
    """Get advanced statistics with comprehensive filtering"""
    try:
        data = request.get_json() or {}
        stat_type = data.get('stat_type', 'batting').lower()
        
        # Extract game filters from request
        game_filters = data.get('game_filters', {})
        situational_filters = data.get('situational_filters', {})
        qualifiers = data.get('qualifiers', {})
        
        # Set default qualifier of 100 AB for batting if no qualifiers specified
        if stat_type == 'batting' and not qualifiers:
            qualifiers = {'ab': {'min': 100}}
        
        # Filter parameters
        filters = {
            'seasons': data.get('seasons', []),  # List of seasons
            'teams': data.get('teams', []),      # List of team IDs
            'positions': data.get('positions', []),  # List of positions (batting only)
            'min_pa': data.get('min_pa', 0),     # Minimum plate appearances (batting)
            'min_ip': data.get('min_ip', 0),     # Minimum innings pitched (pitching)
            'aggregate_by_season': data.get('aggregate_by_season', False),  # Changed default to career totals
            'sort_by': data.get('sort_by', 'wrc_plus' if stat_type == 'batting' else 'era'),
            'sort_order': data.get('sort_order', 'desc'),
            'limit': data.get('limit', None),  # No limit by default
            'game_filters': game_filters,
            'situational_filters': situational_filters,
            'qualifiers': qualifiers
        }
        
        conn = get_db_connection()
        
        if stat_type == 'batting':
            return get_batting_stats_filtered(conn, filters)
        else:
            return get_pitching_stats_filtered(conn, filters)
            
    except Exception as e:
        print(f"Filtered advanced stats error: {e}")
        return jsonify({'error': 'Failed to get filtered advanced stats', 'details': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

def get_batting_stats(conn, aggregate_by_season=False, limit=None):
    """Get batting statistics"""
    
    if aggregate_by_season:
        # Season-by-season stats
        query = """
        SELECT 
            g.season,
            p.player_name as name,
            p.player_name_en as name_en,
            p.player_id,
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            SUM(b.b_h) as h,
            SUM(b.b_r) as r,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_k) as k,
            SUM(b.b_bb) as bb,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            
            -- Calculated stats
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr)) as tb,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            -- wRC+ calculated dynamically in Python
            100 as wrc_plus,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_2b) + SUM(b.b_3b) + SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.b_h), 0) * 100, 1) as xbh_pct
            
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        JOIN players p ON b.player_id = p.player_id
        GROUP BY g.season, p.player_id, p.player_name, p.player_name_en
        ORDER BY g.season DESC, wrc_plus DESC
        """
        
        if limit:
            query += " LIMIT ?"
            cursor = conn.execute(query, (limit,))
        else:
            cursor = conn.execute(query)
        
    else:
        # Career totals
        query = """
        SELECT 
            'Career' as season,
            p.player_name as name,
            p.player_name_en as name_en,
            p.player_id,
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            SUM(b.b_h) as h,
            SUM(b.b_r) as r,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_k) as k,
            SUM(b.b_bb) as bb,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            
            -- Calculated stats
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr)) as tb,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            -- wRC+ calculated dynamically in Python
            100 as wrc_plus,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_2b) + SUM(b.b_3b) + SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.b_h), 0) * 100, 1) as xbh_pct
            
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        JOIN players p ON b.player_id = p.player_id
        GROUP BY p.player_id, p.player_name, p.player_name_en
        ORDER BY wrc_plus DESC
        """
        
        if limit:
            query += " LIMIT ?"
            cursor = conn.execute(query, (limit,))
        else:
            cursor = conn.execute(query)
    
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 for display
    for result in results:
        numeric_fields = ['g', 'pa', 'ab', 'h', 'r', 'doubles', 'triples', 'hr', 'tb', 'rbi', 'k', 'bb', 'hbp', 'sac', 'gidp', 'roe']
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
    
    # Add dynamically calculated advanced stats
    results = enhance_batting_stats_with_advanced(results)
    
    return jsonify({'results': results, 'total': len(results)})

def get_pitching_stats(conn, aggregate_by_season=False, limit=None):
    """Get pitching statistics"""
    
    if aggregate_by_season:
        # Season-by-season stats
        query = """
        SELECT 
            g.season,
            p.player_name as name,
            p.player_name_en as name_en,
            p.player_id,
            COUNT(*) as app,
            SUM(pi.ip) as ip,
            SUM(pi.start) as gs,
            SUM(pi.finish) as gf,
            COUNT(CASE WHEN pi.finish = 1 AND pi.ip >= 9.0 THEN 1 END) as cg,
            COUNT(CASE WHEN pi.start = 1 AND pi.finish = 1 AND pi.r = 0 THEN 1 END) as sho,
            SUM(pi.win) as w,
            SUM(pi.loss) as l,
            SUM(pi.save) as sv,
            SUM(pi.hold) as hld,
            SUM(pi.p_k) as k,
            SUM(pi.p_bb) as bb,
            SUM(pi.balk) as bk,
            SUM(pi.p_hbp) as hbp,
            SUM(pi.r) as r,
            SUM(pi.er) as er,
            SUM(pi.p_h) as h,
            SUM(pi.p_2b) as doubles,
            SUM(pi.p_3b) as triples,
            SUM(pi.p_hr) as hr,
            
            -- Calculated stats
            ROUND(CAST(SUM(pi.win) AS FLOAT) / NULLIF(SUM(pi.win) + SUM(pi.loss), 0), 3) as w_pct,
            ROUND(CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as era,
            ROUND(((13*SUM(pi.p_hr) + 3*(SUM(pi.p_bb) + SUM(pi.p_hbp)) - 2*SUM(pi.p_k)) / NULLIF(SUM(pi.ip), 0)), 2) as raw_fip,
            -- ERA+ calculated dynamically in Python
            100 as era_plus,
            ROUND(CAST(SUM(pi.p_h) + SUM(pi.p_bb) AS FLOAT) / NULLIF(SUM(pi.ip), 0), 3) as whip,
            ROUND(CAST(SUM(pi.p_h) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_bb) - SUM(pi.p_hbp) - SUM(pi.p_sac), 0), 3) as baa,
            ROUND(CAST(SUM(pi.p_h) - SUM(pi.p_hr) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_k) - SUM(pi.p_hr) - SUM(pi.p_bb) - SUM(pi.p_hbp), 0), 3) as babip,
            ROUND(CAST(SUM(pi.p_k) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as k9,
            ROUND(CAST(SUM(pi.p_bb) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as bb9,

            ROUND(CAST(SUM(pi.p_gdp) AS FLOAT) / NULLIF(SUM(pi.p_gb), 0) * 100, 1) as gidp_pct
            
        FROM pitching pi
        JOIN games g ON pi.game_id = g.game_id
        JOIN players p ON pi.player_id = p.player_id
        GROUP BY g.season, p.player_id, p.player_name, p.player_name_en
        ORDER BY g.season DESC, era ASC
        """
        
        if limit:
            query += " LIMIT ?"
            cursor = conn.execute(query, (limit,))
        else:
            cursor = conn.execute(query)
        
    else:
        # Career totals
        query = """
        SELECT 
            'Career' as season,
            p.player_name as name,
            p.player_name_en as name_en,
            p.player_id,
            COUNT(*) as app,
            SUM(pi.ip) as ip,
            SUM(pi.start) as gs,
            SUM(pi.finish) as gf,
            COUNT(CASE WHEN pi.finish = 1 AND pi.ip >= 9.0 THEN 1 END) as cg,
            COUNT(CASE WHEN pi.start = 1 AND pi.finish = 1 AND pi.r = 0 THEN 1 END) as sho,
            SUM(pi.win) as w,
            SUM(pi.loss) as l,
            SUM(pi.save) as sv,
            SUM(pi.hold) as hld,
            SUM(pi.p_k) as k,
            SUM(pi.p_bb) as bb,
            SUM(pi.balk) as bk,
            SUM(pi.p_hbp) as hbp,
            SUM(pi.r) as r,
            SUM(pi.er) as er,
            SUM(pi.p_h) as h,
            SUM(pi.p_2b) as doubles,
            SUM(pi.p_3b) as triples,
            SUM(pi.p_hr) as hr,
            
            -- Calculated stats
            ROUND(CAST(SUM(pi.win) AS FLOAT) / NULLIF(SUM(pi.win) + SUM(pi.loss), 0), 3) as w_pct,
            ROUND(CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as era,
            ROUND(((13*SUM(pi.p_hr) + 3*(SUM(pi.p_bb) + SUM(pi.p_hbp)) - 2*SUM(pi.p_k)) / NULLIF(SUM(pi.ip), 0)), 2) as raw_fip,
            -- ERA+ calculated dynamically in Python
            100 as era_plus,
            ROUND(CAST(SUM(pi.p_h) + SUM(pi.p_bb) AS FLOAT) / NULLIF(SUM(pi.ip), 0), 3) as whip,
            ROUND(CAST(SUM(pi.p_h) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_bb) - SUM(pi.p_hbp) - SUM(pi.p_sac), 0), 3) as baa,
            ROUND(CAST(SUM(pi.p_h) - SUM(pi.p_hr) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_k) - SUM(pi.p_hr) - SUM(pi.p_bb) - SUM(pi.p_hbp), 0), 3) as babip,
            ROUND(CAST(SUM(pi.p_k) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as k9,
            ROUND(CAST(SUM(pi.p_bb) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as bb9,

            ROUND(CAST(SUM(pi.p_gdp) AS FLOAT) / NULLIF(SUM(pi.p_gb), 0) * 100, 1) as gidp_pct
            
        FROM pitching pi
        JOIN games g ON pi.game_id = g.game_id
        JOIN players p ON pi.player_id = p.player_id
        GROUP BY p.player_id, p.player_name, p.player_name_en
        ORDER BY era ASC
        """
        
        if limit:
            query += " LIMIT ?"
            cursor = conn.execute(query, (limit,))
        else:
            cursor = conn.execute(query)
    
    results = [dict(row) for row in cursor.fetchall()]
    
    # Post-process the results
    for result in results:
        # Format innings pitched
        if 'ip' in result and result['ip'] is not None:
            result['ip'] = format_innings_pitched(result['ip'])
        else:
            result['ip'] = '0.0'
            
        # Set null values to 0 for display
        numeric_fields = ['app', 'gs', 'gf', 'cg', 'sho', 'w', 'l', 'sv', 'hld', 'k', 'bb', 'bk', 'hbp', 'r', 'er', 'h', 'doubles', 'triples', 'hr']
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
    
    # Add dynamically calculated advanced stats
    results = enhance_pitching_stats_with_advanced(results)
    
    return jsonify({'results': results, 'total': len(results)})

def get_batting_stats_filtered(conn, filters):
    """Get batting statistics with comprehensive filtering"""
    situational_filters = filters.get('situational_filters', {})
    
    # Check if situational filters are present - if so, use event-level calculation
    if has_situational_filters(situational_filters):
        return get_batting_stats_from_events1(conn, filters)
    
    # Otherwise, use normal aggregated table approach
    game_filters = filters.get('game_filters', {})
    aggregate_by_season = filters.get('aggregate_by_season', False)  # Changed default to career totals
    limit = filters.get('limit', None)  # No limit by default
    min_pa = filters.get('min_pa', 0)  # Get minimum PA threshold
    qualifiers = filters.get('qualifiers', {})  # Get qualifiers
    
    # Build WHERE clause conditions for game filters
    where_conditions = []
    params = []
    
    # Game type filter
    if game_filters.get('game_types') and len(game_filters['game_types']) > 0:
        placeholders = ','.join(['?' for _ in game_filters['game_types']])
        where_conditions.append(f"g.gametype IN ({placeholders})")
        params.extend(game_filters['game_types'])
    
    # Win/Loss filter - handle arrays
    win_loss = game_filters.get('win_loss', [])
    if isinstance(win_loss, str):
        win_loss = [win_loss]
    if win_loss and 'all' not in win_loss and 'overall' not in win_loss:
        win_loss_conditions = []
        if 'wins' in win_loss:
            win_loss_conditions.append("b.team = g.winning_team_id")
        if 'losses' in win_loss:
            win_loss_conditions.append("b.team = g.losing_team_id")
        if win_loss_conditions:
            where_conditions.append(f"({' OR '.join(win_loss_conditions)})")
    
    # Home/Road filter - handle arrays
    home_road = game_filters.get('home_road', [])
    if isinstance(home_road, str):
        home_road = [home_road]
    if home_road and 'all' not in home_road and 'overall' not in home_road:
        home_road_conditions = []
        if 'home' in home_road:
            home_road_conditions.append("b.team = g.home_team_id")
        if 'road' in home_road:
            home_road_conditions.append("b.team = g.away_team_id")
        if home_road_conditions:
            where_conditions.append(f"({' OR '.join(home_road_conditions)})")
    
    # Month filter - handle arrays
    months = game_filters.get('month', [])
    if isinstance(months, str):
        months = [months]
    if months and 'all' not in months:
        # Filter out 'all' and convert valid months to integers
        valid_months = [month for month in months if month != 'all' and str(month).isdigit()]
        if valid_months:
            placeholders = ','.join(['?' for _ in valid_months])
            where_conditions.append(f"strftime('%m', g.date) IN ({placeholders})")
            params.extend([f"{int(month):02d}" for month in valid_months])
    
    # Ballpark filter - handle arrays
    ballparks = game_filters.get('ballpark', [])
    if isinstance(ballparks, str):
        ballparks = [ballparks]
    if ballparks and 'all' not in ballparks:
        # Filter out 'all' values
        valid_ballparks = [bp for bp in ballparks if bp != 'all']
        if valid_ballparks:
            placeholders = ','.join(['?' for _ in valid_ballparks])
            where_conditions.append(f"g.ballpark IN ({placeholders})")
            params.extend(valid_ballparks)
    
    # Date range filter
    date_range = game_filters.get('date_range', {})
    if date_range.get('start'):
        where_conditions.append("g.date >= ?")
        params.append(date_range['start'])
    if date_range.get('end'):
        where_conditions.append("g.date <= ?")
        params.append(date_range['end'])
    
    # Attendance filter
    attendance = game_filters.get('attendance', {})
    if attendance.get('min') and str(attendance['min']).strip():
        where_conditions.append("g.attendance >= ?")
        params.append(int(attendance['min']))
    if attendance.get('max') and str(attendance['max']).strip():
        where_conditions.append("g.attendance <= ?")
        params.append(int(attendance['max']))
    
    # Start time filter
    start_time = game_filters.get('start_time', {})
    if start_time.get('start') and str(start_time['start']).strip():
        where_conditions.append("g.start_time >= ?")
        params.append(start_time['start'])
    if start_time.get('end') and str(start_time['end']).strip():
        where_conditions.append("g.start_time <= ?")
        params.append(start_time['end'])
    
    # Build the WHERE clause
    where_clause = ""
    if where_conditions:
        where_clause = "AND " + " AND ".join(where_conditions)
    
    # Build HAVING clause for qualifiers
    having_conditions = []
    having_params = []
    
    # Process qualifiers - G, PA, AB, H, AVG
    for qualifier_name, qualifier_config in qualifiers.items():
        min_val = qualifier_config.get('min')
        max_val = qualifier_config.get('max')
        
        if qualifier_name == 'g':  # Games
            if min_val is not None:
                having_conditions.append("COUNT(DISTINCT b.game_id) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("COUNT(DISTINCT b.game_id) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'pa':  # Plate Appearances
            if min_val is not None:
                having_conditions.append("SUM(b.pa) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(b.pa) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'ab':  # At Bats
            if min_val is not None:
                having_conditions.append("SUM(b.ab) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(b.ab) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'h':  # Hits
            if min_val is not None:
                having_conditions.append("SUM(b.b_h) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(b.b_h) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'avg':  # Batting Average
            if min_val is not None:
                having_conditions.append("CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0) <= ?")
                having_params.append(max_val)
    
    # Build HAVING clause
    having_clause = ""
    if having_conditions:
        having_clause = "HAVING " + " AND ".join(having_conditions)
    elif min_pa > 0:  # Fallback to min_pa if no qualifiers
        having_clause = "HAVING SUM(b.pa) >= ?"
        having_params.append(min_pa)
    
    if aggregate_by_season:
        # Season-by-season stats with filtering
        base_where = "WHERE 1=1"
        if where_clause:
            base_where += f" {where_clause}"
            
        query = f"""
        SELECT 
            g.season,
            p.player_name as name,
            p.player_name_en as name_en,
            p.player_id,
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            SUM(b.b_h) as h,
            SUM(b.b_r) as r,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_k) as k,
            SUM(b.b_bb) as bb,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            
            -- Calculated stats
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr)) as tb,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            100 as wrc_plus,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_2b) + SUM(b.b_3b) + SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.b_h), 0) * 100, 1) as xbh_pct
            
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        JOIN players p ON b.player_id = p.player_id
        {base_where}
        GROUP BY g.season, p.player_id, p.player_name, p.player_name_en
        {having_clause}
        ORDER BY g.season DESC, wrc_plus DESC
        """
    else:
        # Career totals with filtering
        base_where = "WHERE 1=1"
        if where_clause:
            base_where += f" {where_clause}"
            
        query = f"""
        SELECT 
            'Career' as season,
            p.player_name as name,
            p.player_name_en as name_en,
            p.player_id,
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            SUM(b.b_h) as h,
            SUM(b.b_r) as r,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_k) as k,
            SUM(b.b_bb) as bb,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            
            -- Calculated stats
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr)) as tb,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0) - CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            100 as wrc_plus,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_2b) + SUM(b.b_3b) + SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.b_h), 0) * 100, 1) as xbh_pct
            
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        JOIN players p ON b.player_id = p.player_id
        {base_where}
        GROUP BY p.player_id, p.player_name, p.player_name_en
        {having_clause}
        ORDER BY wrc_plus DESC
        """
    
    # Add HAVING parameters and execute
    final_params = params + having_params
    if limit:
        query += " LIMIT ?"
        final_params.append(limit)
    
    cursor = conn.execute(query, final_params)
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 for display
    numeric_fields = ['g', 'pa', 'ab', 'h', 'r', 'doubles', 'triples', 'hr', 'tb', 'rbi', 'k', 'bb', 'hbp', 'sac', 'gidp', 'roe']
    for result in results:
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
    
    # Add dynamically calculated advanced stats
    results = enhance_batting_stats_with_advanced(results)
    
    return jsonify({'results': results, 'total': len(results)})

def get_pitching_stats_filtered(conn, filters):
    """Get pitching statistics with comprehensive filtering"""
    situational_filters = filters.get('situational_filters', {})
    
    # Check if situational filters are present - if so, use event-level calculation
    if has_situational_filters(situational_filters):
        return get_pitching_stats_from_events(conn, filters)
    
    # Otherwise, use normal aggregated table approach
    game_filters = filters.get('game_filters', {})
    aggregate_by_season = filters.get('aggregate_by_season', False)  # Changed default to career totals
    limit = filters.get('limit', None)  # No limit by default
    min_ip = filters.get('min_ip', 0)  # Get minimum IP threshold
    qualifiers = filters.get('qualifiers', {})  # Get qualifiers
    
    # Build WHERE clause conditions for game filters
    where_conditions = []
    params = []
    
    # Game type filter
    if game_filters.get('game_types') and len(game_filters['game_types']) > 0:
        placeholders = ','.join(['?' for _ in game_filters['game_types']])
        where_conditions.append(f"g.gametype IN ({placeholders})")
        params.extend(game_filters['game_types'])
    
    # Win/Loss filter - handle arrays
    win_loss = game_filters.get('win_loss', [])
    if isinstance(win_loss, str):
        win_loss = [win_loss]
    if win_loss and 'all' not in win_loss and 'overall' not in win_loss:
        win_loss_conditions = []
        if 'wins' in win_loss:
            win_loss_conditions.append("p.team = g.winning_team_id")
        if 'losses' in win_loss:
            win_loss_conditions.append("p.team = g.losing_team_id")
        if win_loss_conditions:
            where_conditions.append(f"({' OR '.join(win_loss_conditions)})")
    
    # Home/Road filter - handle arrays
    home_road = game_filters.get('home_road', [])
    if isinstance(home_road, str):
        home_road = [home_road]
    if home_road and 'all' not in home_road and 'overall' not in home_road:
        home_road_conditions = []
        if 'home' in home_road:
            home_road_conditions.append("p.team = g.home_team_id")
        if 'road' in home_road:
            home_road_conditions.append("p.team = g.away_team_id")
        if home_road_conditions:
            where_conditions.append(f"({' OR '.join(home_road_conditions)})")
    
    # Month filter - handle arrays
    months = game_filters.get('month', [])
    if isinstance(months, str):
        months = [months]
    if months and 'all' not in months:
        # Filter out 'all' and convert valid months to integers
        valid_months = [month for month in months if month != 'all' and str(month).isdigit()]
        if valid_months:
            placeholders = ','.join(['?' for _ in valid_months])
            where_conditions.append(f"strftime('%m', g.date) IN ({placeholders})")
            params.extend([f"{int(month):02d}" for month in valid_months])
    
    # Ballpark filter - handle arrays
    ballparks = game_filters.get('ballpark', [])
    if isinstance(ballparks, str):
        ballparks = [ballparks]
    if ballparks and 'all' not in ballparks:
        # Filter out 'all' values
        valid_ballparks = [bp for bp in ballparks if bp != 'all']
        if valid_ballparks:
            placeholders = ','.join(['?' for _ in valid_ballparks])
            where_conditions.append(f"g.ballpark IN ({placeholders})")
            params.extend(valid_ballparks)
    
    # Date range filter
    date_range = game_filters.get('date_range', {})
    if date_range.get('start'):
        where_conditions.append("g.date >= ?")
        params.append(date_range['start'])
    if date_range.get('end'):
        where_conditions.append("g.date <= ?")
        params.append(date_range['end'])
    
    # Attendance filter
    attendance = game_filters.get('attendance', {})
    if attendance.get('min') and str(attendance['min']).strip():
        where_conditions.append("g.attendance >= ?")
        params.append(int(attendance['min']))
    if attendance.get('max') and str(attendance['max']).strip():
        where_conditions.append("g.attendance <= ?")
        params.append(int(attendance['max']))
    
    # Start time filter
    start_time = game_filters.get('start_time', {})
    if start_time.get('start') and str(start_time['start']).strip():
        where_conditions.append("g.start_time >= ?")
        params.append(start_time['start'])
    if start_time.get('end') and str(start_time['end']).strip():
        where_conditions.append("g.start_time <= ?")
        params.append(start_time['end'])
    
    # Build the WHERE clause
    where_clause = ""
    if where_conditions:
        where_clause = "AND " + " AND ".join(where_conditions)
    
    # Build HAVING clause for qualifiers (pitching)
    having_conditions = []
    having_params = []
    
    # Process qualifiers - G, APP, IP, ERA
    for qualifier_name, qualifier_config in qualifiers.items():
        min_val = qualifier_config.get('min')
        max_val = qualifier_config.get('max')
        
        if qualifier_name == 'g':  # Games (pitching appearances)
            if min_val is not None:
                having_conditions.append("COUNT(*) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("COUNT(*) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'app':  # Appearances (same as G for pitching)
            if min_val is not None:
                having_conditions.append("COUNT(*) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("COUNT(*) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'ip':  # Innings Pitched
            if min_val is not None:
                having_conditions.append("SUM(pi.ip) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(pi.ip) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'era':  # ERA
            if min_val is not None:
                having_conditions.append("CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0) <= ?")
                having_params.append(max_val)
    
    # Build HAVING clause
    having_clause = ""
    if having_conditions:
        having_clause = "HAVING " + " AND ".join(having_conditions)
    elif min_ip > 0:  # Fallback to min_ip if no qualifiers
        having_clause = "HAVING SUM(pi.ip) >= ?"
        having_params.append(min_ip)
    
    if aggregate_by_season:
        # Season-by-season stats with filtering
        base_where = "WHERE 1=1"
        if where_clause:
            base_where += f" {where_clause}"
            
        query = f"""
        SELECT 
            g.season,
            pl.player_name as name,
            pl.player_name_en as name_en,
            pl.player_id,
            COUNT(DISTINCT p.game_id) as g,
            COUNT(*) as app,
            SUM(p.win) as w,
            SUM(p.loss) as l,
            SUM(p.save) as sv,
            SUM(p.hold) as hld,
            SUM(p.start) as gs,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 THEN 1 ELSE 0 END) as cg,
            SUM(p.ip) as ip,
            SUM(p.pitches_thrown) as pitches,
            SUM(p.batters_faced) as bf,
            SUM(p.r) as r,
            SUM(p.er) as er,
            SUM(p.p_h) as h,
            SUM(p.p_hr) as hr,
            SUM(p.p_k) as k,
            SUM(p.p_bb) as bb,
            SUM(p.p_hbp) as hbp,
            SUM(p.p_2b) as doubles,
            SUM(p.p_3b) as triples,
            SUM(p.p_gb) as gb,
            SUM(p.p_fb) as fb,
            SUM(p.wild_pitch) as wp,
            SUM(p.balk) as bk,
            SUM(p.p_roe) as roe,
            SUM(p.p_gdp) as gidp,
            ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
            ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,
            ROUND(CAST(SUM(p.win) AS FLOAT) / NULLIF(SUM(p.win) + SUM(p.loss), 0), 3) as w_pct,
            ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as k9,
            ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as bb9,
            ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as hr9,
            ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as fo_pct,
            ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as go_pct,
            ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gdp) + SUM(p.p_gb), 0) * 100, 1) as gidp_pct,
            ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
            ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
            0 as era_plus,
            ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_hr) - SUM(p.p_bb) - SUM(p.p_hbp), 0), 3) as babip
            
        FROM pitching p
        JOIN games g ON p.game_id = g.game_id
        JOIN players pl ON p.player_id = pl.player_id
        {base_where}
        GROUP BY g.season, pl.player_id, pl.player_name, pl.player_name_en
        {having_clause}
        ORDER BY g.season DESC, era ASC
        """
    else:
        # Career totals with filtering
        base_where = "WHERE 1=1"
        if where_clause:
            base_where += f" {where_clause}"
            
        query = f"""
        SELECT 
            'Career' as season,
            pl.player_name as name,
            pl.player_name_en as name_en,
            pl.player_id,
            COUNT(DISTINCT p.game_id) as g,
            COUNT(*) as app,
            SUM(p.win) as w,
            SUM(p.loss) as l,
            SUM(p.save) as sv,
            SUM(p.hold) as hld,
            SUM(p.start) as gs,
            SUM(CASE WHEN p.start = 1 AND p.finish = 1 THEN 1 ELSE 0 END) as cg,
            SUM(p.ip) as ip,
            SUM(p.pitches_thrown) as pitches,
            SUM(p.batters_faced) as bf,
            SUM(p.r) as r,
            SUM(p.er) as er,
            SUM(p.p_h) as h,
            SUM(p.p_hr) as hr,
            SUM(p.p_k) as k,
            SUM(p.p_bb) as bb,
            SUM(p.p_hbp) as hbp,
            SUM(p.p_2b) as doubles,
            SUM(p.p_3b) as triples,
            SUM(p.p_gb) as gb,
            SUM(p.p_fb) as fb,
            SUM(p.wild_pitch) as wp,
            SUM(p.balk) as bk,
            SUM(p.p_roe) as roe,
            SUM(p.p_gdp) as gidp,
            ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as era,
            ROUND(CAST(SUM(p.p_h) + SUM(p.p_bb) AS FLOAT) / NULLIF(SUM(p.ip), 0), 3) as whip,
            ROUND(CAST(SUM(p.win) AS FLOAT) / NULLIF(SUM(p.win) + SUM(p.loss), 0), 3) as w_pct,
            ROUND(CAST(SUM(p.p_k) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as k9,
            ROUND(CAST(SUM(p.p_bb) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as bb9,
            ROUND(CAST(SUM(p.p_hr) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as hr9,
            ROUND(CAST(SUM(p.p_fb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as fo_pct,
            ROUND(CAST(SUM(p.p_gb) AS FLOAT) / NULLIF(SUM(p.batters_faced), 0) * 100, 1) as go_pct,
            ROUND(CAST(SUM(p.p_gdp) AS FLOAT) / NULLIF(SUM(p.p_gdp) + SUM(p.p_gb), 0) * 100, 1) as gidp_pct,
            ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as raw_fip,
            ROUND(CAST(SUM(p.p_h) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_bb) - SUM(p.p_hbp) - SUM(p.p_sac), 0), 3) as baa,
            0 as era_plus,
            ROUND(CAST(SUM(p.p_h) - SUM(p.p_hr) AS FLOAT) / NULLIF(SUM(p.batters_faced) - SUM(p.p_k) - SUM(p.p_hr) - SUM(p.p_bb) - SUM(p.p_hbp), 0), 3) as babip
            
        FROM pitching p
        JOIN games g ON p.game_id = g.game_id
        JOIN players pl ON p.player_id = pl.player_id
        {base_where}
        GROUP BY pl.player_id, pl.player_name, pl.player_name_en
        {having_clause}
        ORDER BY era ASC
        """
    
    # Add HAVING parameters and execute
    final_params = params + having_params
    if limit:
        query += " LIMIT ?"
        final_params.append(limit)
    
    cursor = conn.execute(query, final_params)
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 for display
    numeric_fields = ['app', 'gs', 'cg', 'w', 'l', 'sv', 'hld', 'k', 'bb', 'bk', 'hbp', 'r', 'er', 'h', 'doubles', 'triples', 'hr']
    for result in results:
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
    
    # Add dynamically calculated advanced stats and format IP
    for result in results:
        result['ip'] = format_innings_pitched(result.get('ip'))
    
    results = enhance_pitching_stats_with_advanced(results)
    
    return jsonify({'results': results, 'total': len(results)})

# Team Statistics Endpoints

def get_team_batting_stats_filtered(conn, filters):
    """Get team batting statistics with comprehensive filtering"""
    situational_filters = filters.get('situational_filters', {})
    
    # Check if situational filters are present - if so, use event-level calculation
    if has_situational_filters(situational_filters):
        return get_team_batting_stats_from_events(conn, filters)
    
    # Otherwise, use normal aggregated table approach
    game_filters = filters.get('game_filters', {})
    aggregate_by_season = filters.get('aggregate_by_season', True)
    limit = filters.get('limit', 100)
    seasons = filters.get('seasons', [])
    qualifiers = filters.get('qualifiers', {})
    
    # Build WHERE clause conditions for game filters
    where_conditions = []
    params = []
    
    # Game type filter
    if game_filters.get('game_types') and len(game_filters['game_types']) > 0:
        placeholders = ','.join(['?' for _ in game_filters['game_types']])
        where_conditions.append(f"g.gametype IN ({placeholders})")
        params.extend(game_filters['game_types'])
    else:
        # Default to regular season games if no filter specified
        where_conditions.append("g.gametype = '公式戦'")
    
    # Win/Loss filter - handle arrays for team stats
    win_loss = game_filters.get('win_loss', [])
    if isinstance(win_loss, str):
        win_loss = [win_loss]
    if win_loss and 'all' not in win_loss and 'overall' not in win_loss:
        win_loss_conditions = []
        if 'wins' in win_loss:
            win_loss_conditions.append("b.team = g.winning_team_id")
        if 'losses' in win_loss:
            win_loss_conditions.append("b.team = g.losing_team_id")
        if win_loss_conditions:
            where_conditions.append(f"({' OR '.join(win_loss_conditions)})")
    
    # Home/Road filter - handle arrays for team stats
    home_road = game_filters.get('home_road', [])
    if isinstance(home_road, str):
        home_road = [home_road]
    if home_road and 'all' not in home_road and 'overall' not in home_road:
        home_road_conditions = []
        if 'home' in home_road:
            home_road_conditions.append("b.team = g.home_team_id")
        if 'road' in home_road:
            home_road_conditions.append("b.team = g.away_team_id")
        if home_road_conditions:
            where_conditions.append(f"({' OR '.join(home_road_conditions)})")
    
    # Month filter - handle arrays
    months = game_filters.get('month', [])
    if isinstance(months, str):
        months = [months]
    if months and 'all' not in months:
        # Filter out 'all' and convert valid months to integers
        valid_months = [month for month in months if month != 'all' and str(month).isdigit()]
        if valid_months:
            placeholders = ','.join(['?' for _ in valid_months])
            where_conditions.append(f"strftime('%m', g.date) IN ({placeholders})")
            params.extend([f"{int(month):02d}" for month in valid_months])
    
    # Ballpark filter - handle arrays
    ballparks = game_filters.get('ballpark', [])
    if isinstance(ballparks, str):
        ballparks = [ballparks]
    if ballparks and 'all' not in ballparks:
        # Filter out 'all' values
        valid_ballparks = [bp for bp in ballparks if bp != 'all']
        if valid_ballparks:
            placeholders = ','.join(['?' for _ in valid_ballparks])
            where_conditions.append(f"g.ballpark IN ({placeholders})")
            params.extend(valid_ballparks)
    
    # Date range filter
    date_range = game_filters.get('date_range', {})
    if date_range.get('start'):
        where_conditions.append("g.date >= ?")
        params.append(date_range['start'])
    if date_range.get('end'):
        where_conditions.append("g.date <= ?")
        params.append(date_range['end'])
    
    # Attendance filter
    attendance = game_filters.get('attendance', {})
    if attendance.get('min') and str(attendance['min']).strip():
        where_conditions.append("g.attendance >= ?")
        params.append(int(attendance['min']))
    if attendance.get('max') and str(attendance['max']).strip():
        where_conditions.append("g.attendance <= ?")
        params.append(int(attendance['max']))
    
    # Start time filter
    start_time = game_filters.get('start_time', {})
    if start_time.get('start') and str(start_time['start']).strip():
        where_conditions.append("g.start_time >= ?")
        params.append(start_time['start'])
    if start_time.get('end') and str(start_time['end']).strip():
        where_conditions.append("g.start_time <= ?")
        params.append(start_time['end'])
    
    # Seasons filter
    if seasons:
        season_placeholders = ','.join(['?' for _ in seasons])
        where_conditions.append(f"g.season IN ({season_placeholders})")
        params.extend(seasons)
    
    # Build the WHERE clause
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    else:
        where_clause = "WHERE 1=1"
    
    # Build HAVING clause for qualifiers
    having_conditions = []
    having_params = []
    
    # Process qualifiers for teams - G, PA, AB, H, AVG
    for qualifier_name, qualifier_config in qualifiers.items():
        min_val = qualifier_config.get('min')
        max_val = qualifier_config.get('max')
        
        if qualifier_name == 'g':  # Games
            if min_val is not None:
                having_conditions.append("COUNT(DISTINCT b.game_id) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("COUNT(DISTINCT b.game_id) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'pa':  # Plate Appearances
            if min_val is not None:
                having_conditions.append("SUM(b.pa) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(b.pa) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'ab':  # At Bats
            if min_val is not None:
                having_conditions.append("SUM(b.ab) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(b.ab) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'h':  # Hits
            if min_val is not None:
                having_conditions.append("SUM(b.b_h) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(b.b_h) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'avg':  # Batting Average
            if min_val is not None:
                having_conditions.append("CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0) <= ?")
                having_params.append(max_val)
    
    # Build HAVING clause
    having_clause = ""
    if having_conditions:
        having_clause = "HAVING " + " AND ".join(having_conditions)
    
    if aggregate_by_season:
        # Season-by-season team stats
        query = f"""
        SELECT 
            g.season,
            t.team_name as team_name,
            t.team_name_en as team_name_en,
            t.team_id as team_id,
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            SUM(b.b_h) as h,
            SUM(b.b_r) as r,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_k) as k,
            SUM(b.b_bb) as bb,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            
            -- Calculated stats
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr)) as tb,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_gdp) AS FLOAT) / NULLIF(SUM(b.b_h) - SUM(b.b_hr), 0) * 100, 1) as gidp_pct
            
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        JOIN teams t ON b.team = t.team_id
        {where_clause}
        GROUP BY g.season, t.team_id, t.team_name, t.team_name_en
        {having_clause}
        ORDER BY g.season DESC, avg DESC
        """
        
        if limit:
            query += " LIMIT ?"
            params.extend(having_params)
            params.append(limit)
        else:
            params.extend(having_params)
            
    else:
        # Career team totals
        query = f"""
        SELECT 
            'Career' as season,
            t.team_name as team_name,
            t.team_name_en as team_name_en,
            t.team_id as team_id,
            COUNT(DISTINCT b.game_id) as g,
            SUM(b.pa) as pa,
            SUM(b.ab) as ab,
            SUM(b.b_h) as h,
            SUM(b.b_r) as r,
            SUM(b.b_2b) as doubles,
            SUM(b.b_3b) as triples,
            SUM(b.b_hr) as hr,
            SUM(b.b_rbi) as rbi,
            SUM(b.b_k) as k,
            SUM(b.b_bb) as bb,
            SUM(b.b_hbp) as hbp,
            SUM(b.b_sac) as sac,
            SUM(b.b_gdp) as gidp,
            SUM(b.b_roe) as roe,
            
            -- Calculated stats
            (SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr)) as tb,
            ROUND(CAST(SUM(b.b_h) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as avg,
            ROUND(CAST((SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr)) + 2*SUM(b.b_2b) + 3*SUM(b.b_3b) + 4*SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as slg,
            ROUND(CAST(SUM(b.b_h) + SUM(b.b_bb) + SUM(b.b_hbp) AS FLOAT) / NULLIF(SUM(b.pa), 0), 3) as obp,
            ROUND(CAST(SUM(b.b_h) - SUM(b.b_2b) - SUM(b.b_3b) - SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab) - SUM(b.b_k) - SUM(b.b_hr), 0), 3) as babip,
            ROUND(CAST(SUM(b.b_hr) AS FLOAT) / NULLIF(SUM(b.ab), 0), 3) as iso,
            ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as woba,
            ROUND(CAST(SUM(b.b_k) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as k_pct,
            ROUND(CAST(SUM(b.b_bb) AS FLOAT) / NULLIF(SUM(b.pa), 0) * 100, 1) as bb_pct,
            ROUND(CAST(SUM(b.b_gdp) AS FLOAT) / NULLIF(SUM(b.b_h) - SUM(b.b_hr), 0) * 100, 1) as gidp_pct
            
        FROM batting b
        JOIN games g ON b.game_id = g.game_id
        JOIN teams t ON b.team = t.team_id
        {where_clause}
        GROUP BY t.team_id, t.team_name, t.team_name_en
        {having_clause}
        ORDER BY avg DESC
        """
        
        if limit:
            query += " LIMIT ?"
            params.extend(having_params)
            params.append(limit)
        else:
            params.extend(having_params)
    
    cursor = conn.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 for display
    numeric_fields = ['g', 'pa', 'ab', 'h', 'r', 'doubles', 'triples', 'hr', 'rbi', 'k', 'bb', 'hbp', 'sac', 'gdp', 'roe', 'tb']
    for result in results:
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
    
    # Add dynamically calculated advanced stats (wRC+ with team-level park factors)
    results = enhance_batting_stats_with_advanced(results)
    
    return jsonify({'results': results, 'total': len(results)})

@app.route('/api/advanced-stats/teams/batting', methods=['POST'])
def get_team_batting_stats1():
    """Get team batting statistics with advanced stats"""
    try:
        data = request.get_json() or {}
        
        conn = get_db_connection()
        return get_team_batting_stats_filtered(conn, data)
        
    except Exception as e:
        print(f"Team batting stats error: {e}")
        return jsonify({'error': 'Failed to get team batting stats', 'details': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

def get_team_batting_stats_from_events(conn, filters):
    """Calculate team batting stats from event table when situational filters are present"""
    game_filters = filters.get('game_filters', {})
    situational_filters = filters.get('situational_filters', {})
    aggregate_by_season = filters.get('aggregate_by_season', True)
    limit = filters.get('limit', 100)
    qualifiers = filters.get('qualifiers', {})
    
    # Build game-level WHERE conditions (reuse same logic as individual)
    game_conditions = []
    game_params = []
    
    # Game type filter
    if game_filters.get('game_types') and len(game_filters['game_types']) > 0:
        placeholders = ','.join(['?' for _ in game_filters['game_types']])
        game_conditions.append(f"g.gametype IN ({placeholders})")
        game_params.extend(game_filters['game_types'])
    else:
        # Default to regular season games if no filter specified
        game_conditions.append("g.gametype = '公式戦'")
    
    # Win/Loss filter - handle arrays for team stats
    win_loss = game_filters.get('win_loss', [])
    if isinstance(win_loss, str):
        win_loss = [win_loss]
    if win_loss and 'all' not in win_loss and 'overall' not in win_loss:
        win_loss_conditions = []
        if 'wins' in win_loss:
            win_loss_conditions.append("e.team = g.winning_team_id")
        if 'losses' in win_loss:
            win_loss_conditions.append("e.team = g.losing_team_id")
        if win_loss_conditions:
            game_conditions.append(f"({' OR '.join(win_loss_conditions)})")
    
    # Home/Road filter - handle arrays for team stats
    home_road = game_filters.get('home_road', [])
    if isinstance(home_road, str):
        home_road = [home_road]
    if home_road and 'all' not in home_road and 'overall' not in home_road:
        home_road_conditions = []
        if 'home' in home_road:
            home_road_conditions.append("e.team = g.home_team_id")
        if 'road' in home_road:
            home_road_conditions.append("e.team = g.away_team_id")
        if home_road_conditions:
            game_conditions.append(f"({' OR '.join(home_road_conditions)})")
    
    # Month filter - handle arrays
    months = game_filters.get('month', [])
    if isinstance(months, str):
        months = [months]
    if months and 'all' not in months:
        valid_months = [month for month in months if month != 'all' and str(month).isdigit()]
        if valid_months:
            placeholders = ','.join(['?' for _ in valid_months])
            game_conditions.append(f"strftime('%m', g.date) IN ({placeholders})")
            game_params.extend([f"{int(month):02d}" for month in valid_months])
    
    # Ballpark filter - handle arrays
    ballparks = game_filters.get('ballpark', [])
    if isinstance(ballparks, str):
        ballparks = [ballparks]
    if ballparks and 'all' not in ballparks:
        valid_ballparks = [bp for bp in ballparks if bp != 'all']
        if valid_ballparks:
            placeholders = ','.join(['?' for _ in valid_ballparks])
            game_conditions.append(f"g.ballpark IN ({placeholders})")
            game_params.extend(valid_ballparks)
    
    # Date range filter
    date_range = game_filters.get('date_range', {})
    if date_range.get('start'):
        game_conditions.append("g.date >= ?")
        game_params.append(date_range['start'])
    if date_range.get('end'):
        game_conditions.append("g.date <= ?")
        game_params.append(date_range['end'])
    
    # Attendance filter
    attendance = game_filters.get('attendance', {})
    if attendance.get('min') and str(attendance['min']).strip():
        game_conditions.append("g.attendance >= ?")
        game_params.append(int(attendance['min']))
    if attendance.get('max') and str(attendance['max']).strip():
        game_conditions.append("g.attendance <= ?")
        game_params.append(int(attendance['max']))
    
    # Start time filter
    start_time = game_filters.get('start_time', {})
    if start_time.get('start') and str(start_time['start']).strip():
        game_conditions.append("g.start_time >= ?")
        game_params.append(start_time['start'])
    if start_time.get('end') and str(start_time['end']).strip():
        game_conditions.append("g.start_time <= ?")
        game_params.append(start_time['end'])
    
    # Seasons filter
    seasons = filters.get('seasons', [])
    if seasons:
        placeholders = ','.join(['?' for _ in seasons])
        game_conditions.append(f"g.season IN ({placeholders})")
        game_params.extend(seasons)
    
    # Build situational WHERE conditions
    situational_conditions, situational_params = build_situational_where_clause(situational_filters)
    
    # Combine all conditions
    all_conditions = game_conditions + situational_conditions
    all_params = game_params + situational_params
    
    # Build WHERE clause
    where_clause = "WHERE 1=1"
    if all_conditions:
        where_clause += " AND " + " AND ".join(all_conditions)
    
    # Build GROUP BY clause (group by team instead of player)
    if aggregate_by_season:
        group_by = "GROUP BY g.season, e.team, t.team_name, t.team_name_en"
        select_season = "g.season"
        order_by = "ORDER BY g.season DESC, obp DESC"
    else:
        group_by = "GROUP BY e.team, t.team_name, t.team_name_en"  
        select_season = "'Career' as season"
        order_by = "ORDER BY obp DESC"
    
    # Event-based team batting query with manual stat calculation
    query = f"""
    SELECT 
        {select_season},
        t.team_name as team_name,
        t.team_name_en as team_name_en,
        e.team as team_id,
        COUNT(*) as pa,
        SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END) as ab,
        SUM(e.h) as h,
        SUM(e.rbi) as rbi,
        SUM(e."2b") as doubles,
        SUM(e."3b") as triples, 
        SUM(e.hr) as hr,
        SUM(e.k) as k,
        SUM(e.bb) as bb,
        SUM(e.hbp) as hbp,
        SUM(e.sac) as sac,
        SUM(e.gdp) as gidp,
        
        -- Calculate derived stats
        (SUM(e.h) + SUM(e."2b") + 2*SUM(e."3b") + 3*SUM(e.hr)) as tb,
         
        ROUND(CAST(SUM(e.h) AS FLOAT) / 
              NULLIF(SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END), 0), 3) as avg,
              
        ROUND(CAST((SUM(e.h) + SUM(e."2b") + 2*SUM(e."3b") + 3*SUM(e.hr)) AS FLOAT) / 
              NULLIF(SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END), 0), 3) as slg,
              
        ROUND(CAST((SUM(e.h) + SUM(e.bb) + SUM(e.hbp)) AS FLOAT) / 
              NULLIF(COUNT(*), 0), 3) as obp,
              
        ROUND((0.69*SUM(e.bb) + 0.72*SUM(e.hbp) + 0.89*(SUM(e.h)-SUM(e."2b")-SUM(e."3b")-SUM(e.hr)) + 1.27*SUM(e."2b") + 1.62*SUM(e."3b") + 2.10*SUM(e.hr)) / NULLIF(COUNT(*), 0), 3) as woba,
              
        ROUND(CAST((SUM(e.h) - SUM(e.hr)) AS FLOAT) / 
              NULLIF(SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END) - SUM(e.k) - SUM(e.hr), 0), 3) as babip,
              
        -- Percentage stats
        ROUND(CAST(SUM(e.k) AS FLOAT) / NULLIF(COUNT(*), 0) * 100, 1) as k_pct,
        ROUND(CAST(SUM(e.bb) AS FLOAT) / NULLIF(COUNT(*), 0) * 100, 1) as bb_pct,
        ROUND(CAST(SUM(e."2b") + SUM(e."3b") + SUM(e.hr) AS FLOAT) / NULLIF(SUM(e.h), 0) * 100, 1) as xbh_pct
        
    FROM event e
    JOIN games g ON e.game_id = g.game_id
    JOIN teams t ON e.team = t.team_id
    LEFT JOIN players pb ON e.batter_player_id = pb.player_id
    LEFT JOIN players pp ON e.pitcher_player_id = pp.player_id
    {where_clause}
    {group_by}
    """
    
    # Add qualifiers as HAVING clause
    having_conditions = []
    having_params = []
    
    for qualifier_name, qualifier_config in qualifiers.items():
        min_val = qualifier_config.get('min')
        max_val = qualifier_config.get('max')
        
        if qualifier_name == 'pa':
            if min_val is not None:
                having_conditions.append("COUNT(*) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("COUNT(*) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'ab':
            if min_val is not None:
                having_conditions.append("SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END) >= ?")
                having_params.append(min_val)
    
    if having_conditions:
        query += " HAVING " + " AND ".join(having_conditions)
    
    query += f" {order_by}"
    
    if limit:
        query += " LIMIT ?"
        having_params.append(limit)
    
    # Execute query
    final_params = all_params + having_params
    cursor = conn.execute(query, final_params)
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 
    numeric_fields = ['pa', 'ab', 'h', 'doubles', 'triples', 'hr', 'tb', 'rbi', 'k', 'bb', 'hbp', 'sac', 'gidp']
    for result in results:
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
        
        # Calculate ISO
        if result.get('slg') is not None and result.get('avg') is not None:
            result['iso'] = round(result['slg'] - result['avg'], 3)
        else:
            result['iso'] = 0.000
    
    # Add dynamically calculated advanced stats (wRC+ with team-level park factors)
    results = enhance_batting_stats_with_advanced(results)
    
    # Re-sort by wrc_plus after enhancement (since SQL couldn't sort by it)
    if aggregate_by_season:
        # Sort by season desc, then wrc_plus desc
        results = sorted(results, key=lambda x: (-x.get('season', 0), -x.get('wrc_plus', 0)))
    else:
        # Sort by wrc_plus desc
        results = sorted(results, key=lambda x: -x.get('wrc_plus', 0))
    
    return jsonify({'results': results, 'total': len(results)})

def get_team_pitching_stats_filtered(conn, filters):
    """Get team pitching statistics with comprehensive filtering"""
    game_filters = filters.get('game_filters', {})
    aggregate_by_season = filters.get('aggregate_by_season', True)
    limit = filters.get('limit', 100)
    seasons = filters.get('seasons', [])
    qualifiers = filters.get('qualifiers', {})
    
    # Build WHERE clause conditions for game filters
    where_conditions = []
    params = []
    
    # Game type filter
    if game_filters.get('game_types') and len(game_filters['game_types']) > 0:
        placeholders = ','.join(['?' for _ in game_filters['game_types']])
        where_conditions.append(f"g.gametype IN ({placeholders})")
        params.extend(game_filters['game_types'])
    else:
        # Default to regular season games if no filter specified
        where_conditions.append("g.gametype = '公式戦'")
    
    # Win/Loss filter - handle arrays for team stats
    win_loss = game_filters.get('win_loss', [])
    if isinstance(win_loss, str):
        win_loss = [win_loss]
    if win_loss and 'all' not in win_loss and 'overall' not in win_loss:
        win_loss_conditions = []
        if 'wins' in win_loss:
            win_loss_conditions.append("pi.team = g.winning_team_id")
        if 'losses' in win_loss:
            win_loss_conditions.append("pi.team = g.losing_team_id")
        if win_loss_conditions:
            where_conditions.append(f"({' OR '.join(win_loss_conditions)})")
    
    # Home/Road filter - handle arrays for team stats
    home_road = game_filters.get('home_road', [])
    if isinstance(home_road, str):
        home_road = [home_road]
    if home_road and 'all' not in home_road and 'overall' not in home_road:
        home_road_conditions = []
        if 'home' in home_road:
            home_road_conditions.append("pi.team = g.home_team_id")
        if 'road' in home_road:
            home_road_conditions.append("pi.team = g.away_team_id")
        if home_road_conditions:
            where_conditions.append(f"({' OR '.join(home_road_conditions)})")
    
    # Month filter - handle arrays
    months = game_filters.get('month', [])
    if isinstance(months, str):
        months = [months]
    if months and 'all' not in months:
        # Filter out 'all' and convert valid months to integers
        valid_months = [month for month in months if month != 'all' and str(month).isdigit()]
        if valid_months:
            placeholders = ','.join(['?' for _ in valid_months])
            where_conditions.append(f"strftime('%m', g.date) IN ({placeholders})")
            params.extend([f"{int(month):02d}" for month in valid_months])
    
    # Ballpark filter - handle arrays
    ballparks = game_filters.get('ballpark', [])
    if isinstance(ballparks, str):
        ballparks = [ballparks]
    if ballparks and 'all' not in ballparks:
        # Filter out 'all' values
        valid_ballparks = [bp for bp in ballparks if bp != 'all']
        if valid_ballparks:
            placeholders = ','.join(['?' for _ in valid_ballparks])
            where_conditions.append(f"g.ballpark IN ({placeholders})")
            params.extend(valid_ballparks)
    
    # Date range filter
    date_range = game_filters.get('date_range', {})
    if date_range.get('start'):
        where_conditions.append("g.date >= ?")
        params.append(date_range['start'])
    if date_range.get('end'):
        where_conditions.append("g.date <= ?")
        params.append(date_range['end'])
    
    # Attendance filter
    attendance = game_filters.get('attendance', {})
    if attendance.get('min') and str(attendance['min']).strip():
        where_conditions.append("g.attendance >= ?")
        params.append(int(attendance['min']))
    if attendance.get('max') and str(attendance['max']).strip():
        where_conditions.append("g.attendance <= ?")
        params.append(int(attendance['max']))
    
    # Start time filter
    start_time = game_filters.get('start_time', {})
    if start_time.get('start') and str(start_time['start']).strip():
        where_conditions.append("g.start_time >= ?")
        params.append(start_time['start'])
    if start_time.get('end') and str(start_time['end']).strip():
        where_conditions.append("g.start_time <= ?")
        params.append(start_time['end'])
    
    # Seasons filter
    if seasons:
        season_placeholders = ','.join(['?' for _ in seasons])
        where_conditions.append(f"g.season IN ({season_placeholders})")
        params.extend(seasons)
    
    # Build the WHERE clause
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    else:
        where_clause = "WHERE 1=1"
    
    # Build HAVING clause for qualifiers
    having_conditions = []
    having_params = []
    
    # Process qualifiers for teams - G, APP, IP, ERA
    for qualifier_name, qualifier_config in qualifiers.items():
        min_val = qualifier_config.get('min')
        max_val = qualifier_config.get('max')
        
        if qualifier_name == 'g' or qualifier_name == 'app':  # Games/Appearances
            if min_val is not None:
                having_conditions.append("COUNT(*) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("COUNT(*) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'ip':  # Innings Pitched
            if min_val is not None:
                having_conditions.append("SUM(pi.ip) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("SUM(pi.ip) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'era':  # ERA
            if min_val is not None:
                having_conditions.append("CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0) <= ?")
                having_params.append(max_val)
    
    # Build HAVING clause
    having_clause = ""
    if having_conditions:
        having_clause = "HAVING " + " AND ".join(having_conditions)
    
    if aggregate_by_season:
        # Season-by-season team stats
        query = f"""
        SELECT 
            g.season,
            t.team_name as team_name,
            t.team_name_en as team_name_en,
            t.team_id as team_id,
            COUNT(*) as app,
            SUM(pi.ip) as ip,
            SUM(pi.start) as gs,
            SUM(pi.finish) as gf,
            COUNT(CASE WHEN pi.finish = 1 AND pi.ip >= 9.0 THEN 1 END) as cg,
            COUNT(CASE WHEN pi.start = 1 AND pi.finish = 1 AND pi.r = 0 THEN 1 END) as sho,
            SUM(pi.win) as w,
            SUM(pi.loss) as l,
            SUM(pi.save) as sv,
            SUM(pi.hold) as hld,
            SUM(pi.p_k) as k,
            SUM(pi.p_bb) as bb,
            SUM(pi.balk) as bk,
            SUM(pi.p_hbp) as hbp,
            SUM(pi.r) as r,
            SUM(pi.er) as er,
            SUM(pi.p_h) as h,
            SUM(pi.p_2b) as doubles,
            SUM(pi.p_3b) as triples,
            SUM(pi.p_hr) as hr,
            
            -- Calculated stats
            ROUND(CAST(SUM(pi.win) AS FLOAT) / NULLIF(SUM(pi.win) + SUM(pi.loss), 0), 3) as w_pct,
            ROUND(CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as era,
            ROUND(((13*SUM(pi.p_hr) + 3*(SUM(pi.p_bb) + SUM(pi.p_hbp)) - 2*SUM(pi.p_k)) / NULLIF(SUM(pi.ip), 0)), 2) as raw_fip,
            -- ERA+ calculated dynamically in Python
            100 as era_plus,
            ROUND(CAST(SUM(pi.p_h) + SUM(pi.p_bb) AS FLOAT) / NULLIF(SUM(pi.ip), 0), 3) as whip,
            ROUND(CAST(SUM(pi.p_h) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_bb) - SUM(pi.p_hbp) - SUM(pi.p_sac), 0), 3) as baa,
            ROUND(CAST(SUM(pi.p_h) - SUM(pi.p_hr) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_k) - SUM(pi.p_hr) - SUM(pi.p_bb) - SUM(pi.p_hbp), 0), 3) as babip,
            ROUND(CAST(SUM(pi.p_k) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as k9,
            ROUND(CAST(SUM(pi.p_bb) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as bb9,

            ROUND(CAST(SUM(pi.p_gdp) AS FLOAT) / NULLIF(SUM(pi.p_gb), 0) * 100, 1) as gidp_pct
            
        FROM pitching pi
        JOIN games g ON pi.game_id = g.game_id
        JOIN teams t ON pi.team = t.team_id
        {where_clause}
        GROUP BY g.season, t.team_id, t.team_name
        {having_clause}
        ORDER BY g.season DESC, era ASC
        """
        
        if limit:
            query += " LIMIT ?"
            params.extend(having_params)
            params.append(limit)
        else:
            params.extend(having_params)
            
    else:
        # Career team totals
        query = f"""
        SELECT 
            'Career' as season,
            t.team_name as team_name,
            t.team_name_en as team_name_en,
            t.team_id as team_id,
            COUNT(*) as app,
            SUM(pi.ip) as ip,
            SUM(pi.start) as gs,
            SUM(pi.finish) as gf,
            COUNT(CASE WHEN pi.finish = 1 AND pi.ip >= 9.0 THEN 1 END) as cg,
            COUNT(CASE WHEN pi.start = 1 AND pi.finish = 1 AND pi.r = 0 THEN 1 END) as sho,
            SUM(pi.win) as w,
            SUM(pi.loss) as l,
            SUM(pi.save) as sv,
            SUM(pi.hold) as hld,
            SUM(pi.p_k) as k,
            SUM(pi.p_bb) as bb,
            SUM(pi.balk) as bk,
            SUM(pi.p_hbp) as hbp,
            SUM(pi.r) as r,
            SUM(pi.er) as er,
            SUM(pi.p_h) as h,
            SUM(pi.p_2b) as doubles,
            SUM(pi.p_3b) as triples,
            SUM(pi.p_hr) as hr,
            
            -- Calculated stats
            ROUND(CAST(SUM(pi.win) AS FLOAT) / NULLIF(SUM(pi.win) + SUM(pi.loss), 0), 3) as w_pct,
            ROUND(CAST(SUM(pi.er) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as era,
            ROUND(((13*SUM(pi.p_hr) + 3*(SUM(pi.p_bb) + SUM(pi.p_hbp)) - 2*SUM(pi.p_k)) / NULLIF(SUM(pi.ip), 0)), 2) as raw_fip,
            -- ERA+ calculated dynamically in Python
            100 as era_plus,
            ROUND(CAST(SUM(pi.p_h) + SUM(pi.p_bb) AS FLOAT) / NULLIF(SUM(pi.ip), 0), 3) as whip,
            ROUND(CAST(SUM(pi.p_h) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_bb) - SUM(pi.p_hbp) - SUM(pi.p_sac), 0), 3) as baa,
            ROUND(CAST(SUM(pi.p_h) - SUM(pi.p_hr) AS FLOAT) / NULLIF(SUM(pi.batters_faced) - SUM(pi.p_k) - SUM(pi.p_hr) - SUM(pi.p_bb) - SUM(pi.p_hbp), 0), 3) as babip,
            ROUND(CAST(SUM(pi.p_k) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as k9,
            ROUND(CAST(SUM(pi.p_bb) AS FLOAT) * 9 / NULLIF(SUM(pi.ip), 0), 2) as bb9,

            ROUND(CAST(SUM(pi.p_gdp) AS FLOAT) / NULLIF(SUM(pi.p_gb), 0) * 100, 1) as gidp_pct
            
        FROM pitching pi
        JOIN games g ON pi.game_id = g.game_id
        JOIN teams t ON pi.team = t.team_id
        {where_clause}
        GROUP BY t.team_id, t.team_name
        {having_clause}
        ORDER BY era ASC
        """
        
        if limit:
            query += " LIMIT ?"
            params.extend(having_params)
            params.append(limit)
        else:
            params.extend(having_params)
    
    cursor = conn.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 for display
    numeric_fields = ['app', 'gs', 'cg', 'w', 'l', 'sv', 'hld', 'k', 'bb', 'bk', 'hbp', 'r', 'er', 'h', 'doubles', 'triples', 'hr']
    for result in results:
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
    
    # Add dynamically calculated advanced stats and format IP
    for result in results:
        result['ip'] = format_innings_pitched(result.get('ip'))
    
    results = enhance_pitching_stats_with_advanced(results)
    
    return jsonify({'results': results, 'total': len(results)})

@app.route('/api/advanced-stats/teams/pitching', methods=['POST'])
def get_team_pitching_stats1():
    """Get team pitching statistics with advanced stats"""
    try:
        data = request.get_json() or {}
        
        conn = get_db_connection()
        return get_team_pitching_stats_filtered(conn, data)
        
    except Exception as e:
        print(f"Team pitching stats error: {e}")
        return jsonify({'error': 'Failed to get team pitching stats', 'details': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

# Player Search Endpoints for Situational Filters

@app.route('/api/players/search/batters', methods=['GET'])
def search_batters():
    """Search for batters for the situational filter modal"""
    try:
        query = request.args.get('q', '').strip()
        limit = int(request.args.get('limit', 20))
        
        if len(query) < 2:
            return jsonify({'players': []})
        
        conn = get_db_connection()
        
        # Search in both Japanese and English names
        search_query = """
        SELECT DISTINCT 
            p.player_id,
            p.player_name,
            p.player_name_en,
            p.bat
        FROM players p
        WHERE (p.player_name LIKE ? OR p.player_name_en LIKE ?)
        AND EXISTS (
            SELECT 1 FROM batting b 
            WHERE b.player_id = p.player_id
        )
        ORDER BY p.player_name
        LIMIT ?
        """
        
        search_term = f"%{query}%"
        cursor = conn.execute(search_query, (search_term, search_term, limit))
        results = cursor.fetchall()
        
        players = []
        for row in results:
            players.append({
                'player_id': row[0],
                'player_name': row[1],
                'player_name_en': row[2],
                'handedness': row[3] if row[3] else 'Unknown'
            })
        
        return jsonify({'players': players})
        
    except Exception as e:
        print(f"Batter search error: {e}")
        return jsonify({'error': 'Failed to search batters', 'details': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/api/players/search/pitchers', methods=['GET'])
def search_pitchers():
    """Search for pitchers for the situational filter modal"""
    try:
        query = request.args.get('q', '').strip()
        limit = int(request.args.get('limit', 20))
        
        if len(query) < 2:
            return jsonify({'players': []})
        
        conn = get_db_connection()
        
        # Search in both Japanese and English names
        search_query = """
        SELECT DISTINCT 
            p.player_id,
            p.player_name,
            p.player_name_en,
            p.throw
        FROM players p
        WHERE (p.player_name LIKE ? OR p.player_name_en LIKE ?)
        AND EXISTS (
            SELECT 1 FROM pitching pt 
            WHERE pt.player_id = p.player_id
        )
        ORDER BY p.player_name
        LIMIT ?
        """
        
        search_term = f"%{query}%"
        cursor = conn.execute(search_query, (search_term, search_term, limit))
        results = cursor.fetchall()
        
        players = []
        for row in results:
            players.append({
                'player_id': row[0],
                'player_name': row[1],
                'player_name_en': row[2],
                'handedness': row[3] if row[3] else 'Unknown'
            })
        
        return jsonify({'players': players})
        
    except Exception as e:
        print(f"Pitcher search error: {e}")
        return jsonify({'error': 'Failed to search pitchers', 'details': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

# Event-level Filtering Functions

def has_situational_filters(situational_filters):
    """Check if any situational filters are active"""
    if not situational_filters:
        return False
    
    # Check each filter type
    filters_to_check = ['inning', 'outs', 'on_base', 'count', 'batter', 'pitcher']
    
    for filter_name in filters_to_check:
        filter_data = situational_filters.get(filter_name)
        if filter_data:
            if filter_name in ['batter', 'pitcher']:
                # For complex filters, check if handedness or players are selected
                handedness = filter_data.get('handedness', [])
                players = filter_data.get('players', [])
                
                # Check if handedness has actual filters (not just 'all' or empty)
                has_handedness_filter = (handedness and 
                                       len(handedness) > 0 and 
                                       not (len(handedness) == 1 and handedness[0] == 'all') and
                                       handedness != ['all'])
                
                # Check if players are selected
                has_players_filter = players and len(players) > 0
                
                if has_handedness_filter or has_players_filter:
                    return True
            elif isinstance(filter_data, list) and len(filter_data) > 0:
                # Check if list has actual filters (not just 'all' or empty)
                # More strict checking - must have values and not be default 'all'
                if (filter_data != ['all'] and 
                    len(filter_data) > 0 and 
                    not (len(filter_data) == 1 and filter_data[0] == 'all')):
                    return True
    
    return False

def build_situational_where_clause(situational_filters):
    """Build WHERE clause conditions for situational filtering"""
    conditions = []
    params = []
    
    if not situational_filters:
        return conditions, params
    
    # Inning filter
    innings = situational_filters.get('inning', [])
    if innings and 'all' not in innings:
        # Filter out 'all' values
        valid_innings = [inning for inning in innings if inning != 'all']
        if valid_innings:
            placeholders = ','.join(['?' for _ in valid_innings])
            conditions.append(f"e.inning IN ({placeholders})")
            params.extend(valid_innings)
    
    # Outs filter
    outs = situational_filters.get('outs', [])
    if outs and 'all' not in outs:
        # Filter out 'all' values
        valid_outs = [out for out in outs if out != 'all']
        if valid_outs:
            placeholders = ','.join(['?' for _ in valid_outs])
            conditions.append(f"e.out IN ({placeholders})")
            params.extend([int(out) for out in valid_outs])
    
    # On base filter
    on_base = situational_filters.get('on_base', [])
    if on_base and 'all' not in on_base:
        # Filter out 'all' values
        valid_on_base = [base for base in on_base if base != 'all']
        if valid_on_base:
            on_base_conditions = []
            for base_situation in valid_on_base:
                if base_situation == '':  # Bases empty (empty string)
                    on_base_conditions.append("(e.on_base IS NULL OR e.on_base = '')")
                else:
                    on_base_conditions.append("e.on_base = ?")
                    params.append(base_situation)
            
            if on_base_conditions:
                conditions.append(f"({' OR '.join(on_base_conditions)})")
    
    # Count filter
    counts = situational_filters.get('count', [])
    if counts and 'all' not in counts:
        # Filter out 'all' values
        valid_counts = [count for count in counts if count != 'all']
        if valid_counts:
            placeholders = ','.join(['?' for _ in valid_counts])
            conditions.append(f"e.count IN ({placeholders})")
            params.extend(valid_counts)
    
    # Batter filter
    batter_filter = situational_filters.get('batter')
    if batter_filter:
        batter_conditions = []
        
        # Handedness filter
        handedness = batter_filter.get('handedness', [])
        if handedness and 'all' not in handedness:
            # Filter out 'all' values
            valid_handedness = [h for h in handedness if h != 'all']
            if valid_handedness:
                placeholders = ','.join(['?' for _ in valid_handedness])
                batter_conditions.append(f"pb.bat IN ({placeholders})")
                params.extend(valid_handedness)
        
        # Specific players filter
        players = batter_filter.get('players', [])
        if players:
            placeholders = ','.join(['?' for _ in players])
            batter_conditions.append(f"e.batter_player_id IN ({placeholders})")
            params.extend(players)
        
        if batter_conditions:
            conditions.append(f"({' AND '.join(batter_conditions)})")
    
    # Pitcher filter
    pitcher_filter = situational_filters.get('pitcher')
    if pitcher_filter:
        pitcher_conditions = []
        
        # Handedness filter
        handedness = pitcher_filter.get('handedness', [])
        if handedness and 'all' not in handedness:
            # Filter out 'all' values
            valid_handedness = [h for h in handedness if h != 'all']
            if valid_handedness:
                placeholders = ','.join(['?' for _ in valid_handedness])
                pitcher_conditions.append(f"pp.throw IN ({placeholders})")
                params.extend(valid_handedness)
        
        # Specific players filter
        players = pitcher_filter.get('players', [])
        if players:
            placeholders = ','.join(['?' for _ in players])
            pitcher_conditions.append(f"e.pitcher_player_id IN ({placeholders})")
            params.extend(players)
        
        if pitcher_conditions:
            conditions.append(f"({' AND '.join(pitcher_conditions)})")
    
    return conditions, params

def get_batting_stats_from_events1(conn, filters):
    """Calculate batting stats from event table when situational filters are present"""
    game_filters = filters.get('game_filters', {})
    situational_filters = filters.get('situational_filters', {})
    aggregate_by_season = filters.get('aggregate_by_season', False)
    limit = filters.get('limit', None)
    qualifiers = filters.get('qualifiers', {})
    
    # Build game-level WHERE conditions
    game_conditions = []
    game_params = []
    
    # Game type filter
    if game_filters.get('game_types') and len(game_filters['game_types']) > 0:
        placeholders = ','.join(['?' for _ in game_filters['game_types']])
        game_conditions.append(f"g.gametype IN ({placeholders})")
        game_params.extend(game_filters['game_types'])
    
    # Win/Loss filter
    win_loss = game_filters.get('win_loss', [])
    if isinstance(win_loss, str):
        win_loss = [win_loss]
    if win_loss and 'all' not in win_loss:
        win_loss_conditions = []
        if 'wins' in win_loss:
            win_loss_conditions.append("e.team = g.winning_team_id")
        if 'losses' in win_loss:
            win_loss_conditions.append("e.team = g.losing_team_id")
        if win_loss_conditions:
            game_conditions.append(f"({' OR '.join(win_loss_conditions)})")
    
    # Home/Road filter
    home_road = game_filters.get('home_road', [])
    if isinstance(home_road, str):
        home_road = [home_road]
    if home_road and 'all' not in home_road:
        home_road_conditions = []
        if 'home' in home_road:
            home_road_conditions.append("e.team = g.home_team_id")
        if 'road' in home_road:
            home_road_conditions.append("e.team = g.away_team_id")
        if home_road_conditions:
            game_conditions.append(f"({' OR '.join(home_road_conditions)})")
    
    # Month filter
    months = game_filters.get('month', [])
    if isinstance(months, str):
        months = [months]
    if months and 'all' not in months:
        valid_months = [month for month in months if month != 'all' and str(month).isdigit()]
        if valid_months:
            placeholders = ','.join(['?' for _ in valid_months])
            game_conditions.append(f"strftime('%m', g.date) IN ({placeholders})")
            game_params.extend([f"{int(month):02d}" for month in valid_months])
    
    # Ballpark filter
    ballparks = game_filters.get('ballpark', [])
    if isinstance(ballparks, str):
        ballparks = [ballparks]
    if ballparks and 'all' not in ballparks:
        valid_ballparks = [bp for bp in ballparks if bp != 'all']
        if valid_ballparks:
            placeholders = ','.join(['?' for _ in valid_ballparks])
            game_conditions.append(f"g.ballpark IN ({placeholders})")
            game_params.extend(valid_ballparks)
    
    # Date range filter
    date_range = game_filters.get('date_range', {})
    if date_range.get('start'):
        game_conditions.append("g.date >= ?")
        game_params.append(date_range['start'])
    if date_range.get('end'):
        game_conditions.append("g.date <= ?")
        game_params.append(date_range['end'])
    
    # Build situational WHERE conditions
    situational_conditions, situational_params = build_situational_where_clause(situational_filters)
    
    # Combine all conditions
    all_conditions = game_conditions + situational_conditions
    all_params = game_params + situational_params
    
    # Build WHERE clause
    where_clause = "WHERE 1=1"
    if all_conditions:
        where_clause += " AND " + " AND ".join(all_conditions)
    
    # Build GROUP BY clause
    if aggregate_by_season:
        group_by = "GROUP BY g.season, e.batter_player_id, p.player_name, p.player_name_en"
        select_season = "g.season"
        order_by = "ORDER BY g.season DESC, obp DESC"  # Use obp for sorting, will re-sort by wrc_plus in Python
    else:
        group_by = "GROUP BY e.batter_player_id, p.player_name, p.player_name_en"  
        select_season = "'Career' as season"
        order_by = "ORDER BY obp DESC"  # Use obp for sorting, will re-sort by wrc_plus in Python
    
    # Event-based batting query with manual stat calculation
    query = f"""
    SELECT 
        {select_season},
        p.player_name as name,
        p.player_name_en as name_en,
        p.player_id,
        COUNT(*) as pa,
        SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END) as ab,
        SUM(e.h) as h,
        SUM(e.rbi) as rbi,
        SUM(e."2b") as doubles,
        SUM(e."3b") as triples, 
        SUM(e.hr) as hr,
        SUM(e.k) as k,
        SUM(e.bb) as bb,
        SUM(e.hbp) as hbp,
        SUM(e.sac) as sac,
        SUM(e.gdp) as gidp,
        
        -- Calculate derived stats
        (SUM(e.h) + SUM(e."2b") + 2*SUM(e."3b") + 3*SUM(e.hr)) as tb,
         
        ROUND(CAST(SUM(e.h) AS FLOAT) / 
              NULLIF(SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END), 0), 3) as avg,
              
        ROUND(CAST((SUM(e.h) + SUM(e."2b") + 2*SUM(e."3b") + 3*SUM(e.hr)) AS FLOAT) / 
              NULLIF(SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END), 0), 3) as slg,
              
        ROUND(CAST((SUM(e.h) + SUM(e.bb) + SUM(e.hbp)) AS FLOAT) / 
              NULLIF(COUNT(*), 0), 3) as obp,
              
        ROUND((0.69*SUM(e.bb) + 0.72*SUM(e.hbp) + 0.89*(SUM(e.h)-SUM(e."2b")-SUM(e."3b")-SUM(e.hr)) + 1.27*SUM(e."2b") + 1.62*SUM(e."3b") + 2.10*SUM(e.hr)) / NULLIF(COUNT(*), 0), 3) as woba,
              
        ROUND(CAST((SUM(e.h) - SUM(e.hr)) AS FLOAT) / 
              NULLIF(SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END) - SUM(e.k) - SUM(e.hr), 0), 3) as babip,
              
        -- Percentage stats
        ROUND(CAST(SUM(e.k) AS FLOAT) / NULLIF(COUNT(*), 0) * 100, 1) as k_pct,
        ROUND(CAST(SUM(e.bb) AS FLOAT) / NULLIF(COUNT(*), 0) * 100, 1) as bb_pct,
        ROUND(CAST(SUM(e."2b") + SUM(e."3b") + SUM(e.hr) AS FLOAT) / NULLIF(SUM(e.h), 0) * 100, 1) as xbh_pct
        
    FROM event e
    JOIN games g ON e.game_id = g.game_id
    JOIN players p ON e.batter_player_id = p.player_id
    LEFT JOIN players pb ON e.batter_player_id = pb.player_id
    LEFT JOIN players pp ON e.pitcher_player_id = pp.player_id
    {where_clause}
    {group_by}
    """
    
    # Add qualifiers as HAVING clause
    having_conditions = []
    having_params = []
    
    for qualifier_name, qualifier_config in qualifiers.items():
        min_val = qualifier_config.get('min')
        max_val = qualifier_config.get('max')
        
        if qualifier_name == 'pa':
            if min_val is not None:
                having_conditions.append("COUNT(*) >= ?")
                having_params.append(min_val)
            if max_val is not None:
                having_conditions.append("COUNT(*) <= ?")
                having_params.append(max_val)
        elif qualifier_name == 'ab':
            if min_val is not None:
                having_conditions.append("SUM(CASE WHEN e.bb = 0 AND e.hbp = 0 AND e.sac = 0 THEN 1 ELSE 0 END) >= ?")
                having_params.append(min_val)
    
    if having_conditions:
        query += " HAVING " + " AND ".join(having_conditions)
    
    query += f" {order_by}"
    
    if limit:
        query += " LIMIT ?"
        having_params.append(limit)
    
    # Execute query
    final_params = all_params + having_params
    cursor = conn.execute(query, final_params)
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 (removed 'g', 'r' and 'roe' since these can't be calculated from event table)
    numeric_fields = ['pa', 'ab', 'h', 'doubles', 'triples', 'hr', 'tb', 'rbi', 'k', 'bb', 'hbp', 'sac', 'gidp']
    for result in results:
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
        
        # Calculate ISO
        if result.get('slg') is not None and result.get('avg') is not None:
            result['iso'] = round(result['slg'] - result['avg'], 3)
        else:
            result['iso'] = 0.000
    
    # Add dynamically calculated advanced stats  
    results = enhance_batting_stats_with_advanced(results)
    
    # Re-sort by wrc_plus after enhancement (since SQL couldn't sort by it)
    if aggregate_by_season:
        # Sort by season desc, then wrc_plus desc
        results = sorted(results, key=lambda x: (-x.get('season', 0), -x.get('wrc_plus', 0)))
    else:
        # Sort by wrc_plus desc
        results = sorted(results, key=lambda x: -x.get('wrc_plus', 0))
    
    return jsonify({'results': results, 'total': len(results)})

def get_pitching_stats_from_events(conn, filters):
    """Calculate pitching stats from event table when situational filters are present"""
    game_filters = filters.get('game_filters', {})
    situational_filters = filters.get('situational_filters', {})
    aggregate_by_season = filters.get('aggregate_by_season', False)
    limit = filters.get('limit', None)
    qualifiers = filters.get('qualifiers', {})
    
    # Build game-level WHERE conditions (similar to batting)
    game_conditions = []
    game_params = []
    
    # Game type filter
    if game_filters.get('game_types') and len(game_filters['game_types']) > 0:
        placeholders = ','.join(['?' for _ in game_filters['game_types']])
        game_conditions.append(f"g.gametype IN ({placeholders})")
        game_params.extend(game_filters['game_types'])
    
    # Add other game filters (similar logic to batting)
    # Win/Loss filter
    win_loss = game_filters.get('win_loss', [])
    if isinstance(win_loss, str):
        win_loss = [win_loss]
    if win_loss and 'all' not in win_loss:
        win_loss_conditions = []
        if 'wins' in win_loss:
            win_loss_conditions.append("e.team = g.winning_team_id")
        if 'losses' in win_loss:
            win_loss_conditions.append("e.team = g.losing_team_id")
        if win_loss_conditions:
            game_conditions.append(f"({' OR '.join(win_loss_conditions)})")
    
    # Build situational WHERE conditions
    situational_conditions, situational_params = build_situational_where_clause(situational_filters)
    
    # Combine all conditions
    all_conditions = game_conditions + situational_conditions
    all_params = game_params + situational_params
    
    # Build WHERE clause
    where_clause = "WHERE 1=1"
    if all_conditions:
        where_clause += " AND " + " AND ".join(all_conditions)
    
    # Build GROUP BY clause
    if aggregate_by_season:
        group_by = "GROUP BY g.season, e.pitcher_player_id, p.player_name, p.player_name_en"
        select_season = "g.season"
        order_by = "ORDER BY g.season DESC, COUNT(*) DESC"  # Order by batters faced since no ERA+
    else:
        group_by = "GROUP BY e.pitcher_player_id, p.player_name, p.player_name_en"  
        select_season = "'Career' as season"
        order_by = "ORDER BY COUNT(*) DESC"  # Order by batters faced since no ERA+
    
    # Event-based pitching query - only calculate stats available from event data
    query = f"""
    SELECT 
        {select_season},
        p.player_name as name,
        p.player_name_en as name_en,
        p.player_id,
        COUNT(DISTINCT e.game_id) as g,
        COUNT(*) as batters_faced,
        
        -- Basic pitching stats from events (only what's available)
        SUM(e.h) as h,
        SUM(e."2b" + e."3b" + e.hr) as xbh,
        SUM(e.hr) as hr,
        SUM(e.bb) as bb,
        SUM(e.k) as k,
        SUM(e.hbp) as hbp
        
    FROM event e
    JOIN games g ON e.game_id = g.game_id
    JOIN players p ON e.pitcher_player_id = p.player_id
    LEFT JOIN players pb ON e.batter_player_id = pb.player_id
    LEFT JOIN players pp ON e.pitcher_player_id = pp.player_id
    {where_clause}
    {group_by}
    """
    
    # Add qualifiers as HAVING clause
    having_conditions = []
    having_params = []
    
    for qualifier_name, qualifier_config in qualifiers.items():
        min_val = qualifier_config.get('min')
        max_val = qualifier_config.get('max')
        
        if qualifier_name == 'ip':
            # IP cannot be calculated from event table, skip this qualifier
            pass
        elif qualifier_name == 'g':
            if min_val is not None:
                having_conditions.append("COUNT(DISTINCT e.game_id) >= ?")
                having_params.append(min_val)
    
    if having_conditions:
        query += " HAVING " + " AND ".join(having_conditions)
    
    query += f" {order_by}"
    
    if limit:
        query += " LIMIT ?"
        having_params.append(limit)
    
    # Execute query
    final_params = all_params + having_params
    cursor = conn.execute(query, final_params)
    results = [dict(row) for row in cursor.fetchall()]
    
    # Set null values to 0 for event-based stats only
    numeric_fields = ['g', 'batters_faced', 'h', 'xbh', 'hr', 'bb', 'k', 'hbp']
    for result in results:
        for field in numeric_fields:
            if result.get(field) is None:
                result[field] = 0
    
    # Note: Advanced stats (ERA, ERA+, FIP, WHIP, etc.) cannot be calculated from event data
    
    return jsonify({'results': results, 'total': len(results)})

# Route to serve the advanced.html page
@app.route('/advanced')
def advanced_page():
    """Serve the advanced stats page"""
    return send_from_directory('frontend', 'advanced.html')

@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory('frontend', filename)

@app.route('/about')
def about_page():
    """Serve the about page"""
    return app.send_static_file('about.html')


if __name__ == '__main__':
    print("🚀 Starting Advanced Stat Finder App...")
    print("📊 Available endpoints:")
    print("   - POST /api/advanced-stats/standard")
    print("   - POST /api/advanced-stats/filtered")
    print("   - POST /api/advanced-stats/teams/batting")
    print("   - POST /api/advanced-stats/teams/pitching")
    print("   - GET  /api/players/search/batters")
    print("   - GET  /api/players/search/pitchers")
    print("   - GET  /api/options/game-types")
    print("   - GET  /api/options/ballparks") 
    print("   - GET  /advanced (Advanced Stat Finder page)")
    print("🌐 Server running at: http://127.0.0.1:5000")
    print("🔍 Situational filtering now supports:")
    print("   - Inning-by-inning analysis")
    print("   - Outs-based filtering")
    print("   - On-base situations")
    print("   - Count-specific analysis")
    print("   - Batter/Pitcher handedness and individual player selection")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
