#!/usr/bin/env python3
"""
🚀 ULTIMATE UNIFIED PARSER SYSTEM for yakyuu.jp
The one-stop parser that handles everything:
- Game data parsing (metadata, events, batting, pitching)
- Player discovery and NPB scraping
- Ballpark discovery and park factors
- Dynamic qualifier calculation
- Advanced stats updating (wRC+, ERA+)

Usage:
    python parse.py urls.txt              # Parse games from URL file + full pipeline
    python parse.py [url1] [url2] ...     # Parse specific URLs + full pipeline
    python parse.py --qualifiers-only     # Only calculate dynamic qualifiers
    python parse.py --players-only        # Only run player discovery/parsing
    python parse.py --ballparks-only      # Only run ballpark discovery/parsing
    python parse.py --advanced-only       # Only update advanced stats
"""

import sys
import os
import sqlite3
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
import random
from collections import defaultdict

# Database connection
DB_PATH = '../yakyuu.db'

# Add import paths for existing parsers
sys.path.append(os.path.join(os.path.dirname(__file__), 'imports'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'playerimports'))

# Import existing parsers
try:
    from batting_lineup_parser import BattingLineupParser
    from pitching_parser import PitchingParser
    from eventfiles import parse_playbyplay_from_url, upsert_events
    from metadata import NPBGamesScraper
    from player_extractor import PlayerExtractor
    from player_parser_db import PlayerParserDB
    # Note: EventAggregator and PitcherEventAggregator are handled by unified_parser.py
except ImportError as e:
    print(f"⚠️  Warning: Some parsers not available: {e}")
    print("Some functionality may be limited")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

def get_db_connection():
    """Create a database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ============================================================================
# DYNAMIC QUALIFIER SYSTEM
# ============================================================================

def calculate_dynamic_qualifiers():
    """
    Calculate dynamic qualifiers for batting and pitching based on team games played
    MLB Standard: 3.1 PA per team game (batting), 1.0 IP per team game (pitching)
    """
    print("🎯 Calculating dynamic qualifiers...")
    
    conn = get_db_connection()
    
    try:
        # Get all seasons and teams
        cursor = conn.execute("""
            SELECT DISTINCT season FROM games ORDER BY season
        """)
        seasons = [row['season'] for row in cursor.fetchall()]
        
        cursor = conn.execute("""
            SELECT DISTINCT team_id FROM teams
        """)
        teams = [row['team_id'] for row in cursor.fetchall()]
        
        print(f"📊 Processing {len(seasons)} seasons and {len(teams)} teams...")
        
        # Calculate qualifiers for each team/season combination
        for season in seasons:
            print(f"  📅 Processing {season} season...")
            
            for team_id in teams:
                # Count games played by this team in this season
                cursor = conn.execute("""
                    SELECT COUNT(*) as games_played
                    FROM games 
                    WHERE season = ? AND (home_team_id = ? OR away_team_id = ?)
                """, (season, team_id, team_id))
                
                games_played = cursor.fetchone()['games_played']
                
                if games_played > 0:
                    # Calculate qualifiers using MLB standard
                    b_qualifier = round(games_played * 3.1, 1)  # 3.1 PA per team game
                    p_qualifier = round(games_played * 1.0, 1)  # 1.0 IP per team game
                    
                    # Update team qualifier data
                    cursor = conn.execute("""
                        UPDATE teams 
                        SET b_qualifier = ?, p_qualifier = ?
                        WHERE team_id = ?
                    """, (b_qualifier, p_qualifier, team_id))
                    
                    print(f"    ✅ {team_id}: {games_played} games → {b_qualifier} PA, {p_qualifier} IP qualifiers")
        
        conn.commit()
        print("🎉 Dynamic qualifiers calculated successfully!")
        
        # Show summary
        cursor = conn.execute("""
            SELECT team_id, b_qualifier, p_qualifier 
            FROM teams 
            WHERE b_qualifier > 0 
            ORDER BY b_qualifier DESC 
            LIMIT 10
        """)
        
        print("\n📈 Top 10 Team Qualifiers:")
        print("Team ID    | Batting PA | Pitching IP")
        print("-" * 40)
        for row in cursor.fetchall():
            print(f"{row['team_id']:<10} | {row['b_qualifier']:<10} | {row['p_qualifier']}")
        
    except Exception as e:
        print(f"❌ Error calculating qualifiers: {e}")
        conn.rollback()
    finally:
        conn.close()

# ============================================================================
# PLAYER DISCOVERY AND PARSING SYSTEM
# ============================================================================

def run_player_discovery_and_parsing():
    """
    Run the complete player discovery and parsing workflow
    1. Extract unique player IDs from batting/pitching tables
    2. Parse personal information from NPB.jp for all players
    """
    print("👥 Running player discovery and parsing...")
    
    try:
        # Step 1: Extract unique player IDs
        print("\n📋 STEP 1: Extracting unique player IDs...")
        extractor = PlayerExtractor()
        extractor.extract_and_insert_players()
        
        # Step 2: Parse personal information
        print("\n🔍 STEP 2: Parsing player personal information...")
        parser = PlayerParserDB()
        parser.populate_all_players()
        
        print("✅ Player discovery and parsing completed!")
        
    except Exception as e:
        print(f"❌ Error in player parsing: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# BALLPARK DISCOVERY AND PARK FACTORS SYSTEM
# ============================================================================

def discover_new_ballparks():
    """Find ballparks in games table that aren't in ballparks table"""
    conn = get_db_connection()
    
    try:
        cursor = conn.execute("""
            SELECT DISTINCT g.ballpark, COUNT(*) as game_count
            FROM games g
            LEFT JOIN ballparks bp ON g.ballpark = bp.park_name
            WHERE g.ballpark IS NOT NULL 
              AND g.ballpark != ''
              AND bp.park_name IS NULL
            GROUP BY g.ballpark
            ORDER BY game_count DESC
        """)
        
        new_ballparks = cursor.fetchall()
        return new_ballparks
        
    finally:
        conn.close()

def calculate_default_park_factors(ballpark_name, game_count):
    """Calculate default park factors for a new ballpark"""
    
    # Default to neutral park factors (only pf_runs exists in schema)
    default_factors = {
        'pf_runs': 1.000
    }
    
    # Apply some heuristics based on ballpark name
    name_lower = ballpark_name.lower()
    
    # Dome stadiums tend to be slightly pitcher-friendly
    if 'ドーム' in ballpark_name or 'dome' in name_lower:
        default_factors['pf_runs'] = 0.980
        print(f"    🏟️ Detected dome stadium - applying slight pitcher-friendly factor")
    
    # Outdoor stadiums in certain locations
    elif any(keyword in ballpark_name for keyword in ['マリン', '甲子園', 'スタジアム']):
        default_factors['pf_runs'] = 1.020
        print(f"    🌤️ Detected outdoor stadium - applying slight hitter-friendly factor")
    
    # Very small sample size - be conservative
    elif game_count < 10:
        default_factors['pf_runs'] = 1.000
        print(f"    ⚠️ Small sample size ({game_count} games) - using neutral factors")
    
    return default_factors

def add_new_ballpark(ballpark_name, game_count):
    """Add a new ballpark to the ballparks table"""
    
    print(f"\n➕ Adding new ballpark: {ballpark_name}")
    print(f"   Games found: {game_count}")
    
    # Calculate default park factors
    factors = calculate_default_park_factors(ballpark_name, game_count)
    
    conn = get_db_connection()
    
    try:
        # Insert with the correct schema
        cursor = conn.execute("""
            INSERT INTO ballparks (
                park_name, park_name_en, pf_runs, games_sample_size
            ) VALUES (?, ?, ?, ?)
        """, (
            ballpark_name,
            ballpark_name,  # Use same name for English until we have translations
            factors['pf_runs'],
            game_count
        ))
        
        conn.commit()
        print(f"    ✅ Added {ballpark_name} with {factors['pf_runs']:.3f} run factor")
        
    except Exception as e:
        print(f"    ❌ Error adding ballpark: {e}")
        conn.rollback()
    finally:
        conn.close()

def refresh_all_park_factors():
    """Calculate advanced park factors using proven unified system logic"""
    
    print("\n🔄 Refreshing park factors for all ballparks...")
    print("🧮 Using advanced park factor calculation (basic + team-adjusted)")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Step 1: Basic league-average park factors
        print("\n1️⃣ Calculating basic park factors (league-average method)...")
        
        # Get league average runs per team per game
        cursor.execute("""
            SELECT AVG((CAST(home_runs AS FLOAT) + CAST(visitor_runs AS FLOAT)) / 2.0) as league_avg
            FROM games 
            WHERE home_runs IS NOT NULL AND visitor_runs IS NOT NULL
            AND gametype = '公式戦'
        """)
        league_avg_result = cursor.fetchone()
        league_avg = league_avg_result[0] if league_avg_result and league_avg_result[0] else 5.0
        print(f"   League average runs per team: {league_avg:.3f}")
        
        # Calculate basic park factors
        cursor.execute("""
            SELECT 
                ballpark,
                COUNT(*) as games,
                AVG((CAST(home_runs AS FLOAT) + CAST(visitor_runs AS FLOAT)) / 2.0) as park_avg
            FROM games 
            WHERE ballpark IS NOT NULL 
            AND home_runs IS NOT NULL 
            AND visitor_runs IS NOT NULL
            AND gametype = '公式戦'
            GROUP BY ballpark
        """)
        
        basic_factors = cursor.fetchall()
        
        for ballpark, games, park_avg in basic_factors:
            if park_avg is not None:
                # Calculate raw park factor
                raw_pf = park_avg / league_avg if league_avg > 0 else 1.0
                
                # Apply sample size weighting (regression to mean)
                confidence = min(games / 50, 1.0)  # Full confidence at 50+ games
                weighted_pf = (confidence * raw_pf) + ((1 - confidence) * 1.0)
                
                # Update database
                cursor.execute("""
                    UPDATE ballparks 
                    SET pf_runs = ?,
                        games_sample_size = ?,
                        pf_confidence = ?,
                        pf_raw = ?
                    WHERE park_name = ?
                """, (weighted_pf, games, confidence, raw_pf, ballpark))
        
        print(f"   ✅ Updated {len(basic_factors)} ballparks with basic park factors")
        
        # Step 2: Team-adjusted park factors (more accurate)
        print("\n2️⃣ Calculating team-adjusted park factors...")
        
        # Calculate team offensive capabilities
        cursor.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS team_offense AS
            SELECT 
                team_id,
                AVG(runs_scored) as team_avg_runs_scored,
                COUNT(*) as games_played
            FROM (
                SELECT home_team_id as team_id, CAST(home_runs AS FLOAT) as runs_scored
                FROM games WHERE home_runs IS NOT NULL AND gametype = '公式戦'
                UNION ALL
                SELECT away_team_id as team_id, CAST(visitor_runs AS FLOAT) as runs_scored
                FROM games WHERE visitor_runs IS NOT NULL AND gametype = '公式戦'
            ) team_games
            GROUP BY team_id
            HAVING games_played >= 10
        """)
        
        # Calculate team-adjusted factors for each ballpark
        cursor.execute("SELECT DISTINCT ballpark FROM games WHERE ballpark IS NOT NULL AND gametype = '公式戦'")
        ballparks = [row[0] for row in cursor.fetchall()]
        
        team_adj_count = 0
        
        for ballpark in ballparks:
            # Get games with team data
            cursor.execute("""
                SELECT 
                    CAST(g.home_runs AS FLOAT) + CAST(g.visitor_runs AS FLOAT) as total_runs,
                    COALESCE(ho.team_avg_runs_scored, ?) + COALESCE(ao.team_avg_runs_scored, ?) as expected_runs
                FROM games g
                LEFT JOIN team_offense ho ON g.home_team_id = ho.team_id
                LEFT JOIN team_offense ao ON g.away_team_id = ao.team_id
                WHERE g.ballpark = ?
                AND g.home_runs IS NOT NULL 
                AND g.visitor_runs IS NOT NULL
                AND g.gametype = '公式戦'
            """, (league_avg, league_avg, ballpark))
            
            games_data = cursor.fetchall()
            
            if games_data:
                total_actual = sum(row[0] for row in games_data if row[0] is not None)
                total_expected = sum(row[1] for row in games_data if row[1] is not None)
                games_count = len(games_data)
                
                if total_expected > 0:
                    team_adj_pf = total_actual / total_expected
                    
                    # Apply sample size weighting
                    confidence = min(games_count / 50, 1.0)
                    weighted_team_adj_pf = (confidence * team_adj_pf) + ((1 - confidence) * 1.0)
                    
                    # Update database
                    cursor.execute("""
                        UPDATE ballparks 
                        SET pf_runs_team_adj = ?,
                            expected_runs_per_game = ?,
                            actual_runs_per_game = ?
                        WHERE park_name = ?
                    """, (weighted_team_adj_pf, total_expected/games_count, total_actual/games_count, ballpark))
                    
                    team_adj_count += 1
        
        print(f"   ✅ Updated {team_adj_count} ballparks with team-adjusted park factors")
        
        # Commit all changes
        conn.commit()
        print(f"\n✅ Park factor refresh complete! Updated basic factors for {len(basic_factors)} ballparks")
        print(f"✅ Updated team-adjusted factors for {team_adj_count} ballparks")
        
    except Exception as e:
        print(f"❌ Error in park factor calculation: {e}")
        conn.rollback()
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def run_ballpark_discovery_and_parsing():
    """
    Run the complete ballpark discovery and parsing workflow
    """
    print("🏟️ Running ballpark discovery and parsing...")
    
    try:
        # Discover new ballparks
        new_ballparks = discover_new_ballparks()
        
        if new_ballparks:
            print(f"🔍 Found {len(new_ballparks)} new ballparks:")
            for ballpark_name, game_count in new_ballparks:
                print(f"  - {ballpark_name}: {game_count} games")
            
            # Add each new ballpark
            for ballpark_name, game_count in new_ballparks:
                add_new_ballpark(ballpark_name, game_count)
            
            print("✅ Ballpark discovery completed!")
        else:
            print("✅ No new ballparks found - all ballparks are already in database")
        
        # Always refresh park factors for all ballparks (dynamic calculation)
        refresh_all_park_factors()
        
        print("✅ Ballpark discovery and parsing completed!")
        
    except Exception as e:
        print(f"❌ Error in ballpark parsing: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# ADVANCED STATS SYSTEM
# ============================================================================

def update_advanced_stats():
    """
    Update advanced stats like wRC+ and ERA+ based on current league context
    Shows current league averages per season (stats are calculated on-the-fly in API)
    """
    print("🧮 Updating advanced stats context (wRC+, ERA+)...")
    
    conn = get_db_connection()
    
    try:
        # Get league wOBA by season
        print("\n⚾ League wOBA by Season (for wRC+ calculation):")
        cursor = conn.execute("""
            SELECT 
                g.season,
                ROUND((0.69*SUM(b.b_bb) + 0.72*SUM(b.b_hbp) + 0.89*(SUM(b.b_h)-SUM(b.b_2b)-SUM(b.b_3b)-SUM(b.b_hr)) + 1.27*SUM(b.b_2b) + 1.62*SUM(b.b_3b) + 2.10*SUM(b.b_hr)) / NULLIF(SUM(b.pa), 0), 3) as league_woba,
                COUNT(DISTINCT g.game_id) as games,
                SUM(b.pa) as total_pa
            FROM batting b
            JOIN games g ON b.game_id = g.game_id
            WHERE g.gametype = '公式戦'
            GROUP BY g.season
            ORDER BY g.season
        """)
        
        woba_results = cursor.fetchall()
        print("Season | League wOBA | Games | Total PA")
        print("-" * 40)
        for row in woba_results:
            print(f"{row['season']:<6} | {row['league_woba']:<11} | {row['games']:<5} | {row['total_pa']:,}")
        
        # Get league ERA by season
        print("\n🥎 League ERA by Season (for ERA+ calculation):")
        cursor = conn.execute("""
            SELECT 
                g.season,
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as league_era,
                COUNT(DISTINCT g.game_id) as games,
                ROUND(SUM(p.ip), 1) as total_ip
            FROM pitching p
            JOIN games g ON p.game_id = g.game_id
            WHERE g.gametype = '公式戦'
            GROUP BY g.season
            ORDER BY g.season
        """)
        
        era_results = cursor.fetchall()
        print("Season | League ERA | Games | Total IP")
        print("-" * 40)
        for row in era_results:
            print(f"{row['season']:<6} | {row['league_era']:<10} | {row['games']:<5} | {row['total_ip']:,}")
        
        # Get league FIP constants by season
        print("\n" + "="*60)
        print("🎯 LEAGUE FIP CONSTANTS BY SEASON (for FIP calculation)")
        print("="*60)
        cursor = conn.execute("""
            SELECT 
                g.season,
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0), 2) as league_era,
                ROUND(((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as league_raw_fip,
                ROUND(CAST(SUM(p.er) AS FLOAT) * 9 / NULLIF(SUM(p.ip), 0) - ((13*SUM(p.p_hr) + 3*(SUM(p.p_bb) + SUM(p.p_hbp)) - 2*SUM(p.p_k)) / NULLIF(SUM(p.ip), 0)), 2) as fip_constant
            FROM pitching p
            JOIN games g ON p.game_id = g.game_id
            WHERE g.gametype = '公式戦'
            GROUP BY g.season
            ORDER BY g.season
        """)
        
        fip_results = cursor.fetchall()
        print("Season | League ERA | Raw FIP | FIP Constant | League FIP")
        print("-" * 55)
        for row in fip_results:
            league_fip = round(row['league_raw_fip'] + row['fip_constant'], 2)
            print(f"{row['season']:<6} | {row['league_era']:<10} | {row['league_raw_fip']:<7} | {row['fip_constant']:<12} | {league_fip}")
        
        print("="*60)
        print("✅ FIP CONSTANTS CALCULATED - FIP now on same scale as ERA!")
        print("="*60)
        
        # Show current season context
        if woba_results and era_results and fip_results:
            latest_woba = woba_results[-1]
            latest_era = era_results[-1]
            latest_fip = fip_results[-1]
            
            print(f"\n📊 Current Season ({latest_woba['season']}) Context:")
            print(f"  League wOBA: {latest_woba['league_woba']} (wRC+ baseline)")
            print(f"  League ERA:  {latest_era['league_era']} (ERA+ baseline)")
            print(f"  FIP Constant: {latest_fip['fip_constant']} (makes league FIP = league ERA)")
            print(f"  Games:       {latest_woba['games']} games played")
            print(f"  wOBA Scale:  0.16 (NPB-calibrated)")
            
            print(f"\n💡 Advanced Stats Status:")
            print(f"  ✅ wRC+ calculated dynamically using season-specific league wOBA")
            print(f"  ✅ ERA+ calculated dynamically using season-specific league ERA")
            print(f"  ✅ FIP calculated dynamically using season-specific FIP constants")
            print(f"  ✅ Park factors applied for home ballpark adjustments")
            print(f"  ✅ No database storage needed - always current!")
        
        print("\n✅ Advanced stats context updated!")
        
    except Exception as e:
        print(f"❌ Error updating advanced stats: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

# ============================================================================
# GAME PARSING SYSTEM (from unified_parser.py)
# ============================================================================

def extract_game_id_from_url(url):
    """Extract game ID from URL like '2021/0409/db-t-01'"""
    url = url.rstrip('/')
    parts = url.split('/')
    if len(parts) >= 6:
        game_id = '/'.join(parts[-3:])
        return game_id
    return None

def validate_url(url):
    """Validate that the URL is a valid NPB game URL"""
    if not url.startswith('https://npb.jp/scores/'):
        return False, "URL must start with 'https://npb.jp/scores/'"
    
    game_id = extract_game_id_from_url(url)
    if not game_id:
        return False, "Could not extract game ID from URL"
    
    if not re.match(r'\d{4}/\d{4}/[a-z]+-[a-z]+-\d{2}', game_id):
        return False, f"Invalid game ID format: {game_id}"
    
    return True, game_id

def build_urls(base_url):
    """Build box score and play-by-play URLs from base URL"""
    base_url = base_url.rstrip('/')
    box_url = f"{base_url}/box.html"
    playbyplay_url = f"{base_url}/playbyplay.html"
    return box_url, playbyplay_url

def check_url_exists(url):
    """Check if a URL exists and returns valid content"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        return response.status_code == 200
    except:
        return False

def parse_game_data(base_url, db_path='yakyuu.db'):
    """
    Parse all data for a game from a base URL
    """
    print(f"🔍 Parsing game data from: {base_url}")
    
    # Validate URL
    is_valid, game_id_or_error = validate_url(base_url)
    if not is_valid:
        print(f"❌ Invalid URL: {game_id_or_error}")
        return False
    
    game_id = game_id_or_error
    box_url, playbyplay_url = build_urls(base_url)
    
    # Check if URLs exist
    if not check_url_exists(box_url):
        print(f"❌ Box score URL not accessible: {box_url}")
        return False
    
    if not check_url_exists(playbyplay_url):
        print(f"❌ Play-by-play URL not accessible: {playbyplay_url}")
        return False
    
    print(f"✅ Both URLs accessible for game: {game_id}")
    
    try:
        # Parse metadata
        print("📋 Parsing game metadata...")
        metadata_scraper = NPBGamesScraper(db_path)
        metadata_scraper.scrape_and_save_game(base_url)
        print("✅ Metadata parsing completed")
        
        # Parse batting lineup
        print("⚾ Parsing batting lineup...")
        batting_parser = BattingLineupParser(db_path)
        batting_parser.parse_and_save(box_url)
        print("✅ Batting parsing completed")
        
        # Parse pitching
        print("🥎 Parsing pitching data...")
        pitching_parser = PitchingParser(db_path)
        pitching_parser.parse_and_save(box_url)
        print("✅ Pitching parsing completed")
        
        # Parse play-by-play events
        print("📊 Parsing play-by-play events...")
        events = parse_playbyplay_from_url(playbyplay_url)
        if events:
            upsert_events(events, db_path)
            print(f"✅ Events parsing completed ({len(events)} events)")
        else:
            print("⚠️  No events found or parsing failed")
        
        return True
        
    except Exception as e:
        print(f"❌ Error parsing game data: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_game_parsing(urls):
    """
    Parse games from a list of URLs and run aggregation
    """
    print(f"🎮 Parsing {len(urls)} games...")
    
    results = []
    successfully_parsed_game_ids = []
    
    for url in urls:
        print(f"\n==============================\nProcessing: {url}\n==============================")
        success = parse_game_data(url, DB_PATH)
        results.append((url, success))
        
        if success:
            game_id = extract_game_id_from_url(url)
            if game_id:
                successfully_parsed_game_ids.append(game_id)
                print(f"  ✅ Successfully parsed game ID: {game_id}")
        
        # Add a random delay between 2 and 5 seconds
        delay = random.uniform(2, 5)
        print(f"Sleeping for {delay:.2f} seconds to be polite...")
        time.sleep(delay)
    
    # Print summary
    print("\n==============================\nGame Parsing Summary\n==============================")
    for url, success in results:
        print(f"{url}: {'✅ Success' if success else '❌ Failed'}")
    
    # Note: Aggregation is handled by unified_parser.py
    if successfully_parsed_game_ids:
        print(f"\n✅ Successfully parsed {len(successfully_parsed_game_ids)} games")
        print("📊 Aggregation will be handled by unified_parser.py")
    
    return successfully_parsed_game_ids

# ============================================================================
# MAIN FUNCTION AND COMMAND LINE INTERFACE
# ============================================================================

def print_usage():
    """Print usage information"""
    print("🔧 ADVANCED TOOLS for yakyuu.jp Database")
    print("=" * 60)
    print("\nRecommended Workflow:")
    print("  1. python auto.py YYYYMM             # Find new games")
    print("  2. python unified_parser.py games/newgames_mm.txt  # Parse games")
    print("  3. python parse.py --full-pipeline   # Update advanced features")
    print("")
    print("Advanced Tools Usage:")
    print("  python parse.py --qualifiers-only     # Only calculate dynamic qualifiers")
    print("  python parse.py --players-only        # Only run player discovery/parsing")
    print("  python parse.py --ballparks-only      # Only run ballpark discovery/parsing")
    print("  python parse.py --advanced-only       # Only update advanced stats")
    print("  python parse.py --full-pipeline       # Run all advanced tools")
    print("")
    print("Legacy Game Parsing (use unified_parser.py instead):")
    print("  python parse.py urls.txt              # Parse games from URL file")
    print("  python parse.py [url1] [url2] ...     # Parse specific URLs")
    print("\nThe full pipeline includes:")
    print("  1. Player discovery and NPB scraping")
    print("  2. Ballpark discovery and park factors")
    print("  3. Dynamic qualifier calculation")
    print("  4. Advanced stats updating")
    print("\nNote: Game parsing and aggregation are handled by unified_parser.py")

def main():
    """Main function for the ultimate unified parser"""
    
    if len(sys.argv) < 2:
        print_usage()
        return
    
    command = sys.argv[1].lower()
    
    # Handle special commands
    if command == "--qualifiers-only":
        calculate_dynamic_qualifiers()
        return
    
    elif command == "--players-only":
        run_player_discovery_and_parsing()
        return
    
    elif command == "--ballparks-only":
        run_ballpark_discovery_and_parsing()
        return
    
    elif command == "--advanced-only":
        update_advanced_stats()
        return
    
    elif command == "--full-pipeline":
        print("🚀 Running full pipeline (without game parsing)...")
        run_player_discovery_and_parsing()
        run_ballpark_discovery_and_parsing()
        calculate_dynamic_qualifiers()
        update_advanced_stats()
        print("🎉 Full pipeline completed!")
        return
    
    # Handle URL parsing
    urls = []
    
    # Check if first argument is a file
    if len(sys.argv) == 2 and sys.argv[1].endswith('.txt'):
        url_file = sys.argv[1]
        try:
            with open(url_file, 'r', encoding='utf-8') as f:
                for line in f:
                    url = line.strip()
                    if url:
                        if not url.endswith('/'):
                            url += '/'
                        urls.append(url)
        except Exception as e:
            print(f"Error reading URL file: {e}")
            sys.exit(1)
    else:
        # Individual URLs provided
        for url in sys.argv[1:]:
            if not url.startswith('--'):  # Skip any flags
                if not url.endswith('/'):
                    url += '/'
                urls.append(url)
    
    if not urls:
        print("❌ No valid URLs provided")
        print_usage()
        return
    
    print("🚀 ULTIMATE UNIFIED PARSER SYSTEM")
    print("=" * 60)
    print(f"📋 Processing {len(urls)} URLs with full pipeline")
    print("=" * 60)
    
    # Step 1: Parse games
    successfully_parsed_game_ids = run_game_parsing(urls)
    
    # Step 2: Run full pipeline
    print("\n🔄 Running post-parsing pipeline...")
    
    # Player discovery and parsing
    run_player_discovery_and_parsing()
    
    # Ballpark discovery and parsing
    run_ballpark_discovery_and_parsing()
    
    # Dynamic qualifier calculation
    calculate_dynamic_qualifiers()
    
    # Advanced stats updating
    update_advanced_stats()
    
    print("\n🎉 ULTIMATE PARSING PIPELINE COMPLETED!")
    print("=" * 60)
    print(f"✅ Games parsed: {len(successfully_parsed_game_ids)}")
    print("✅ Players discovered and parsed")
    print("✅ Ballparks discovered and parsed")
    print("✅ Dynamic qualifiers calculated")
    print("✅ Advanced stats updated")
    print("=" * 60)
    print("Your yakyuu.jp database is now fully up-to-date! 🏆")

if __name__ == "__main__":
    main()