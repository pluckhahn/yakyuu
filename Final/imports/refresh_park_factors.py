#!/usr/bin/env python3
"""
Park Factor Refresh - Updated for cleaned ballparks table structure
Calculates advanced park factors with multiple methods
"""

import sqlite3
import os
import sys
from datetime import datetime

def get_database_connection():
    """Get database connection"""
    db_path = r"c:\Users\pluck\Documents\yakyuu\yakyuu.db"
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    return sqlite3.connect(db_path)

def populate_new_ballparks():
    """Add any new ballparks found in games table"""
    
    # Import the unified ballpark parser
    sys.path.append(r"c:\Users\pluck\Documents\yakyuu\final")
    from unified_ballpark_parser import run_ballpark_parser
    
    print("🔍 Checking for new ballparks...")
    run_ballpark_parser(dry_run=False)

def calculate_basic_park_factors():
    """Calculate basic park factors using league-average method"""
    
    print("\n🧮 Calculating basic park factors (league-average method)...")
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Get league average runs per team per game
    cursor.execute("""
        SELECT AVG((CAST(home_runs AS FLOAT) + CAST(visitor_runs AS FLOAT)) / 2.0) as league_avg
        FROM games 
        WHERE home_runs IS NOT NULL AND visitor_runs IS NOT NULL
        AND gametype = '公式戦'
    """)
    league_avg = cursor.fetchone()[0]
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
    
    conn.commit()
    conn.close()
    
    print(f"   ✅ Updated {len(basic_factors)} ballparks with basic park factors")

def calculate_team_adjusted_factors():
    """Calculate team-adjusted park factors (most accurate method)"""
    
    print("\n🎯 Calculating team-adjusted park factors...")
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
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
    
    # Get league average for fallback
    cursor.execute("""
        SELECT AVG((CAST(home_runs AS FLOAT) + CAST(visitor_runs AS FLOAT)) / 2.0) as league_avg
        FROM games 
        WHERE home_runs IS NOT NULL AND visitor_runs IS NOT NULL
        AND gametype = '公式戦'
    """)
    league_avg = cursor.fetchone()[0]
    
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
            total_actual = sum(row[0] for row in games_data)
            total_expected = sum(row[1] for row in games_data)
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
    
    conn.commit()
    conn.close()
    
    print(f"   ✅ Updated {team_adj_count} ballparks with team-adjusted park factors")

def generate_park_factors_report():
    """Generate a comprehensive park factors report"""
    
    print("\n📊 Generating park factors report...")
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Get summary statistics
    cursor.execute("""
        SELECT 
            COUNT(*) as total_parks,
            COUNT(CASE WHEN games_sample_size >= 50 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN games_sample_size >= 20 AND games_sample_size < 50 THEN 1 END) as med_confidence,
            COUNT(CASE WHEN games_sample_size < 20 THEN 1 END) as low_confidence,
            AVG(games_sample_size) as avg_games,
            SUM(games_sample_size) as total_games,
            AVG(pf_runs_team_adj) as avg_pf,
            MAX(pf_runs_team_adj) as max_pf,
            MIN(pf_runs_team_adj) as min_pf
        FROM ballparks
        WHERE games_sample_size IS NOT NULL
    """)
    
    stats = cursor.fetchone()
    total_parks, high_conf, med_conf, low_conf, avg_games, total_games, avg_pf, max_pf, min_pf = stats
    
    # Get top/bottom parks
    cursor.execute("""
        SELECT park_name, pf_runs_team_adj, games_sample_size, home_team
        FROM ballparks 
        WHERE pf_runs_team_adj IS NOT NULL
        AND games_sample_size >= 20
        ORDER BY pf_runs_team_adj DESC
        LIMIT 10
    """)
    top_hitter_parks = cursor.fetchall()
    
    cursor.execute("""
        SELECT park_name, pf_runs_team_adj, games_sample_size, home_team
        FROM ballparks 
        WHERE pf_runs_team_adj IS NOT NULL
        AND games_sample_size >= 20
        ORDER BY pf_runs_team_adj ASC
        LIMIT 10
    """)
    top_pitcher_parks = cursor.fetchall()
    
    # Generate report
    report = f"""# Park Factors Report - {timestamp}

## 📊 Summary Statistics
- **Total Parks**: {total_parks}
- **Total Games Analyzed**: {total_games:,}
- **Average Games per Park**: {avg_games:.1f}
- **Average Park Factor**: {avg_pf:.3f}
- **Park Factor Range**: {min_pf:.3f} - {max_pf:.3f}

### Confidence Levels:
- **High Confidence (50+ games)**: {high_conf} parks
- **Medium Confidence (20-49 games)**: {med_conf} parks  
- **Low Confidence (<20 games)**: {low_conf} parks

## 🏟️ Most Hitter-Friendly Parks (20+ games)
| Rank | Park | Team-Adj PF | Games | Home Team |
|------|------|-------------|-------|-----------|"""
    
    for i, (park, pf, games, team) in enumerate(top_hitter_parks, 1):
        team_str = team[:20] if team else "Unknown"
        report += f"\n| {i} | {park[:20]} | {pf:.3f} | {games} | {team_str} |"
    
    report += f"""

## 🛡️ Most Pitcher-Friendly Parks (20+ games)
| Rank | Park | Team-Adj PF | Games | Home Team |
|------|------|-------------|-------|-----------|"""
    
    for i, (park, pf, games, team) in enumerate(top_pitcher_parks, 1):
        team_str = team[:20] if team else "Unknown"
        report += f"\n| {i} | {park[:20]} | {pf:.3f} | {games} | {team_str} |"
    
    report += f"""

## 🔧 Usage in Player Stats
```python
# Recommended: Use team-adjusted park factors
park_adjustment = (2 - ballpark.pf_runs_team_adj)
adjusted_wrc_plus = base_wrc_plus * park_adjustment
```

## 📝 Notes
- Park factors calculated using team-adjusted method
- Sample-size weighting applied (50+ games = full confidence)
- Factors regressed toward 1.0 for small sample sizes
- Use `pf_runs_team_adj` for most accurate results

---
*Generated by refresh_park_factors.py*
"""
    
    # Write report to file
    report_path = r"c:\Users\pluck\Documents\yakyuu\park_factors_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    
    print(f"   ✅ Report saved to: {report_path}")
    
    conn.close()

def refresh_all_park_factors():
    """Complete refresh of park factors after new data is parsed"""
    
    print("🏟️ PARK FACTORS - COMPLETE REFRESH")
    print("=" * 60)
    print("This will update all park factors with the latest game data.")
    print("Run this after parsing new games or updating the database.\n")
    
    try:
        # Step 1: Update ballparks table with any new parks
        print("Step 1: Checking for new ballparks...")
        populate_new_ballparks()
        
        # Step 2: Calculate basic park factors
        print("\nStep 2: Calculating basic park factors...")
        calculate_basic_park_factors()
        
        # Step 3: Calculate team-adjusted park factors (most accurate)
        print("\nStep 3: Calculating team-adjusted park factors...")
        calculate_team_adjusted_factors()
        
        # Step 4: Generate summary report
        print("\nStep 4: Generating summary report...")
        generate_park_factors_report()
        
        print("\n✅ Park factors refresh complete!")
        print("📊 Check 'park_factors_report.md' for detailed analysis")
        print("🚀 Your player stats now use the most accurate park adjustments!")
        
    except Exception as e:
        print(f"\n❌ Error during park factors refresh: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point"""
    import sys
    
    auto_mode = '--auto' in sys.argv or '-a' in sys.argv
    
    if not auto_mode:
        print("🏟️ PARK FACTORS REFRESH")
        print("=" * 40)
        print("This will recalculate all park factors with current data.")
        
        response = input("\nProceed with refresh? (y/n): ").lower().strip()
        
        if response not in ['y', 'yes']:
            print("❌ Refresh cancelled")
            return
    
    refresh_all_park_factors()

if __name__ == "__main__":
    main()