#!/usr/bin/env python3
"""
Unified Ballpark Parser - Auto-discovers and adds new ballparks from games table
Updated for cleaned ballparks table structure
"""

import sqlite3
import os
from datetime import datetime

def get_database_connection():
    """Get database connection"""
    db_path = r"c:\Users\pluck\Documents\yakyuu\yakyuu.db"
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    return sqlite3.connect(db_path)

def discover_new_ballparks():
    """Find ballparks in games table that aren't in ballparks table"""
    conn = get_database_connection()
    cursor = conn.cursor()
    
    print("🔍 Discovering new ballparks from games table...")
    
    # Find ballparks in games that aren't in ballparks table
    cursor.execute("""
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
    conn.close()
    
    return new_ballparks

def get_existing_ballparks():
    """Get list of existing ballparks"""
    conn = get_database_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT park_name, pf_runs FROM ballparks ORDER BY park_name")
    existing = cursor.fetchall()
    conn.close()
    
    return existing

def calculate_default_park_factors(ballpark_name, game_count):
    """Calculate default park factors for a new ballpark"""
    
    # Default to neutral park factors
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
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO ballparks (
                park_name, park_name_en, pf_runs, games_sample_size
            ) VALUES (?, ?, ?, ?)
        """, (
            ballpark_name,
            ballpark_name,  # Use same name for English version
            factors['pf_runs'],
            game_count
        ))
        
        conn.commit()
        print(f"   ✅ Added with park factors:")
        print(f"      Runs: {factors['pf_runs']:.3f}")
        print(f"      Sample size: {game_count} games")
        
        return True
        
    except sqlite3.IntegrityError as e:
        print(f"   ❌ Error adding ballpark: {e}")
        return False
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        return False
    finally:
        conn.close()

def update_ballpark_metadata():
    """Update metadata for existing ballparks (game counts, etc.)"""
    
    print("\n📊 Updating ballpark metadata...")
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    # Get game counts for all ballparks
    cursor.execute("""
        SELECT g.ballpark, COUNT(*) as game_count
        FROM games g
        WHERE g.ballpark IS NOT NULL AND g.ballpark != ''
        GROUP BY g.ballpark
    """)
    
    ballpark_stats = cursor.fetchall()
    
    # Update each ballpark's game count
    for ballpark, game_count in ballpark_stats:
        cursor.execute("""
            UPDATE ballparks 
            SET games_sample_size = ?
            WHERE park_name = ?
        """, (
            game_count,
            ballpark
        ))
    
    conn.commit()
    conn.close()
    
    print(f"   ✅ Updated game counts for {len(ballpark_stats)} ballparks")

def show_ballpark_summary():
    """Show summary of all ballparks"""
    
    print("\n📋 BALLPARK SUMMARY")
    print("=" * 80)
    
    conn = get_database_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            bp.park_name,
            bp.pf_runs,
            COALESCE(bp.games_sample_size, 0) as games,
            bp.home_team
        FROM ballparks bp
        ORDER BY COALESCE(bp.games_sample_size, 0) DESC
    """)
    
    ballparks = cursor.fetchall()
    conn.close()
    
    print("Ballpark Name           | PF_Runs | Games | Home Team")
    print("-----------------------|---------|-------|------------------")
    
    for park_name, pf_runs, games, home_team in ballparks:
        home_team_str = home_team or "Unknown"
        pf_runs = pf_runs or 1.0
        print(f"{park_name[:22]:22} | {pf_runs:7.3f} | {games:5} | {home_team_str[:15]}")
    
    print(f"\nTotal ballparks: {len(ballparks)}")
    
    # Show park factor distribution
    pf_values = [pf for _, pf, _, _ in ballparks if pf]
    if pf_values:
        avg_pf = sum(pf_values) / len(pf_values)
        min_pf = min(pf_values)
        max_pf = max(pf_values)
        
        print(f"\nPark Factor Statistics:")
        print(f"  Average: {avg_pf:.3f}")
        print(f"  Range: {min_pf:.3f} - {max_pf:.3f}")
        
        # Categorize parks
        hitter_friendly = [pf for pf in pf_values if pf > 1.02]
        pitcher_friendly = [pf for pf in pf_values if pf < 0.98]
        neutral = [pf for pf in pf_values if 0.98 <= pf <= 1.02]
        
        print(f"  Hitter-friendly (>1.02): {len(hitter_friendly)}")
        print(f"  Neutral (0.98-1.02): {len(neutral)}")
        print(f"  Pitcher-friendly (<0.98): {len(pitcher_friendly)}")

def run_ballpark_parser(dry_run=False):
    """Main function to run the ballpark parser"""
    
    print("🏟️ UNIFIED BALLPARK PARSER")
    print("=" * 60)
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE RUN'}")
    print()
    
    try:
        # 1. Show existing ballparks
        print("📋 Current ballparks in database:")
        existing = get_existing_ballparks()
        for park_name, pf_runs in existing[:5]:  # Show first 5
            print(f"   {park_name} (PF: {pf_runs:.3f})")
        if len(existing) > 5:
            print(f"   ... and {len(existing) - 5} more")
        print(f"   Total: {len(existing)} ballparks")
        
        # 2. Discover new ballparks
        new_ballparks = discover_new_ballparks()
        
        if not new_ballparks:
            print("\n✅ No new ballparks found - all ballparks are already in database!")
        else:
            print(f"\n🆕 Found {len(new_ballparks)} new ballparks:")
            
            added_count = 0
            for ballpark_name, game_count in new_ballparks:
                print(f"\n📍 {ballpark_name} ({game_count} games)")
                
                if not dry_run:
                    if add_new_ballpark(ballpark_name, game_count):
                        added_count += 1
                else:
                    print("   🔍 Would add with default factors (dry run)")
                    calculate_default_park_factors(ballpark_name, game_count)
            
            if not dry_run:
                print(f"\n✅ Successfully added {added_count}/{len(new_ballparks)} new ballparks")
        
        # 3. Update metadata for all ballparks
        if not dry_run:
            update_ballpark_metadata()
        else:
            print("\n🔍 Would update ballpark metadata (dry run)")
        
        # 4. Show final summary
        print()
        show_ballpark_summary()
        
        print("\n🎉 BALLPARK PARSER COMPLETED!")
        print("=" * 60)
        if not dry_run:
            print("✅ All ballparks are now synchronized with games table")
            print("🏟️ New ballparks added with intelligent default park factors")
            print("📊 Metadata updated for all ballparks")
        else:
            print("🔍 Dry run completed - no changes made")
            print("💡 Run without --dry-run to apply changes")
        
    except Exception as e:
        print(f"\n❌ Error during ballpark parsing: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point"""
    import sys
    
    # Check for dry run flag
    dry_run = '--dry-run' in sys.argv or '-d' in sys.argv
    
    run_ballpark_parser(dry_run=dry_run)

if __name__ == "__main__":
    main()