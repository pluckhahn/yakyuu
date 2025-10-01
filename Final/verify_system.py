#!/usr/bin/env python3
"""
Verify the final park factor system is working correctly
"""

import sqlite3
import os

def verify_system():
    """Verify the park factor system is working correctly"""
    
    print("🔍 VERIFYING PARK FACTOR SYSTEM")
    print("=" * 60)
    
    db_path = r"c:\Users\pluck\Documents\yakyuu\yakyuu.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. Check ballparks table structure
        print("1️⃣ Checking ballparks table structure...")
        cursor.execute("PRAGMA table_info(ballparks)")
        columns = [row[1] for row in cursor.fetchall()]
        
        expected_columns = [
            'park_name', 'park_name_en', 'city', 'home_team', 'opened', 
            'capacity', 'surface', 'left_field', 'center_field', 'right_field',
            'pf_runs', 'games_sample_size', 'pf_confidence', 'pf_raw',
            'pf_runs_team_adj', 'expected_runs_per_game', 'actual_runs_per_game'
        ]
        
        print(f"   Expected columns: {len(expected_columns)}")
        print(f"   Actual columns: {len(columns)}")
        
        missing = set(expected_columns) - set(columns)
        extra = set(columns) - set(expected_columns)
        
        if missing:
            print(f"   ❌ Missing columns: {missing}")
        if extra:
            print(f"   ⚠️ Extra columns: {extra}")
        if not missing and not extra:
            print("   ✅ Table structure is correct")
        
        # 2. Check data quality
        print("\n2️⃣ Checking data quality...")
        
        cursor.execute("SELECT COUNT(*) FROM ballparks")
        total_parks = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM ballparks WHERE games_sample_size > 0")
        parks_with_games = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM ballparks WHERE pf_runs_team_adj IS NOT NULL")
        parks_with_team_adj = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM ballparks WHERE games_sample_size >= 50")
        high_confidence_parks = cursor.fetchone()[0]
        
        print(f"   Total ballparks: {total_parks}")
        print(f"   Parks with game data: {parks_with_games}")
        print(f"   Parks with team-adjusted PF: {parks_with_team_adj}")
        print(f"   High confidence parks (50+ games): {high_confidence_parks}")
        
        if parks_with_games == total_parks:
            print("   ✅ All parks have game data")
        else:
            print(f"   ⚠️ {total_parks - parks_with_games} parks missing game data")
        
        # 3. Check park factor ranges
        print("\n3️⃣ Checking park factor ranges...")
        
        cursor.execute("""
            SELECT 
                MIN(pf_runs_team_adj) as min_pf,
                MAX(pf_runs_team_adj) as max_pf,
                AVG(pf_runs_team_adj) as avg_pf,
                COUNT(*) as count_pf
            FROM ballparks 
            WHERE pf_runs_team_adj IS NOT NULL
        """)
        
        min_pf, max_pf, avg_pf, count_pf = cursor.fetchone()
        
        print(f"   Park factor range: {min_pf:.3f} - {max_pf:.3f}")
        print(f"   Average park factor: {avg_pf:.3f}")
        print(f"   Parks with team-adj PF: {count_pf}")
        
        # Check if range is reasonable
        if 0.7 <= min_pf <= 0.9 and 1.1 <= max_pf <= 1.4 and 0.98 <= avg_pf <= 1.02:
            print("   ✅ Park factor ranges are reasonable")
        else:
            print("   ⚠️ Park factor ranges may need review")
        
        # 4. Show top/bottom parks
        print("\n4️⃣ Top extreme parks...")
        
        cursor.execute("""
            SELECT park_name, pf_runs_team_adj, games_sample_size
            FROM ballparks 
            WHERE pf_runs_team_adj IS NOT NULL AND games_sample_size >= 20
            ORDER BY pf_runs_team_adj DESC
            LIMIT 3
        """)
        
        print("   Most hitter-friendly:")
        for park, pf, games in cursor.fetchall():
            print(f"     {park}: {pf:.3f} ({games} games)")
        
        cursor.execute("""
            SELECT park_name, pf_runs_team_adj, games_sample_size
            FROM ballparks 
            WHERE pf_runs_team_adj IS NOT NULL AND games_sample_size >= 20
            ORDER BY pf_runs_team_adj ASC
            LIMIT 3
        """)
        
        print("   Most pitcher-friendly:")
        for park, pf, games in cursor.fetchall():
            print(f"     {park}: {pf:.3f} ({games} games)")
        
        # 5. Check file organization
        print("\n5️⃣ Checking file organization...")
        
        files_to_check = [
            (r"c:\Users\pluck\Documents\yakyuu\final\unified_ballpark_parser.py", "Unified Ballpark Parser"),
            (r"c:\Users\pluck\Documents\yakyuu\final\imports\refresh_park_factors.py", "Park Factor Refresh"),
            (r"c:\Users\pluck\Documents\yakyuu\final\README.md", "Documentation"),
            (r"c:\Users\pluck\Documents\yakyuu\park_factors_report.md", "Latest Report")
        ]
        
        for file_path, description in files_to_check:
            if os.path.exists(file_path):
                print(f"   ✅ {description}: Found")
            else:
                print(f"   ❌ {description}: Missing")
        
        print("\n" + "=" * 60)
        print("🎉 SYSTEM VERIFICATION COMPLETED!")
        print("=" * 60)
        
        # Overall status
        issues = 0
        if missing or extra:
            issues += 1
        if parks_with_games != total_parks:
            issues += 1
        if not (0.7 <= min_pf <= 0.9 and 1.1 <= max_pf <= 1.4 and 0.98 <= avg_pf <= 1.02):
            issues += 1
        
        if issues == 0:
            print("🟢 STATUS: EXCELLENT - System is production ready!")
            print("✅ All components working correctly")
            print("✅ Data quality is high")
            print("✅ Park factors are reasonable")
            print("🚀 Ready for integration with player stats!")
        elif issues <= 2:
            print("🟡 STATUS: GOOD - Minor issues detected")
            print("⚠️ System is functional but may need minor adjustments")
        else:
            print("🔴 STATUS: NEEDS ATTENTION - Multiple issues detected")
            print("❌ System needs review before production use")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    verify_system()