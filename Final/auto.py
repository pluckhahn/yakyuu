#!/usr/bin/env python3
"""
Auto-discover NPB games for a specific month
Saves new game URLs to games/newgames_mm.txt for unified_parser.py processing
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import sys
import subprocess
from datetime import datetime

BASE_URL = "https://npb.jp"
DB_PATH = '../yakyuu.db'

def generate_schedule_url(year_month):
    """Convert YYYYMM to NPB schedule URL"""
    if len(year_month) != 6:
        raise ValueError("year_month must be in YYYYMM format")
    
    year = year_month[:4]
    month = year_month[4:6]
    
    return f"https://npb.jp/games/{year}/schedule_{month}_detail.html"

def parse_schedule_page(schedule_url):
    """Parse a schedule page and extract game URLs"""
    print(f"Parsing schedule: {schedule_url}")
    
    try:
        response = requests.get(schedule_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        urls = []
        for row in soup.find_all('tr'):
            links = row.find_all('a', href=True)
            for link in links:
                href = link['href']
                if href.startswith('/scores/') and href.endswith('/'):
                    score_cell = link.parent
                    # Skip cancelled games
                    if '中止' in score_cell.get_text() or 'ノーゲーム' in score_cell.get_text():
                        continue
                    
                    full_url = BASE_URL + href
                    if not full_url.endswith('/'):
                        full_url += '/'
                    urls.append(full_url)
        
        # Remove duplicates
        urls = list(dict.fromkeys(urls))
        print(f"  Found {len(urls)} valid game URLs")
        return urls
        
    except Exception as e:
        print(f"  Error parsing {schedule_url}: {e}")
        return []

def get_existing_game_ids():
    """Get all game_ids currently in the database"""
    if not os.path.exists(DB_PATH):
        print("Database not found, treating all games as new")
        return set()
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT game_id FROM games")
        existing_ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        print(f"Found {len(existing_ids):,} existing games in database")
        return existing_ids
    except Exception as e:
        print(f"Error reading database: {e}")
        return set()

def extract_game_id_from_url(url):
    """Extract game_id from NPB URL"""
    # URL format: https://npb.jp/scores/2025/0328/g-s-01/
    # Database game_id format: 2025/0328/g-s-01
    try:
        parts = url.rstrip('/').split('/')
        # Get the date and game parts: ['2025', '0328', 'g-s-01']
        if len(parts) >= 3:
            year = parts[-3]
            date = parts[-2] 
            game = parts[-1]
            return f"{year}/{date}/{game}"
        return None
    except:
        return None

def filter_new_games(game_urls, existing_game_ids):
    """Filter out games that already exist in database"""
    new_urls = []
    existing_count = 0
    
    for url in game_urls:
        game_id = extract_game_id_from_url(url)
        if game_id and game_id not in existing_game_ids:
            new_urls.append(url)
        else:
            existing_count += 1
    
    print(f"Game filtering results:")
    print(f"  Total games found: {len(game_urls)}")
    print(f"  New games: {len(new_urls)}")
    print(f"  Already in database: {existing_count}")
    
    return new_urls

def save_urls_to_file(urls, year_month):
    """Save URLs to games/newgames_mm.txt file"""
    if not urls:
        print("No URLs to save")
        return None
    
    # Create games directory if it doesn't exist
    games_dir = "games"
    if not os.path.exists(games_dir):
        os.makedirs(games_dir)
    
    # Generate filename
    month = year_month[4:6]
    filename = f"games/newgames_{month}.txt"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for url in urls:
                f.write(url + '\n')
        
        print(f"✅ Saved {len(urls)} URLs to {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ Error saving URLs to file: {e}")
        return None

def auto_discover_month(year_month):
    """Main function to auto-discover games for a specific month"""
    print(f"Auto-discovering NPB games for {year_month}")
    print("=" * 50)
    
    # Generate schedule URL
    schedule_url = generate_schedule_url(year_month)
    
    # Parse schedule page for game URLs
    game_urls = parse_schedule_page(schedule_url)
    
    if not game_urls:
        print("No games found for this month")
        return None
    
    # Get existing games from database
    existing_game_ids = get_existing_game_ids()
    
    # Filter out existing games
    new_game_urls = filter_new_games(game_urls, existing_game_ids)
    
    if not new_game_urls:
        print("No new games to parse")
        return None
    
    # Save URLs to file
    output_file = save_urls_to_file(new_game_urls, year_month)
    
    print("=" * 50)
    print(f"Auto-discovery complete for {year_month}")
    
    if output_file:
        print(f"\nFound {len(new_game_urls)} new games saved to {output_file}")
        
        # Ask if user wants to run unified_parser immediately
        while True:
            response = input(f"\nRun unified_parser.py on {output_file} now? (y/n): ").lower().strip()
            if response in ['y', 'yes']:
                print(f"\n🚀 Running unified_parser.py {output_file}...")
                print("=" * 50)
                
                try:
                    # Run unified_parser.py with the generated file
                    result = subprocess.run([
                        sys.executable, 'unified_parser.py', output_file
                    ], cwd=os.path.dirname(__file__), capture_output=False)
                    
                    if result.returncode == 0:
                        print("\n" + "=" * 50)
                        print("🎉 unified_parser.py completed successfully!")
                        
                        # Ask if user wants to run parse.py --full-pipeline
                        while True:
                            pipeline_response = input(f"\nRun parse.py --full-pipeline now? (y/n): ").lower().strip()
                            if pipeline_response in ['y', 'yes']:
                                print(f"\n🔧 Running parse.py --full-pipeline...")
                                print("=" * 50)
                                
                                try:
                                    # Run parse.py --full-pipeline
                                    pipeline_result = subprocess.run([
                                        sys.executable, 'parse.py', '--full-pipeline'
                                    ], cwd=os.path.dirname(__file__), capture_output=False)
                                    
                                    if pipeline_result.returncode == 0:
                                        print("\n" + "=" * 50)
                                        print("🎉 COMPLETE! Full database update finished!")
                                        print("✅ All systems updated:")
                                        print("  - Games parsed and aggregated")
                                        print("  - Player data updated")
                                        print("  - Ballpark factors calculated")
                                        print("  - Dynamic qualifiers set")
                                        print("  - Advanced stats context updated")
                                        print("\n🚀 Database is fully up-to-date!")
                                        return output_file, True
                                    else:
                                        print(f"\n⚠️ parse.py --full-pipeline completed with exit code {pipeline_result.returncode}")
                                        print("Games were parsed successfully, but some advanced features may need attention.")
                                        return output_file, True
                                        
                                except Exception as e:
                                    print(f"\n❌ Error running parse.py --full-pipeline: {e}")
                                    print("Games were parsed successfully, but advanced features failed.")
                                    print("You can run 'py parse.py --full-pipeline' manually later.")
                                    return output_file, True
                                    
                            elif pipeline_response in ['n', 'no']:
                                print("\nNext steps:")
                                print("  1. Run: py parse.py --full-pipeline")
                                print("  2. Database will be fully updated!")
                                return output_file, True
                            else:
                                print("Please enter 'y' or 'n'")
                    else:
                        print(f"\n❌ unified_parser.py failed with exit code {result.returncode}")
                        return output_file, False
                        
                except Exception as e:
                    print(f"\n❌ Error running unified_parser.py: {e}")
                    return output_file, False
                    
            elif response in ['n', 'no']:
                print(f"\nNext steps:")
                print(f"  1. Review {output_file}")
                print(f"  2. Run: py unified_parser.py {output_file}")
                print(f"  3. Run: py parse.py --full-pipeline")
                print(f"  4. Database will be fully updated!")
                return output_file, False
            else:
                print("Please enter 'y' or 'n'")
    
    return None, False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python auto.py YYYYMM")
        print("Example: python auto.py 202503")
        print("")
        print("This will:")
        print("  1. Find all games for the specified month")
        print("  2. Filter out games already in database")
        print("  3. Save new game URLs to games/newgames_mm.txt")
        print("  4. Optionally run unified_parser.py immediately (y/n prompt)")
        print("  5. Optionally run parse.py --full-pipeline (y/n prompt)")
        print("  6. Complete full database update in one command!")
        sys.exit(1)
    
    year_month = sys.argv[1]
    
    if len(year_month) != 6 or not year_month.isdigit():
        print("Error: year_month must be in YYYYMM format (e.g., 202503)")
        sys.exit(1)
    
    try:
        output_file, parsed = auto_discover_month(year_month)
        
        if output_file:
            if parsed:
                print(f"\n🎉 SUCCESS! Complete workflow finished for {year_month}!")
            else:
                print(f"\n✅ Success! Games for {year_month} discovered and ready to parse")
        else:
            print(f"\n⚪ No new games found for {year_month}")
            
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)