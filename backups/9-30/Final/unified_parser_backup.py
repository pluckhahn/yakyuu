#!/usr/bin/env python3
"""
Unified Parser for Japanese Baseball Data
Takes a base URL and automatically parses both box scores and play-by-play data
"""

import sys
import os
import sqlite3
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Add the imports directory to the path so we can import the existing parsers
sys.path.append(os.path.join(os.path.dirname(__file__), 'imports'))

try:
    from batting_lineup_parser import BattingLineupParser
    from pitching_parser import PitchingParser
    from eventfiles import parse_playbyplay_from_url, upsert_events
    from metadata import NPBGamesScraper
    from event_aggregator import EventAggregator
    from pitcher_event_aggregator import PitcherEventAggregator
except ImportError as e:
    print(f"Error importing parsers: {e}")
    print("Make sure batting_lineup_parser.py, pitching_parser.py, eventfiles.py, metadata.py, event_aggregator.py, and pitcher_event_aggregator.py are available")
    sys.exit(1)

def extract_game_id_from_url(url):
    """Extract game ID from URL like '2021/0409/db-t-01'"""
    # Remove trailing slash if present
    url = url.rstrip('/')
    
    # Extract the game ID part
    # URL format: https://npb.jp/scores/2021/0409/db-t-01/ or similar
    parts = url.split('/')
    if len(parts) >= 6:
        # Get the last 3 parts: year, date, game_id
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
    
    # Basic pattern validation for game ID
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
        response = requests.get(url, timeout=10)
        return response.status_code == 200
    except:
        return False

def parse_game_data(base_url, db_path='yakyuu.db'):
    """
    Parse all data for a game from a base URL
    
    Args:
        base_url: Base URL like 'https://npb.jp/scores/2021/0409/db-t-01/'
        db_path: Path to SQLite database
    """
    
    print(f"🔍 Parsing game data from: {base_url}")
    
    # Validate URL
    is_valid, game_id_or_error = validate_url(base_url)
    if not is_valid:
        print(f"❌ Invalid URL: {game_id_or_error}")
        return False
    
    game_id = game_id_or_error
    print(f"✅ Game ID: {game_id}")
    
    # Build URLs
    box_url, playbyplay_url = build_urls(base_url)
    print(f"📊 Box score URL: {box_url}")
    print(f"🎯 Play-by-play URL: {playbyplay_url}")
    
    # Check if URLs exist
    box_exists = check_url_exists(box_url)
    playbyplay_exists = check_url_exists(playbyplay_url)
    
    if not box_exists and not playbyplay_exists:
        print("❌ Neither box score nor play-by-play URLs are accessible")
        return False
    
    success_count = 0
    
    # Parse box score data (batting, pitching, games)
    if box_exists:
        print("\n📊 Parsing box score data...")
        try:
            # Parse batting lineup
            print("  - Parsing batting lineup...")
            batting_parser = BattingLineupParser(db_path)
            batting_result = batting_parser.parse_single_game(box_url)
            batting_parser.close()
            if batting_result:
                print("  ✅ Batting lineup parsed successfully")
                success_count += 1
            else:
                print("  ❌ Failed to parse batting lineup")
            
            # Parse pitching stats
            print("  - Parsing pitching stats...")
            pitching_parser = PitchingParser(db_path)
            pitching_result = pitching_parser.parse_single_game(box_url)
            pitching_parser.close()
            if pitching_result:
                print("  ✅ Pitching stats parsed successfully")
                success_count += 1
            else:
                print("  ❌ Failed to parse pitching stats")
            
            # Parse game metadata
            print("  - Parsing game metadata...")
            try:
                metadata_scraper = NPBGamesScraper(db_path)
                metadata_result = metadata_scraper.scrape_single_game(box_url)
                metadata_scraper.close()
                if metadata_result:
                    print("  ✅ Game metadata parsed successfully")
                    success_count += 1
                else:
                    print("  ❌ Failed to parse game metadata")
            except Exception as e:
                print(f"  ❌ Error parsing game metadata: {e}")
                import traceback
                traceback.print_exc()
                
        except Exception as e:
            print(f"  ❌ Error parsing box score data: {e}")
    else:
        print("⚠️  Box score URL not accessible, skipping box score parsing")
    
    # Parse play-by-play data (events)
    if playbyplay_exists:
        print("\n🎯 Parsing play-by-play data...")
        try:
            events = parse_playbyplay_from_url(playbyplay_url)
            if events:
                upsert_events(db_path, events)
                print("✅ Play-by-play data parsed successfully")
                success_count += 1
            else:
                print("❌ Failed to parse play-by-play data")
        except Exception as e:
            print(f"❌ Error parsing play-by-play data: {e}")
    else:
        print("⚠️  Play-by-play URL not accessible, skipping event parsing")
    
    # Summary
    print(f"\n📋 Summary:")
    print(f"  - Box score accessible: {'✅' if box_exists else '❌'}")
    print(f"  - Play-by-play accessible: {'✅' if playbyplay_exists else '❌'}")
    print(f"  - Successful parsing operations: {success_count}")
    
    return success_count > 0

def main():
    """Main function for command line usage"""
    if len(sys.argv) != 2:
        print("Usage: python unified_parser.py <base_url>")
        print("Example: python unified_parser.py https://npb.jp/scores/2021/0409/db-t-01/")
        sys.exit(1)
    
    base_url = sys.argv[1]
    
    # Ensure URL ends with slash
    if not base_url.endswith('/'):
        base_url += '/'
    
    success = parse_game_data(base_url, '../yakyuu.db')
    
    if success:
        print("\n🎉 Game data parsing completed successfully!")
        
        # Run aggregation automatically
        print("\n📊 Running event aggregation...")
        try:
            # Aggregate batting stats
            print("  - Aggregating batting stats...")
            batting_aggregator = EventAggregator('../yakyuu.db')
            batting_aggregator.aggregate_and_update()
            batting_aggregator.close()
            print("  ✅ Batting aggregation completed")
            
            # Aggregate pitching stats
            print("  - Aggregating pitching stats...")
            pitching_aggregator = PitcherEventAggregator('../yakyuu.db')
            pitching_aggregator.aggregate_and_update()
            pitching_aggregator.close()
            print("  ✅ Pitching aggregation completed")
            
            print("\n🎉 All processing completed successfully!")
            
        except Exception as e:
            print(f"❌ Error during aggregation: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("\n❌ Game data parsing failed")
        sys.exit(1)

if __name__ == "__main__":
    main() 