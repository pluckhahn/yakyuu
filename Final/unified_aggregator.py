#!/usr/bin/env python3
"""
Simple Unified Aggregator for NPB Database
Imports and orchestrates the two existing aggregators for batch processing.
"""

import sys
import os
from typing import List

# Add the imports directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'imports'))

try:
    from event_aggregator import EventAggregator
    from pitcher_event_aggregator import PitcherEventAggregator
except ImportError as e:
    print(f"Error importing aggregators: {e}")
    print("Make sure event_aggregator.py and pitcher_event_aggregator.py are available in the imports directory")
    sys.exit(1)

def read_game_ids_from_file(file_path: str) -> List[str]:
    """Read game IDs from a text file (supports both URLs and direct game IDs)"""
    game_ids = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Check if line is a URL (contains npb.jp/scores/)
                if 'npb.jp/scores/' in line:
                    # Extract game ID from URL: https://npb.jp/scores/2018/0529/t-h-01/playbyplay.html
                    # Game ID format: 2018/0529/t-h-01
                    import re
                    match = re.search(r'/scores/(\d{4}/\d{4}/[^/]+)', line)
                    if match:
                        game_id = match.group(1)
                        game_ids.append(game_id)
                    else:
                        print(f"Warning: Could not extract game ID from URL: {line}")
                else:
                    # Assume it's already a game ID
                    game_ids.append(line)
        
        print(f"Read {len(game_ids)} game IDs from {file_path}")
        return game_ids
    except Exception as e:
        print(f"Error reading game IDs file: {e}")
        return []

def aggregate_batch(game_ids: List[str] = None, db_path: str = "C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"):
    """
    Run both aggregators on the specified game IDs or all games if None
    
    Args:
        game_ids: Optional list of game IDs to aggregate. If None, aggregates all games.
        db_path: Path to the SQLite database
    """
    try:
        print("="*60)
        print("SIMPLE UNIFIED AGGREGATION - NPB Database")
        print("="*60)
        
        if game_ids:
            print(f"Aggregating data for {len(game_ids)} specific games...")
            print(f"Game IDs: {', '.join(game_ids)}")
        else:
            print("Aggregating data for ALL games...")
        
        # Step 1: Aggregate batting stats
        print("\n📊 STEP 1: Aggregating batting statistics...")
        batting_aggregator = EventAggregator(db_path)
        batting_aggregator.aggregate_and_update(game_ids)
        batting_aggregator.close()
        print("✅ Batting aggregation completed")
        
        # Step 2: Aggregate pitching stats
        print("\n⚾ STEP 2: Aggregating pitching statistics...")
        pitching_aggregator = PitcherEventAggregator(db_path)
        pitching_aggregator.aggregate_and_update(game_ids)
        pitching_aggregator.close()
        print("✅ Pitching aggregation completed")
        
        print("\n" + "="*60)
        print("✅ UNIFIED AGGREGATION COMPLETED!")
        print("="*60)
        print("All event data has been aggregated and statistics")
        print("have been updated in the database.")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error during unified aggregation: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function for command line usage"""
    if len(sys.argv) < 2:
        print("Usage: python unified_aggregator.py <game_ids_file.txt> OR python unified_aggregator.py --all")
        print("Example: python unified_aggregator.py game_list.txt")
        print("         python unified_aggregator.py --all")
        sys.exit(1)
    
    if sys.argv[1] == "--all":
        # Aggregate all games
        print("Running aggregation for ALL games...")
        aggregate_batch()
    else:
        # Read game IDs from file
        game_ids_file = sys.argv[1]
        if not os.path.exists(game_ids_file):
            print(f"Error: File {game_ids_file} not found")
            sys.exit(1)
        
        game_ids = read_game_ids_from_file(game_ids_file)
        if game_ids:
            aggregate_batch(game_ids)
        else:
            print("No game IDs found in file")
            sys.exit(1)

if __name__ == "__main__":
    main() 