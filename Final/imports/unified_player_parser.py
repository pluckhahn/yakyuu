import sys
import os
import sqlite3
from typing import List

# Import our existing modules
from player_extractor import PlayerExtractor
from player_parser_db import PlayerParserDB

class UnifiedPlayerParser:
    def __init__(self, db_path: str = r"C:\Users\pluck\Documents\yakyuu\yakyuu.db"):
        """Initialize the unified player parser"""
        self.db_path = db_path
        self.extractor = PlayerExtractor(db_path)
        self.parser = PlayerParserDB(db_path)
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def run_extractor(self) -> int:
        """Run player extractor and return number of new players found"""
        print("=== STEP 1: RUNNING PLAYER EXTRACTOR ===")
        print("Finding unique players from batting and pitching tables...")
        
        # Run the extractor
        new_players_count = self.extractor.extract_and_insert_players()
        
        print(f"✅ Player extractor completed: {new_players_count} new players added")
        return new_players_count
    
    def run_parser(self, limit: int = None):
        """Run player parser on newly added players"""
        print("\n=== STEP 2: RUNNING PLAYER PARSER ===")
        print("Parsing player data from NPB website...")
        
        # Get players that need data (should be the ones just added by extractor)
        players_needing_data = self.parser.get_players_needing_data()
        
        if limit:
            players_needing_data = players_needing_data[:limit]
        
        print(f"Found {len(players_needing_data)} players needing data")
        
        if not players_needing_data:
            print("No players need data!")
            return
        
        # Run the parser
        self.parser.populate_all_players(limit=limit)
    
    def run_full_process(self, parse_limit: int = None):
        """Run the complete unified player parsing process"""
        print("🚀 STARTING UNIFIED PLAYER PARSER")
        print("=" * 50)
        
        # Step 1: Extract new players
        new_players_count = self.run_extractor()
        
        if new_players_count == 0:
            print("\n✅ No new players found. All players already in database.")
            return
        
        # Step 2: Parse player data
        self.run_parser(limit=parse_limit)
        
        print("\n" + "=" * 50)
        print("🎉 UNIFIED PLAYER PARSER COMPLETED")
        
        # Show final summary
        self.show_database_summary()
    
    def show_database_summary(self):
        """Show summary of players table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Total players
            cursor.execute("SELECT COUNT(*) FROM players")
            total_players = cursor.fetchone()[0]
            
            # Players with complete data
            cursor.execute("""
                SELECT COUNT(*) FROM players 
                WHERE name IS NOT NULL AND name != ''
            """)
            complete_players = cursor.fetchone()[0]
            
            # Players needing data
            cursor.execute("""
                SELECT COUNT(*) FROM players 
                WHERE name IS NULL OR name = ''
            """)
            incomplete_players = cursor.fetchone()[0]
            
            print(f"\n📊 DATABASE SUMMARY:")
            print(f"Total players: {total_players}")
            print(f"Complete profiles: {complete_players}")
            print(f"Needing data: {incomplete_players}")
            
        finally:
            conn.close()

def main():
    """Main function for unified player parsing"""
    parser = UnifiedPlayerParser()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--extract-only':
            # Only run the extractor
            parser.run_extractor()
        elif sys.argv[1] == '--parse-only':
            # Only run the parser
            limit = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else None
            parser.run_parser(limit=limit)
        elif sys.argv[1].isdigit():
            # Run full process with parse limit
            limit = int(sys.argv[1])
            print(f"Running unified parser with parse limit of {limit} players...")
            parser.run_full_process(parse_limit=limit)
        else:
            print("Usage:")
            print("  py unified_player_parser.py                    # Run full process")
            print("  py unified_player_parser.py [number]           # Run with parse limit")
            print("  py unified_player_parser.py --extract-only     # Only extract new players")
            print("  py unified_player_parser.py --parse-only [num] # Only parse existing players")
    else:
        # Run full process
        parser.run_full_process()

if __name__ == "__main__":
    main() 