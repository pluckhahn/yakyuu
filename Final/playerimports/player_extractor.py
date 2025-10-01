import sqlite3
import sys
from typing import Set, List

class PlayerExtractor:
    def __init__(self, db_path="C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"):
        """Initialize the player extractor with database connection"""
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def get_unique_player_ids_from_batting(self) -> Set[str]:
        """Get all unique player IDs from the batting table"""
        print("Extracting unique player IDs from batting table...")
        
        query = """
        SELECT DISTINCT player_id 
        FROM batting 
        WHERE player_id IS NOT NULL AND player_id != ''
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        player_ids = {row[0] for row in results}
        print(f"Found {len(player_ids)} unique player IDs in batting table")
        return player_ids
    
    def get_unique_player_ids_from_pitching(self) -> Set[str]:
        """Get all unique player IDs from the pitching table"""
        print("Extracting unique player IDs from pitching table...")
        
        query = """
        SELECT DISTINCT player_id 
        FROM pitching 
        WHERE player_id IS NOT NULL AND player_id != ''
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        player_ids = {row[0] for row in results}
        print(f"Found {len(player_ids)} unique player IDs in pitching table")
        return player_ids
    
    def get_existing_player_ids(self) -> Set[str]:
        """Get all existing player IDs from the players table"""
        print("Getting existing player IDs from players table...")
        
        query = """
        SELECT player_id 
        FROM players 
        WHERE player_id IS NOT NULL AND player_id != ''
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        player_ids = {row[0] for row in results}
        print(f"Found {len(player_ids)} existing player IDs in players table")
        return player_ids
    
    def insert_new_players(self, new_player_ids: Set[str]) -> int:
        """Insert new players into the players table with minimal data"""
        if not new_player_ids:
            print("No new players to insert")
            return 0
        
        print(f"Inserting {len(new_player_ids)} new players into players table...")
        
        # Prepare insert statement - only insert player_id for now
        insert_query = """
        INSERT INTO players (player_id)
        VALUES (?)
        """
        
        inserted_count = 0
        for player_id in new_player_ids:
            try:
                # Insert with just player_id
                self.cursor.execute(insert_query, (player_id,))
                inserted_count += 1
            except sqlite3.IntegrityError as e:
                # Player might already exist (though we filtered for this)
                print(f"Warning: Could not insert player {player_id}: {e}")
            except Exception as e:
                print(f"Error inserting player {player_id}: {e}")
        
        self.conn.commit()
        print(f"Successfully inserted {inserted_count} new players")
        return inserted_count
    
    def extract_and_insert_players(self) -> dict:
        """Main method to extract unique players and insert new ones"""
        print("=== PLAYER EXTRACTION PROCESS ===")
        
        # Get all unique player IDs from batting and pitching tables
        batting_player_ids = self.get_unique_player_ids_from_batting()
        pitching_player_ids = self.get_unique_player_ids_from_pitching()
        
        # Combine all unique player IDs
        all_player_ids = batting_player_ids.union(pitching_player_ids)
        print(f"Total unique player IDs from both tables: {len(all_player_ids)}")
        
        # Get existing player IDs
        existing_player_ids = self.get_existing_player_ids()
        
        # Find new player IDs
        new_player_ids = all_player_ids - existing_player_ids
        print(f"New player IDs to insert: {len(new_player_ids)}")
        
        # Insert new players
        inserted_count = self.insert_new_players(new_player_ids)
        
        # Summary
        summary = {
            'total_unique_players': len(all_player_ids),
            'batting_players': len(batting_player_ids),
            'pitching_players': len(pitching_player_ids),
            'existing_players': len(existing_player_ids),
            'new_players_inserted': inserted_count
        }
        
        print("\n=== SUMMARY ===")
        print(f"Total unique players found: {summary['total_unique_players']}")
        print(f"  - From batting table: {summary['batting_players']}")
        print(f"  - From pitching table: {summary['pitching_players']}")
        print(f"  - Already in players table: {summary['existing_players']}")
        print(f"  - New players inserted: {summary['new_players_inserted']}")
        
        return summary
    
    def show_sample_new_players(self, limit: int = 10):
        """Show a sample of newly inserted players"""
        print(f"\n=== SAMPLE OF PLAYERS IN TABLE (showing up to {limit}) ===")
        
        query = """
        SELECT player_id
        FROM players 
        ORDER BY player_id
        LIMIT ?
        """
        
        self.cursor.execute(query, (limit,))
        results = self.cursor.fetchall()
        
        if not results:
            print("No players found in table")
            return
        
        print("player_id")
        print("-" * 20)
        for row in results:
            player_id = row[0]
            print(f"{player_id}")
    
    def close(self):
        """Close database connection"""
        self.conn.close()

def main():
    """Main function for command line usage"""
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = "C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"
    
    print(f"Using database: {db_path}")
    
    extractor = PlayerExtractor(db_path)
    
    try:
        # Extract and insert players
        summary = extractor.extract_and_insert_players()
        
        # Show sample of new players
        extractor.show_sample_new_players(10)
        
        print(f"\n✅ Player extraction completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during player extraction: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        extractor.close()

if __name__ == "__main__":
    main() 