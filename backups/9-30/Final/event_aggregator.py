import sqlite3
import sys
from typing import Dict, List, Tuple

class EventAggregator:
    def __init__(self, db_path="C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"):
        """Initialize the event aggregator with database connection"""
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def aggregate_events_by_game_and_batter(self, game_ids: List[str] = None) -> Dict[Tuple[str, str], Dict]:
        """
        Aggregate event data by game_id and batter_player_id
        Args:
            game_ids: Optional list of game IDs to aggregate. If None, aggregates all games.
        Returns: {(game_id, batter_id): aggregated_stats}
        """
        if game_ids:
            print(f"Aggregating events by game and batter for {len(game_ids)} specific games...")
            placeholders = ','.join(['?'] * len(game_ids))
            query = f"""
            SELECT 
                game_id,
                batter_player_id,
                COUNT(*) as pa,  -- Every event is a plate appearance
                SUM("1b") as b_1b,  -- Binary flags for hit types
                SUM("2b") as b_2b,
                SUM("3b") as b_3b,
                SUM(hr) as b_hr,
                SUM(rbi) as b_rbi,
                SUM(gb) as b_gb,
                SUM(fb) as b_fb,
                SUM(k) as b_k,
                SUM(roe) as b_roe,
                SUM(bb) as b_bb,
                SUM(hbp) as b_hbp,
                SUM(gdp) as b_gdp,
                SUM(sac) as b_sac
            FROM event 
            WHERE game_id IN ({placeholders})
            GROUP BY game_id, batter_player_id
            ORDER BY game_id, batter_player_id
            """
            self.cursor.execute(query, game_ids)
        else:
            print("Aggregating events by game and batter for ALL games...")
            query = """
            SELECT 
                game_id,
                batter_player_id,
                COUNT(*) as pa,  -- Every event is a plate appearance
                SUM("1b") as b_1b,  -- Binary flags for hit types
                SUM("2b") as b_2b,
                SUM("3b") as b_3b,
                SUM(hr) as b_hr,
                SUM(rbi) as b_rbi,
                SUM(gb) as b_gb,
                SUM(fb) as b_fb,
                SUM(k) as b_k,
                SUM(roe) as b_roe,
                SUM(bb) as b_bb,
                SUM(hbp) as b_hbp,
                SUM(gdp) as b_gdp,
                SUM(sac) as b_sac
            FROM event 
            GROUP BY game_id, batter_player_id
            ORDER BY game_id, batter_player_id
            """
            self.cursor.execute(query)
        
        results = self.cursor.fetchall()
        
        aggregated_data = {}
        for row in results:
            game_id, batter_id, pa, b_1b, b_2b, b_3b, b_hr, b_rbi, b_gb, b_fb, b_k, b_roe, b_bb, b_hbp, b_gdp, b_sac = row
            
            # Calculate total hits from binary flags
            b_h = (b_1b or 0) + (b_2b or 0) + (b_3b or 0) + (b_hr or 0)
            
            # Calculate AB using standard formula: AB = PA - (BB + HBP + SAC)
            ab = (pa or 0) - ((b_bb or 0) + (b_hbp or 0) + (b_sac or 0))
            
            aggregated_data[(game_id, batter_id)] = {
                'pa': pa or 0,
                'ab': ab,
                'b_h': b_h,
                'b_1b': b_1b or 0,
                'b_2b': b_2b or 0,
                'b_3b': b_3b or 0,
                'b_hr': b_hr or 0,
                'b_rbi': b_rbi or 0,
                'b_gb': b_gb or 0,
                'b_fb': b_fb or 0,
                'b_k': b_k or 0,
                'b_roe': b_roe or 0,
                'b_bb': b_bb or 0,
                'b_hbp': b_hbp or 0,
                'b_gdp': b_gdp or 0,
                'b_sac': b_sac or 0
            }
        
        print(f"Aggregated data for {len(aggregated_data)} game-batter combinations")
        return aggregated_data
    
    def get_batting_records(self, game_ids: List[str] = None) -> Dict[Tuple[str, str], Dict]:
        """
        Get existing batting records from the batting table
        Args:
            game_ids: Optional list of game IDs to get records for. If None, gets all records.
        Returns: {(game_id, player_id): batting_record}
        """
        if game_ids:
            print(f"Getting existing batting records for {len(game_ids)} specific games...")
            placeholders = ','.join(['?'] * len(game_ids))
            query = f"""
            SELECT game_id, player_id, team, lineup_position, position
            FROM batting
            WHERE game_id IN ({placeholders})
            """
            self.cursor.execute(query, game_ids)
        else:
            print("Getting existing batting records for ALL games...")
            query = """
            SELECT game_id, player_id, team, lineup_position, position
            FROM batting
            """
            self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        batting_records = {}
        for row in results:
            game_id, player_id, team, lineup_position, position = row
            batting_records[(game_id, player_id)] = {
                'team': team,
                'lineup_position': lineup_position,
                'position': position
            }
        
        print(f"Found {len(batting_records)} existing batting records")
        return batting_records
    
    def update_batting_stats(self, aggregated_data: Dict[Tuple[str, str], Dict], batting_records: Dict[Tuple[str, str], Dict]):
        """
        Update batting table with aggregated event statistics
        """
        print("Updating batting statistics...")
        
        updated_count = 0
        new_count = 0
        
        for (game_id, batter_id), event_stats in aggregated_data.items():
            # Check if this game-batter combination exists in batting table
            if (game_id, batter_id) in batting_records:
                # Update existing record
                update_query = """
                UPDATE batting SET
                    pa = ?, ab = ?, b_h = ?, b_1b = ?, b_2b = ?, b_3b = ?, b_hr = ?, b_rbi = ?,
                    b_gb = ?, b_fb = ?, b_k = ?, b_roe = ?, b_bb = ?, b_hbp = ?, b_gdp = ?, b_sac = ?
                WHERE game_id = ? AND player_id = ?
                """
                
                values = (
                    event_stats['pa'], event_stats['ab'], event_stats['b_h'], 
                    event_stats['b_1b'], event_stats['b_2b'], event_stats['b_3b'], event_stats['b_hr'], event_stats['b_rbi'],
                    event_stats['b_gb'], event_stats['b_fb'], event_stats['b_k'], event_stats['b_roe'], 
                    event_stats['b_bb'], event_stats['b_hbp'], event_stats['b_gdp'], event_stats['b_sac'],
                    game_id, batter_id
                )
                
                self.cursor.execute(update_query, values)
                updated_count += 1
                
            else:
                # Create new record (this shouldn't happen if batting lineup parser worked correctly)
                print(f"Warning: No batting record found for game {game_id}, batter {batter_id}")
                new_count += 1
        
        self.conn.commit()
        print(f"Updated {updated_count} batting records")
        if new_count > 0:
            print(f"Warning: {new_count} game-batter combinations not found in batting table")
    
    def aggregate_and_update(self, game_ids: List[str] = None):
        """
        Main method to aggregate events and update batting statistics
        Args:
            game_ids: Optional list of game IDs to aggregate. If None, aggregates all games.
        """
        try:
            # Step 1: Aggregate event data
            aggregated_data = self.aggregate_events_by_game_and_batter(game_ids)
            
            # Step 2: Get existing batting records
            batting_records = self.get_batting_records(game_ids)
            
            # Step 3: Update batting statistics
            self.update_batting_stats(aggregated_data, batting_records)
            
            print("Event aggregation completed successfully!")
            
            # Show some sample results
            self.show_sample_results()
            
        except Exception as e:
            print(f"Error during event aggregation: {e}")
            self.conn.rollback()
    
    def show_sample_results(self):
        """Show sample aggregated results"""
        print("\nSample aggregated results:")
        
        query = """
        SELECT game_id, player_id, pa, ab, b_h, b_rbi, b_hr, b_bb, b_k
        FROM batting 
        WHERE pa > 0 
        ORDER BY game_id, player_id 
        LIMIT 10
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        for row in results:
            game_id, player_id, pa, ab, b_h, b_rbi, b_hr, b_bb, b_k = row
            print(f"  {game_id} | {player_id} | PA:{pa} AB:{ab} H:{b_h} RBI:{b_rbi} HR:{b_hr} BB:{b_bb} K:{b_k}")
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# Usage example
if __name__ == "__main__":
    aggregator = EventAggregator()
    aggregator.aggregate_and_update()
    aggregator.close() 