import sqlite3
import sys
from typing import Dict, List, Tuple

class PitcherEventAggregator:
    def __init__(self, db_path="C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"):
        """Initialize the pitcher event aggregator with database connection"""
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def aggregate_events_by_game_and_pitcher(self, game_ids: List[str] = None) -> Dict[Tuple[str, str], Dict]:
        """
        Aggregate event data by game_id and pitcher_player_id
        Args:
            game_ids: Optional list of game IDs to aggregate. If None, aggregates all games.
        Returns: {(game_id, pitcher_id): aggregated_stats}
        """
        if game_ids:
            print(f"Aggregating events by game and pitcher for {len(game_ids)} specific games...")
            placeholders = ','.join(['?'] * len(game_ids))
            query = f"""
            SELECT 
                game_id,
                pitcher_player_id,
                COUNT(*) as pa_faced,  -- Every event is a plate appearance faced
                SUM("1b") as p_1b,  -- Binary flags for hit types allowed
                SUM("2b") as p_2b,
                SUM("3b") as p_3b,
                SUM(hr) as p_hr,
                SUM(gb) as p_gb,
                SUM(fb) as p_fb,
                SUM(k) as p_k,
                SUM(roe) as p_roe,
                SUM(bb) as p_bb,
                SUM(hbp) as p_hbp,
                SUM(gdp) as p_gdp,
                SUM(sac) as p_sac
            FROM event 
            WHERE game_id IN ({placeholders})
            GROUP BY game_id, pitcher_player_id
            ORDER BY game_id, pitcher_player_id
            """
            self.cursor.execute(query, game_ids)
        else:
            print("Aggregating events by game and pitcher for ALL games...")
            query = """
            SELECT 
                game_id,
                pitcher_player_id,
                COUNT(*) as pa_faced,  -- Every event is a plate appearance faced
                SUM("1b") as p_1b,  -- Binary flags for hit types allowed
                SUM("2b") as p_2b,
                SUM("3b") as p_3b,
                SUM(hr) as p_hr,
                SUM(gb) as p_gb,
                SUM(fb) as p_fb,
                SUM(k) as p_k,
                SUM(roe) as p_roe,
                SUM(bb) as p_bb,
                SUM(hbp) as p_hbp,
                SUM(gdp) as p_gdp,
                SUM(sac) as p_sac
            FROM event 
            GROUP BY game_id, pitcher_player_id
            ORDER BY game_id, pitcher_player_id
            """
            self.cursor.execute(query)
        
        results = self.cursor.fetchall()
        
        aggregated_data = {}
        for row in results:
            game_id, pitcher_id, pa_faced, p_1b, p_2b, p_3b, p_hr, p_gb, p_fb, p_k, p_roe, p_bb, p_hbp, p_gdp, p_sac = row
            
            # Calculate total hits allowed from binary flags
            p_h = (p_1b or 0) + (p_2b or 0) + (p_3b or 0) + (p_hr or 0)
            
            aggregated_data[(game_id, pitcher_id)] = {
                'pa_faced': pa_faced or 0,
                'p_h': p_h,
                'p_1b': p_1b or 0,
                'p_2b': p_2b or 0,
                'p_3b': p_3b or 0,
                'p_hr': p_hr or 0,
                'p_gb': p_gb or 0,
                'p_fb': p_fb or 0,
                'p_k': p_k or 0,
                'p_roe': p_roe or 0,
                'p_bb': p_bb or 0,
                'p_hbp': p_hbp or 0,
                'p_gdp': p_gdp or 0,
                'p_sac': p_sac or 0
            }
        
        print(f"Aggregated data for {len(aggregated_data)} game-pitcher combinations")
        return aggregated_data
    
    def get_pitching_records(self, game_ids: List[str] = None) -> Dict[Tuple[str, str], Dict]:
        """
        Get existing pitching records from the pitching table
        Args:
            game_ids: Optional list of game IDs to get records for. If None, gets all records.
        Returns: {(game_id, player_id): pitching_record}
        """
        if game_ids:
            print(f"Getting existing pitching records for {len(game_ids)} specific games...")
            placeholders = ','.join(['?'] * len(game_ids))
            query = f"""
            SELECT game_id, player_id, team, win, loss, save, hold, start, finish, ip, pitches_thrown, er, r, batters_faced, wild_pitch, balk
            FROM pitching
            WHERE game_id IN ({placeholders})
            """
            self.cursor.execute(query, game_ids)
        else:
            print("Getting existing pitching records for ALL games...")
            query = """
            SELECT game_id, player_id, team, win, loss, save, hold, start, finish, ip, pitches_thrown, er, r, batters_faced, wild_pitch, balk
            FROM pitching
            """
            self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        pitching_records = {}
        for row in results:
            game_id, player_id, team, win, loss, save, hold, start, finish, ip, pitches_thrown, er, r, batters_faced, wild_pitch, balk = row
            pitching_records[(game_id, player_id)] = {
                'team': team,
                'win': win,
                'loss': loss,
                'save': save,
                'hold': hold,
                'start': start,
                'finish': finish,
                'ip': ip,
                'pitches_thrown': pitches_thrown,
                'er': er,
                'r': r,
                'batters_faced': batters_faced,
                'wild_pitch': wild_pitch,
                'balk': balk
            }
        
        print(f"Found {len(pitching_records)} existing pitching records")
        return pitching_records
    
    def update_pitching_stats(self, aggregated_data: Dict[Tuple[str, str], Dict], pitching_records: Dict[Tuple[str, str], Dict]):
        """
        Update pitching table with aggregated event statistics
        """
        print("Updating pitching statistics...")
        
        updated_count = 0
        new_count = 0
        
        for (game_id, pitcher_id), event_stats in aggregated_data.items():
            # Check if this game-pitcher combination exists in pitching table
            if (game_id, pitcher_id) in pitching_records:
                # Update existing record
                update_query = """
                UPDATE pitching SET
                    p_h = ?, p_1b = ?, p_2b = ?, p_3b = ?, p_hr = ?,
                    p_gb = ?, p_fb = ?, p_k = ?, p_roe = ?, p_bb = ?, p_hbp = ?, p_gdp = ?, p_sac = ?
                WHERE game_id = ? AND player_id = ?
                """
                
                values = (
                    event_stats['p_h'], event_stats['p_1b'], event_stats['p_2b'], event_stats['p_3b'], 
                    event_stats['p_hr'], event_stats['p_gb'], event_stats['p_fb'], 
                    event_stats['p_k'], event_stats['p_roe'], event_stats['p_bb'], event_stats['p_hbp'], 
                    event_stats['p_gdp'], event_stats['p_sac'],
                    game_id, pitcher_id
                )
                
                self.cursor.execute(update_query, values)
                updated_count += 1
                
            else:
                # Create new record (this shouldn't happen if pitching parser worked correctly)
                print(f"Warning: No pitching record found for game {game_id}, pitcher {pitcher_id}")
                new_count += 1
        
        self.conn.commit()
        print(f"Updated {updated_count} pitching records")
        if new_count > 0:
            print(f"Warning: {new_count} game-pitcher combinations not found in pitching table")
    
    def aggregate_and_update(self, game_ids: List[str] = None):
        """
        Main method to aggregate events and update pitching statistics
        Args:
            game_ids: Optional list of game IDs to aggregate. If None, aggregates all games.
        """
        try:
            # Step 1: Aggregate event data
            aggregated_data = self.aggregate_events_by_game_and_pitcher(game_ids)
            
            # Step 2: Get existing pitching records
            pitching_records = self.get_pitching_records(game_ids)
            
            # Step 3: Update pitching statistics
            self.update_pitching_stats(aggregated_data, pitching_records)
            
            print("Pitcher event aggregation completed successfully!")
            
            # Show some sample results
            self.show_sample_results()
            
        except Exception as e:
            print(f"Error during pitcher event aggregation: {e}")
            self.conn.rollback()
    
    def show_sample_results(self):
        """Show sample aggregated results"""
        print("\nSample aggregated results:")
        
        query = """
        SELECT game_id, player_id, p_h, p_1b, p_2b, p_3b, p_hr, p_bb, p_k
        FROM pitching 
        WHERE p_h > 0 
        ORDER BY p_h DESC 
        LIMIT 10
        """
        
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        for row in results:
            game_id, player_id, p_h, p_1b, p_2b, p_3b, p_hr, p_bb, p_k = row
            print(f"  {game_id} | {player_id} | H:{p_h} 1B:{p_1b} 2B:{p_2b} 3B:{p_3b} HR:{p_hr} BB:{p_bb} K:{p_k}")
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# Usage example
if __name__ == "__main__":
    aggregator = PitcherEventAggregator()
    aggregator.aggregate_and_update()
    aggregator.close() 