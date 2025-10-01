import sqlite3
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

class BattingLineupParser:
    def __init__(self, db_path="C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"):
        self.db_path = db_path
        self.base_url = "https://npb.jp/scores/"
        
        # Initialize database connection
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
    
    def parse_game_id(self, url):
        """Extract game_id from URL"""
        pattern = r'/scores/(.+?)/box\.html'
        match = re.search(pattern, url)
        if match:
            return match.group(1)
        return None
    
    def parse_teams_from_game_id(self, game_id):
        """Parse home/away teams from game_id"""
        parts = game_id.split('/')
        if len(parts) >= 3:
            team_part = parts[2]  # db-t-01
            team_codes = team_part.split('-')
            if len(team_codes) >= 2:
                home_code = team_codes[0]  # db
                away_code = team_codes[1]  # t
                return home_code, away_code
        return None, None
    
    def extract_player_id_from_link(self, link):
        """Extract player ID from player link"""
        if not link:
            return None
        href = link.get('href', '')
        
        # Try different NPB player URL patterns
        patterns = [
            r'/players/(\d+)\.html',  # Standard pattern
            r'/player/(\d+)',         # Alternative pattern
            r'/players/(\d+)',        # Without .html
            r'/player/(\d+)\.html'    # Another alternative
        ]
        
        for pattern in patterns:
            m = re.search(pattern, href)
            if m:
                return m.group(1)
        
        return None
    
    def parse_batting_lineup(self, box_url):
        """Parse batting lineup data from box.html"""
        try:
            response = requests.get(box_url, headers=HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract game_id from URL
            game_id = self.parse_game_id(box_url)
            if not game_id:
                print(f"Could not parse game_id from {box_url}")
                return []
            
            # Parse teams from game_id
            home_team_id, away_team_id = self.parse_teams_from_game_id(game_id)
            
            print(f"Parsing batting lineup for game {game_id}")
            print(f"Home team: {home_team_id}, Away team: {away_team_id}")
            
            batting_data = []
            batting_table_count = 0
            
            # Find all tables in the page
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables on the page")
            
            for i, table in enumerate(tables):
                # Look for batting lineup tables
                table_text = table.get_text()
                print(f"Table {i}: {table_text[:100]}...")
                
                # Check if this table contains batting lineup data
                # Look for specific batting table headers: 守備 選手 打数 得点 安打 打点 盗塁
                has_batting_headers = all(header in table_text for header in ['守備', '選手', '打数', '得点', '安打', '打点', '盗塁'])
                print(f"  Has batting headers: {has_batting_headers}")
                
                if has_batting_headers:
                    # Determine team based on table order: first = visitor, second = home
                    if batting_table_count == 0:
                        team_id = away_team_id  # First batting table = visitor
                        print(f"Found first batting table (visitor): {team_id}")
                    elif batting_table_count == 1:
                        team_id = home_team_id  # Second batting table = home
                        print(f"Found second batting table (home): {team_id}")
                    else:
                        print(f"Found additional batting table, skipping")
                        continue
                    
                    batting_table_count += 1
                    
                    # Parse rows in the table
                    rows = table.find_all('tr') if hasattr(table, 'find_all') else []
                    
                    # Find the header row to determine column positions
                    header_row = None
                    for row in rows:
                        if hasattr(row, 'find_all'):
                            header_cells = row.find_all(['th', 'td'])
                            header_text = ' '.join([cell.get_text(strip=True) for cell in header_cells])
                            if '守備' in header_text and '選手' in header_text and '打数' in header_text:
                                header_row = header_cells
                                break
                    
                    if not header_row:
                        print(f"Could not find header row for batting table")
                        continue
                    
                    # Find column indices
                    lineup_pos_col = 0  # First column (unlabeled)
                    position_col = None
                    player_col = None
                    runs_col = None
                    
                    for i, cell in enumerate(header_row):
                        cell_text = cell.get_text(strip=True)
                        if '守備' in cell_text:
                            position_col = i
                        elif '選手' in cell_text:
                            player_col = i
                        elif '得点' in cell_text:
                            runs_col = i
                    
                    print(f"Column positions - Position: {position_col}, Player: {player_col}, Runs: {runs_col}")
                    
                    for row_idx, row in enumerate(rows):
                        cells = row.find_all(['th', 'td']) if hasattr(row, 'find_all') else []
                        
                        if len(cells) >= 3:  # Need at least lineup position, position, player
                            try:
                                # Extract lineup position (打順) - first column
                                lineup_pos_cell = cells[lineup_pos_col] if lineup_pos_col < len(cells) else None
                                lineup_pos_text = lineup_pos_cell.get_text(strip=True) if lineup_pos_cell else ""
                                
                                # Parse lineup position (1-9, or DH, or substitute indicators)
                                lineup_position = None
                                if lineup_pos_text.isdigit():
                                    lineup_position = int(lineup_pos_text)
                                elif 'DH' in lineup_pos_text or '指名' in lineup_pos_text:
                                    lineup_position = 10  # DH
                                elif lineup_pos_text == "":  # Empty first column = substitute player
                                    # For substitutes, set lineup position to NULL
                                    lineup_position = None
                                
                                # Extract position (守備)
                                position_cell = cells[position_col] if position_col is not None and position_col < len(cells) else None
                                position = None
                                
                                if position_cell:
                                    position_text = position_cell.get_text(strip=True)
                                    # Map Japanese positions to English
                                    # Note: Order matters - longer patterns must come first
                                    position_map = {
                                        '走指': 'PR',  # Pinch runner for DH (must come before individual chars)
                                        '打指': 'PH',  # Pinch hitter for DH (must come before individual chars)
                                        '投': 'P', '捕': 'C', '一': '1B', '二': '2B', '三': '3B',
                                        '遊': 'SS', '左': 'LF', '中': 'CF', '右': 'RF', 'DH': 'DH',
                                        '指名': 'DH', '指': 'DH', '打': 'PH', '走': 'PR'
                                    }
                                    
                                    for jp, eng in position_map.items():
                                        if jp in position_text:
                                            position = eng
                                            break
                                    
                                    if not position:
                                        position = position_text  # Keep original if no mapping
                                
                                # Extract player info (選手)
                                player_cell = cells[player_col] if player_col is not None and player_col < len(cells) else None
                                player_id = None
                                
                                if player_cell and hasattr(player_cell, 'find'):
                                    player_link = player_cell.find('a', href=True)
                                    if player_link:
                                        player_id = self.extract_player_id_from_link(player_link)
                                
                                if not player_id:
                                    continue
                                
                                # Extract runs (得点) - use correct column index
                                runs = 0
                                if runs_col is not None and runs_col < len(cells):
                                    runs_cell = cells[runs_col]
                                    runs_text = runs_cell.get_text(strip=True)
                                    if runs_text.isdigit():
                                        runs = int(runs_text)
                                
                                # Create batting record
                                batting_record = {
                                    'game_id': game_id,
                                    'player_id': player_id,
                                    'team': team_id,
                                    'lineup_position': lineup_position,
                                    'position': position,
                                    'pa': 0,  # Will be populated in Step 5
                                    'ab': 0,  # Will be populated in Step 5
                                    'b_h': 0, 'b_r': runs, 'b_rbi': 0,  # Set runs from box score
                                    'b_1b': 0, 'b_2b': 0, 'b_3b': 0, 'b_hr': 0,
                                    'b_gb': 0, 'b_fb': 0, 'b_k': 0, 'b_roe': 0,
                                    'b_bb': 0, 'b_hbp': 0, 'b_gdp': 0, 'b_sac': 0
                                }
                                
                                batting_data.append(batting_record)
                                print(f"  Lineup {lineup_position}: {player_id} ({position})")
                                
                            except Exception as e:
                                print(f"Error parsing row {row_idx}: {e}")
                                continue
            
            print(f"Found {len(batting_data)} batting records")
            return batting_data
            
        except requests.RequestException as e:
            print(f"Error fetching {box_url}: {e}")
            return []
        except Exception as e:
            print(f"Error parsing {box_url}: {e}")
            return []
    
    def insert_batting_data(self, batting_data):
        """Insert batting data into batting table"""
        if not batting_data:
            print("No batting data to insert")
            return
        
        try:
            # First, delete existing batting records for this game
            game_id = batting_data[0]['game_id']
            self.cursor.execute("DELETE FROM batting WHERE game_id = ?", (game_id,))
            
            # Insert new batting records
            insert_query = """
            INSERT INTO batting (
                game_id, player_id, team, lineup_position, position,
                pa, ab, b_h, b_r, b_rbi, b_1b, b_2b, b_3b, b_hr,
                b_gb, b_fb, b_k, b_roe, b_bb, b_hbp, b_gdp, b_sac
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for record in batting_data:
                values = (
                    record['game_id'], record['player_id'], record['team'],
                    record['lineup_position'], record['position'],
                    record['pa'], record['ab'], record['b_h'], record['b_r'], record['b_rbi'],
                    record['b_1b'], record['b_2b'], record['b_3b'], record['b_hr'],
                    record['b_gb'], record['b_fb'], record['b_k'], record['b_roe'],
                    record['b_bb'], record['b_hbp'], record['b_gdp'], record['b_sac']
                )
                self.cursor.execute(insert_query, values)
            
            self.conn.commit()
            print(f"Inserted {len(batting_data)} batting records for game {game_id}")
            
        except sqlite3.Error as e:
            print(f"Database error inserting batting data for game {game_id}: {e}")
    
    def parse_single_game(self, box_url):
        """Parse and insert batting lineup data for a single game"""
        batting_data = self.parse_batting_lineup(box_url)
        if batting_data:
            self.insert_batting_data(batting_data)
            return True
        return False
    
    def parse_multiple_games(self, box_urls):
        """Parse multiple games from a list of URLs"""
        success_count = 0
        for url in box_urls:
            if self.parse_single_game(url):
                success_count += 1
        print(f"Successfully parsed {success_count} out of {len(box_urls)} games")
        return success_count
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# Usage example
if __name__ == "__main__":
    import sys
    
    parser = BattingLineupParser()
    
    if len(sys.argv) > 1:
        # Use URL from command line argument
        url = sys.argv[1]
        print(f"Parsing batting lineup for: {url}")
        parser.parse_single_game(url)
    else:
        # Default example URL
        example_url = "https://npb.jp/scores/2018/0501/c-g-04/box.html"
        print(f"Using default URL: {example_url}")
        parser.parse_single_game(example_url)
    
    # Close connection
    parser.close() 