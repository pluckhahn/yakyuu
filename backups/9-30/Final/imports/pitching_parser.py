import requests
from bs4 import BeautifulSoup
import sqlite3
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

class PitchingParser:
    def __init__(self, db_path="C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"):
        """Initialize parser with database connection"""
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def parse_game_id(self, url):
        """Extract game_id from URL"""
        # Extract date and teams from URL like: https://npb.jp/scores/2021/0409/db-t-01/box.html
        # or https://npb.jp/scores/2025/0710/c-t-15/box.html
        # or https://npb.jp/scores/2025/0717/c-db-14/box.html
        match = re.search(r'/(\d{4}/\d{4}/[a-z]+-[a-z]+-\d{2})/box\.html', url)
        if match:
            return match.group(1)
        return None
    
    def parse_teams_from_game_id(self, game_id):
        """Extract team codes from game_id"""
        # game_id format: 2021/0409/db-t-01 or 2025/0710/c-t-15 or 2025/0717/c-db-14
        # Extract team codes (db-t-01 -> db, t) or (c-t-15 -> c, t) or (c-db-14 -> c, db)
        match = re.search(r'([a-z]+)-([a-z]+)-\d{2}', game_id)
        if match:
            return match.group(1), match.group(2)  # home, away
        return None, None
    
    def extract_player_id_from_link(self, link):
        """Extract player_id from player link"""
        if hasattr(link, 'get') and link.get('href'):
            href = link.get('href')
            # Extract player_id from href like: /bis/players/71075138.html
            match = re.search(r'/bis/players/(\d+)\.html', href)
            if match:
                return match.group(1)
        return None
    
    def parse_pitching_stats(self, box_url):
        """Parse pitching statistics from box score page"""
        try:
            print(f"Parsing pitching stats for {box_url}")
            
            # Get game_id and teams
            game_id = self.parse_game_id(box_url)
            if not game_id:
                print("Could not parse game_id from URL")
                return []
            
            home_team, away_team = self.parse_teams_from_game_id(game_id)
            print(f"Home team: {home_team}, Away team: {away_team}")
            
            # Fetch the page
            response = requests.get(box_url, headers=HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables on the page")
            
            pitching_data = []
            pitching_table_count = 0
            
            for table_idx, table in enumerate(tables):
                if not hasattr(table, 'find_all'):
                    continue
                
                # Check if this is a pitching table by looking for pitching headers
                rows = table.find_all('tr')
                has_pitching_headers = False
                
                for row in rows:
                    if hasattr(row, 'find_all'):
                        header_cells = row.find_all(['th', 'td'])
                        header_text = ' '.join([cell.get_text(strip=True) for cell in header_cells])
                        if '投手' in header_text and '投球数' in header_text and '投球回' in header_text:
                            has_pitching_headers = True
                            break
                
                print(f"Table {table_idx}: {hasattr(table, 'get_text') and table.get_text(strip=True)[:50]}...")
                print(f"  Has pitching headers: {has_pitching_headers}")
                
                if has_pitching_headers:
                    pitching_table_count += 1
                    
                    # Determine team based on table order
                    if pitching_table_count == 1:
                        team_id = away_team  # First pitching table = visitor
                        print(f"Found first pitching table (visitor): {team_id}")
                    else:
                        team_id = home_team  # Second pitching table = home
                        print(f"Found second pitching table (home): {team_id}")
                    
                    # Parse rows in the table
                    rows = table.find_all('tr')
                    
                    # Find the header row to determine column positions
                    header_row = None
                    for row in rows:
                        if hasattr(row, 'find_all'):
                            header_cells = row.find_all(['th', 'td'])
                            header_text = ' '.join([cell.get_text(strip=True) for cell in header_cells])
                            if '投手' in header_text and '投球数' in header_text and '投球回' in header_text:
                                header_row = header_cells
                                break
                    
                    if not header_row:
                        print(f"Could not find header row for pitching table")
                        continue
                    
                    # Find column indices
                    player_col = None
                    pitches_col = None
                    batters_col = None
                    ip_col = None
                    hits_col = None
                    hr_col = None
                    bb_col = None
                    hbp_col = None
                    k_col = None
                    wp_col = None
                    balk_col = None
                    runs_col = None
                    er_col = None
                    
                    for i, cell in enumerate(header_row):
                        cell_text = cell.get_text(strip=True)
                        if '投手' in cell_text:
                            player_col = i
                        elif '投球数' in cell_text:
                            pitches_col = i
                        elif '打者' in cell_text:
                            batters_col = i
                        elif '投球回' in cell_text:
                            ip_col = i
                        elif '安打' in cell_text:
                            hits_col = i
                        elif '本塁打' in cell_text:
                            hr_col = i
                        elif '四球' in cell_text:
                            bb_col = i
                        elif '死球' in cell_text:
                            hbp_col = i
                        elif '三振' in cell_text:
                            k_col = i
                        elif '暴投' in cell_text:
                            wp_col = i + 2  # Data rows have 2 extra columns
                        elif 'ボーク' in cell_text:
                            balk_col = i + 2  # Data rows have 2 extra columns
                        elif '失点' in cell_text:
                            runs_col = i + 2  # Data rows have 2 extra columns
                        elif '自責点' in cell_text:
                            er_col = i + 2    # Data rows have 2 extra columns
                    
                    print(f"Column positions - Player: {player_col}, Pitches: {pitches_col}, IP: {ip_col}, Runs: {runs_col}, ER: {er_col}")
                    print(f"                    WP: {wp_col}, HBP: {hbp_col}, Balk: {balk_col}")
                    
                    # Track pitchers for start/finish detection
                    pitchers_in_table = []
                    
                    for row_idx, row in enumerate(rows):
                        cells = row.find_all(['th', 'td']) if hasattr(row, 'find_all') else []
                        
                        if len(cells) >= 3:  # Need at least player, some stats
                            # Debug: print the full row structure
                            print(f"    Full row data: {[cell.get_text(strip=True) for cell in cells]}")
                            print(f"    Row length: {len(cells)}")
                            
                            try:
                                # Extract player info
                                player_cell = cells[player_col] if player_col is not None and player_col < len(cells) else None
                                player_id = None
                                
                                if player_cell and hasattr(player_cell, 'find'):
                                    player_link = player_cell.find('a', href=True)
                                    if player_link:
                                        player_id = self.extract_player_id_from_link(player_link)
                                
                                if not player_id:
                                    continue
                                
                                # Check for win/loss/save/hold indicators
                                cell_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                                win = 1 if '○' in cell_text else 0
                                loss = 1 if '●' in cell_text else 0
                                save = 1 if 'S' in cell_text else 0
                                hold = 1 if 'H' in cell_text else 0
                                
                                # Track this pitcher for start/finish detection
                                pitchers_in_table.append(player_id)
                                
                                # Extract pitching stats
                                pitches_thrown = 0
                                if pitches_col is not None and pitches_col < len(cells):
                                    pitches_cell = cells[pitches_col]
                                    pitches_text = pitches_cell.get_text(strip=True)
                                    if pitches_text.isdigit():
                                        pitches_thrown = int(pitches_text)
                                
                                batters_faced = 0
                                if batters_col is not None and batters_col < len(cells):
                                    batters_cell = cells[batters_col]
                                    batters_text = batters_cell.get_text(strip=True)
                                    if batters_text.isdigit():
                                        batters_faced = int(batters_text)
                                
                                # Parse innings pitched (e.g., "6", "0.2", "1.1") - convert to decimal for season calculations
                                ip = 0.0
                                if ip_col is not None and ip_col < len(cells):
                                    ip_cell = cells[ip_col]
                                    ip_text = ip_cell.get_text(strip=True)
                                    if ip_text:
                                        # Handle formats like "6", "0.2", "1.1", "0 +"
                                        ip_text = ip_text.replace('+', '').strip()
                                        if ip_text:
                                            try:
                                                # Convert fractional innings to decimal (e.g., 5.2 = 5.67, 0.1 = 0.33)
                                                if '.' in ip_text:
                                                    parts = ip_text.split('.')
                                                    whole = int(parts[0])
                                                    fraction = int(parts[1])
                                                    # Convert fraction to decimal: 1 = 0.33, 2 = 0.67
                                                    decimal_fraction = fraction / 3.0
                                                    ip = whole + decimal_fraction
                                                else:
                                                    ip = float(ip_text)
                                            except ValueError:
                                                # Handle cases like "0 +" or other formats
                                                ip = 0.0
                                
                                runs = 0
                                if runs_col is not None and runs_col < len(cells):
                                    runs_cell = cells[runs_col]
                                    runs_text = runs_cell.get_text(strip=True)
                                    print(f"    Runs cell text: '{runs_text}' (column {runs_col})")
                                    if runs_text.isdigit():
                                        runs = int(runs_text)
                                
                                er = 0
                                if er_col is not None and er_col < len(cells):
                                    er_cell = cells[er_col]
                                    er_text = er_cell.get_text(strip=True)
                                    print(f"    ER cell text: '{er_text}' (column {er_col})")
                                    if er_text.isdigit():
                                        er = int(er_text)
                                
                                # Extract other stats
                                wild_pitch = 0
                                if wp_col is not None and wp_col < len(cells):
                                    wp_cell = cells[wp_col]
                                    wp_text = wp_cell.get_text(strip=True)
                                    print(f"    WP cell text: '{wp_text}' (column {wp_col})")
                                    if wp_text.isdigit():
                                        wild_pitch = int(wp_text)
                                
                                balk = 0
                                if balk_col is not None and balk_col < len(cells):
                                    balk_cell = cells[balk_col]
                                    balk_text = balk_cell.get_text(strip=True)
                                    if balk_text.isdigit():
                                        balk = int(balk_text)
                                
                                # Determine start/finish flags
                                start = 1 if len(pitchers_in_table) == 1 else 0  # First pitcher in table
                                finish = 0  # Will be set after we process all pitchers
                                
                                # Create pitching record
                                pitching_record = {
                                    'game_id': game_id,
                                    'player_id': player_id,
                                    'team': team_id,
                                    'win': win,
                                    'loss': loss,
                                    'save': save,
                                    'hold': hold,
                                    'start': start,
                                    'finish': finish,  # Will be updated later
                                    'ip': ip,
                                    'pitches_thrown': pitches_thrown,
                                    'er': er,
                                    'r': runs,
                                    'batters_faced': batters_faced,
                                    'wild_pitch': wild_pitch,
                                    'balk': balk
                                }
                                
                                pitching_data.append(pitching_record)
                                print(f"  {player_id}: {ip} IP, {pitches_thrown} pitches, {runs}R/{er}ER, W:{win} L:{loss} S:{save} H:{hold}")
                                
                            except Exception as e:
                                print(f"Error parsing row {row_idx}: {e}")
                                continue
                    
                    # Set finish flag for the last pitcher in this table
                    if pitching_data and len(pitchers_in_table) > 0:
                        # Find the last pitcher record for this team in this table
                        team_pitchers = [p for p in pitching_data if p['team'] == team_id]
                        if team_pitchers:
                            team_pitchers[-1]['finish'] = 1
            
            print(f"Found {len(pitching_data)} pitching records")
            return pitching_data
            
        except requests.RequestException as e:
            print(f"Error fetching {box_url}: {e}")
            return []
        except Exception as e:
            print(f"Error parsing {box_url}: {e}")
            return []
    
    def insert_pitching_data(self, pitching_data):
        """Insert pitching data into pitching table"""
        if not pitching_data:
            print("No pitching data to insert")
            return
        
        try:
            # First, delete existing pitching records for this game
            game_id = pitching_data[0]['game_id']
            self.cursor.execute("DELETE FROM pitching WHERE game_id = ?", (game_id,))
            
            # Insert new pitching records
            insert_query = """
            INSERT INTO pitching (
                game_id, player_id, team, win, loss, save, hold, start, finish, ip, pitches_thrown, er, r, batters_faced, 
                wild_pitch, balk
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for record in pitching_data:
                values = (
                    record['game_id'], record['player_id'], record['team'],
                    record['win'], record['loss'], record['save'], record['hold'],
                    record['start'], record['finish'],
                    record['ip'], record['pitches_thrown'], record['er'], record['r'],
                    record['batters_faced'], record['wild_pitch'], record['balk']
                )
                self.cursor.execute(insert_query, values)
            
            self.conn.commit()
            print(f"Inserted {len(pitching_data)} pitching records for game {game_id}")
            
        except sqlite3.Error as e:
            print(f"Database error inserting pitching data for game {game_id}: {e}")
    
    def parse_single_game(self, box_url):
        """Parse and insert pitching data for a single game"""
        pitching_data = self.parse_pitching_stats(box_url)
        if pitching_data:
            self.insert_pitching_data(pitching_data)
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
    
    parser = PitchingParser()
    
    if len(sys.argv) > 1:
        # Use URL from command line argument
        url = sys.argv[1]
        print(f"Parsing pitching data for: {url}")
        parser.parse_single_game(url)
    else:
        # Default example URL
        example_url = "https://npb.jp/scores/2025/0710/c-t-15/box.html"
        print(f"Using default URL: {example_url}")
        parser.parse_single_game(example_url)
    
    # Close connection
    parser.close() 