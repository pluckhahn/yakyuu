import sqlite3
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

class NPBGamesScraper:
    def __init__(self, db_path="yakyuu.db"):
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
    
    def scrape_game_data(self, box_url):
        """Scrape game data from box.html"""
        try:
            response = requests.get(box_url, headers=HEADERS)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract game_id from URL
            game_id = self.parse_game_id(box_url)
            if not game_id:
                print(f"Could not parse game_id from {box_url}")
                return None
            
            # Parse teams from game_id
            home_team_id, away_team_id = self.parse_teams_from_game_id(game_id)
            
            # Extract date from game_id
            date_part = game_id.split('/')[1]  # 0409
            year = int(game_id.split('/')[0])  # 2021
            month = int(date_part[:2])  # 04
            day = int(date_part[2:])   # 09
            date = f"{year}-{month:02d}-{day:02d}"
            
            # Extract game number from game_id
            game_number = None
            parts = game_id.split('/')
            if len(parts) >= 3:
                team_part = parts[2]  # db-t-01
                if '-' in team_part:
                    last_part = team_part.split('-')[-1]
                    if last_part.isdigit():
                        game_number = int(last_part)
            
            # Get all text content for parsing
            page_text = soup.get_text()
            
            # Parse start_time - format: 18:00 or 18時00分
            start_time = None
            time_patterns = [
                r'◇開始\s*(\d{1,2}):(\d{2})',  # 18:00 format
                r'◇開始\s*(\d{1,2})時(\d{2})分'  # 18時00分 format
            ]
            for pattern in time_patterns:
                time_match = re.search(pattern, page_text)
                if time_match:
                    hour = time_match.group(1)
                    minute = time_match.group(2)
                    start_time = f"{hour}:{minute}"
                    break
            
            # Parse game_duration - format: 4時間14分
            game_duration = None
            duration_match = re.search(r'◇試合時間\s*(\d+)時間(\d+)分', page_text)
            if duration_match:
                hours = duration_match.group(1)
                minutes = duration_match.group(2)
                game_duration = f"{hours}:{minutes}"
            
            # Parse attendance - always followed by 人
            attendance = None
            attendance_match = re.search(r'◇入場者\s*([\d,]+)人', page_text)
            if attendance_match:
                attendance_str = attendance_match.group(1).replace(',', '')
                attendance = int(attendance_str)
            
            # Parse ballpark - look for ballpark name in the game title area
            ballpark = None
            # Look for ballpark in the game title section (after date, before team names)
            game_title_match = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日[^】]*)\n([^】\n]*)\n【', page_text)
            if game_title_match:
                date_line = game_title_match.group(1)
                venue_line = game_title_match.group(2).strip()
                print(f"Found venue line: '{venue_line}'")
                ballpark = venue_line
            else:
                # Fallback: look for common ballpark names in the game context
                game_section = page_text[:2000]  # Look in first 2000 chars for game info
                ballpark_names = ['横　浜', '東京ドーム', '神宮', 'バンテリン', '楽天モバイル', 'ベルーナ', 'ZOZOマリン', '丸亀']
                for name in ballpark_names:
                    if name in game_section:
                        ballpark = name
                        break
            
            # Parse inning scores from table with headings "計 H E"
            visitor_innings = [None] * 12
            home_innings = [None] * 12
            visitor_runs = None
            home_runs = None
            visitor_hits = None
            home_hits = None
            visitor_errors = None
            home_errors = None
            
            # Find the score table with headings "計 H E"
            tables = soup.find_all('table')
            for table in tables:
                table_text = table.get_text()
                if '計' in table_text and 'H' in table_text and 'E' in table_text:
                    # This is the score table
                    rows = table.find_all('tr')
                    
                    # Find data rows (skip header row)
                    data_rows = []
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 12:  # Need at least 12 columns
                            # Check if this is the header row with "計", "H", "E"
                            row_text = ' '.join([cell.get_text(strip=True) for cell in cells])
                            if '計' in row_text and 'H' in row_text and 'E' in row_text:
                                continue  # Skip header row
                            
                            # Check if this row has numbers (not just headers)
                            has_numbers = False
                            for cell in cells:
                                cell_text = cell.get_text(strip=True)
                                if cell_text.endswith('x'):
                                    cell_text = cell_text[:-1]  # Remove the x
                                if cell_text.isdigit():
                                    has_numbers = True
                                    break
                            
                            if has_numbers:
                                data_rows.append(row)
                    
                    # Process data rows: first is visitor, second is home
                    for i, row in enumerate(data_rows):
                        if i >= 2:  # Only process first two rows
                            break
                            
                        cells = row.find_all(['td', 'th'])
                        
                        # Extract numbers from the row
                        numbers = []
                        for cell in cells:
                            cell_text = cell.get_text(strip=True)
                            # Handle cases like "0x" for called games
                            if cell_text.endswith('x'):
                                cell_text = cell_text[:-1]  # Remove the x
                            if cell_text.isdigit():
                                numbers.append(int(cell_text))
                        
                        if len(numbers) >= 3:  # Need at least runs, hits, errors
                            # First row is visitor (away team)
                            if i == 0:
                                # Parse innings 1-12 (or however many there are)
                                num_innings = len(numbers) - 3  # Subtract 3 for runs, hits, errors
                                for j in range(min(num_innings, 12)):
                                    visitor_innings[j] = numbers[j] if j < len(numbers) - 3 else None
                                visitor_runs = numbers[-3] if len(numbers) >= 3 else None
                                visitor_hits = numbers[-2] if len(numbers) >= 2 else None
                                visitor_errors = numbers[-1] if len(numbers) >= 1 else None
                                print(f"Found visitor: runs={visitor_runs}, hits={visitor_hits}, errors={visitor_errors}")
                            
                            # Second row is home team
                            elif i == 1:
                                num_innings = len(numbers) - 3
                                for j in range(min(num_innings, 12)):
                                    home_innings[j] = numbers[j] if j < len(numbers) - 3 else None
                                home_runs = numbers[-3] if len(numbers) >= 3 else None
                                home_hits = numbers[-2] if len(numbers) >= 2 else None
                                home_errors = numbers[-1] if len(numbers) >= 1 else None
                                print(f"Found home: runs={home_runs}, hits={home_hits}, errors={home_errors}")
                    
                    break  # Found the score table
            
            # Parse win/loss/save pitchers from pitching tables
            winning_pitcher_id = None
            losing_pitcher_id = None
            save_pitcher_id = None
            
            # Determine winning and losing teams based on runs scored
            winning_team_id = None
            losing_team_id = None
            
            if visitor_runs is not None and home_runs is not None:
                print(f"Debug: visitor_runs={visitor_runs}, home_runs={home_runs}")
                if visitor_runs > home_runs:
                    winning_team_id = away_team_id
                    losing_team_id = home_team_id
                    print(f"Debug: Visitor wins - winning_team_id={winning_team_id}, losing_team_id={losing_team_id}")
                elif home_runs > visitor_runs:
                    winning_team_id = home_team_id
                    losing_team_id = away_team_id
                    print(f"Debug: Home wins - winning_team_id={winning_team_id}, losing_team_id={losing_team_id}")
                # If runs are equal, it's a tie - both remain None (NULL)
                else:
                    winning_team_id = None
                    losing_team_id = None
                    print(f"Debug: Tie game - winning_team_id={winning_team_id}, losing_team_id={losing_team_id}")
            
            # Find pitching tables (tables with 投球数 header)
            for table in tables:
                table_text = table.get_text()
                if '投球数' in table_text:
                    # This is a pitching table
                    rows = table.find_all('tr')
                    
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            first_cell = cells[0].get_text(strip=True)
                            
                            # Check for win/loss/save symbols in first cell
                            if '○' in first_cell:  # Win
                                # Extract pitcher ID from the second cell (player cell)
                                if len(cells) >= 2:
                                    player_cell = cells[1]
                                    player_link = player_cell.find('a')
                                    if player_link and 'href' in player_link.attrs:
                                        href = player_link['href']
                                        # Extract ID from /bis/players/ID.html
                                        if '/bis/players/' in href:
                                            pitcher_id = href.split('/bis/players/')[1].split('.html')[0]
                                            winning_pitcher_id = pitcher_id
                                            print(f"Found winning pitcher: {pitcher_id}")
                            
                            elif '●' in first_cell:  # Loss
                                # Extract pitcher ID from the second cell (player cell)
                                if len(cells) >= 2:
                                    player_cell = cells[1]
                                    player_link = player_cell.find('a')
                                    if player_link and 'href' in player_link.attrs:
                                        href = player_link['href']
                                        # Extract ID from /bis/players/ID.html
                                        if '/bis/players/' in href:
                                            pitcher_id = href.split('/bis/players/')[1].split('.html')[0]
                                            losing_pitcher_id = pitcher_id
                                            print(f"Found losing pitcher: {pitcher_id}")
                            
                            elif 'S' in first_cell and first_cell.strip() == 'S':  # Save
                                # Extract pitcher ID from the second cell (player cell)
                                if len(cells) >= 2:
                                    player_cell = cells[1]
                                    player_link = player_cell.find('a')
                                    if player_link and 'href' in player_link.attrs:
                                        href = player_link['href']
                                        # Extract ID from /bis/players/ID.html
                                        if '/bis/players/' in href:
                                            pitcher_id = href.split('/bis/players/')[1].split('.html')[0]
                                            save_pitcher_id = pitcher_id
                                            print(f"Found save pitcher: {pitcher_id}")
            
            # Determine winning and losing teams based on runs scored
            winning_team_id = None
            losing_team_id = None
            
            if visitor_runs is not None and home_runs is not None:
                if visitor_runs > home_runs:
                    winning_team_id = away_team_id
                    losing_team_id = home_team_id
                elif home_runs > visitor_runs:
                    winning_team_id = home_team_id
                    losing_team_id = away_team_id
                # If runs are equal, it's a tie - both remain None (NULL)
                else:
                    winning_team_id = None
                    losing_team_id = None
            
            # Parse game type from the page
            gametype = '公式戦'  # Default to regular season
            game_title_match = re.search(r'【([^】]+)】', page_text)
            if game_title_match:
                title_content = game_title_match.group(1)
                print(f"Found game title: '{title_content}'")
                if 'クライマックスシリーズ' in title_content:
                    gametype = 'クライマックスシリーズ'
                elif 'オールスターゲーム' in title_content:
                    gametype = 'オールスターゲーム'
                elif 'ファーストステージ' in title_content:
                    gametype = 'ファーストステージ'
                elif 'ファイナルステージ' in title_content:
                    gametype = 'ファイナルステージ'
                elif '日本シリーズ' in title_content:
                    gametype = '日本シリーズ'
                elif '公式戦' in title_content:
                    gametype = '公式戦'
            
            # Create game data dictionary
            game_data = {
                'game_id': game_id,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'ballpark': ballpark,
                'date': date,
                'game_number': game_number,
                'start_time': start_time,
                'game_duration': game_duration,
                'attendance': attendance,
                'winning_pitcher_id': winning_pitcher_id,
                'losing_pitcher_id': losing_pitcher_id,
                'save_pitcher_id': save_pitcher_id,
                'home_runs': home_runs,
                'visitor_runs': visitor_runs,
                'home_hits': home_hits,
                'visitor_hits': visitor_hits,
                'home_errors': home_errors,
                'visitor_errors': visitor_errors,
                'winning_team_id': winning_team_id,
                'losing_team_id': losing_team_id,
                'gametype': gametype,
                'season': year,
                'visitor_inn1': visitor_innings[0],
                'visitor_inn2': visitor_innings[1],
                'visitor_inn3': visitor_innings[2],
                'visitor_inn4': visitor_innings[3],
                'visitor_inn5': visitor_innings[4],
                'visitor_inn6': visitor_innings[5],
                'visitor_inn7': visitor_innings[6],
                'visitor_inn8': visitor_innings[7],
                'visitor_inn9': visitor_innings[8],
                'visitor_inn10': visitor_innings[9],
                'visitor_inn11': visitor_innings[10],
                'visitor_inn12': visitor_innings[11],
                'home_inn1': home_innings[0],
                'home_inn2': home_innings[1],
                'home_inn3': home_innings[2],
                'home_inn4': home_innings[3],
                'home_inn5': home_innings[4],
                'home_inn6': home_innings[5],
                'home_inn7': home_innings[6],
                'home_inn8': home_innings[7],
                'home_inn9': home_innings[8],
                'home_inn10': home_innings[9],
                'home_inn11': home_innings[10],
                'home_inn12': home_innings[11]
            }
            
            print(f"Scraped game data for {game_id}")
            return game_data
            
        except requests.RequestException as e:
            print(f"Error fetching {box_url}: {e}")
            return None
        except Exception as e:
            print(f"Error parsing {box_url}: {e}")
            return None
    
    def insert_game_data(self, game_data):
        """Insert game data into games table"""
        try:
            insert_query = """
            INSERT OR REPLACE INTO games (
                game_id, home_team_id, away_team_id, ballpark, date, game_number,
                start_time, game_duration, attendance, winning_pitcher_id,
                losing_pitcher_id, save_pitcher_id, home_runs, visitor_runs,
                home_hits, visitor_hits, home_errors, visitor_errors,
                winning_team_id, losing_team_id, gametype, season,
                visitor_inn1, visitor_inn2, visitor_inn3, visitor_inn4,
                visitor_inn5, visitor_inn6, visitor_inn7, visitor_inn8,
                visitor_inn9, visitor_inn10, visitor_inn11, visitor_inn12,
                home_inn1, home_inn2, home_inn3, home_inn4,
                home_inn5, home_inn6, home_inn7, home_inn8,
                home_inn9, home_inn10, home_inn11, home_inn12
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            values = (
                game_data['game_id'],
                game_data['home_team_id'],
                game_data['away_team_id'],
                game_data['ballpark'],
                game_data['date'],
                game_data['game_number'],
                game_data['start_time'],
                game_data['game_duration'],
                game_data['attendance'],
                game_data['winning_pitcher_id'],
                game_data['losing_pitcher_id'],
                game_data['save_pitcher_id'],
                game_data['home_runs'],
                game_data['visitor_runs'],
                game_data['home_hits'],
                game_data['visitor_hits'],
                game_data['home_errors'],
                game_data['visitor_errors'],
                game_data['winning_team_id'],
                game_data['losing_team_id'],
                game_data['gametype'],
                game_data['season'],
                game_data['visitor_inn1'],
                game_data['visitor_inn2'],
                game_data['visitor_inn3'],
                game_data['visitor_inn4'],
                game_data['visitor_inn5'],
                game_data['visitor_inn6'],
                game_data['visitor_inn7'],
                game_data['visitor_inn8'],
                game_data['visitor_inn9'],
                game_data['visitor_inn10'],
                game_data['visitor_inn11'],
                game_data['visitor_inn12'],
                game_data['home_inn1'],
                game_data['home_inn2'],
                game_data['home_inn3'],
                game_data['home_inn4'],
                game_data['home_inn5'],
                game_data['home_inn6'],
                game_data['home_inn7'],
                game_data['home_inn8'],
                game_data['home_inn9'],
                game_data['home_inn10'],
                game_data['home_inn11'],
                game_data['home_inn12']
            )
            
            self.cursor.execute(insert_query, values)
            self.conn.commit()
            print(f"Inserted game data for {game_data['game_id']}")
            
        except sqlite3.Error as e:
            print(f"Database error inserting game {game_data['game_id']}: {e}")
    
    def scrape_single_game(self, box_url):
        """Scrape and insert data for a single game"""
        game_data = self.scrape_game_data(box_url)
        if game_data:
            self.insert_game_data(game_data)
            return True
        return False
    
    def scrape_multiple_games(self, box_urls):
        """Scrape multiple games from a list of URLs"""
        success_count = 0
        for url in box_urls:
            if self.scrape_single_game(url):
                success_count += 1
        print(f"Successfully scraped {success_count} out of {len(box_urls)} games")
        return success_count
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# Usage example
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python metadata_fixed.py <box_score_url>")
        print("Example: python metadata_fixed.py https://npb.jp/scores/2021/0409/db-t-01/box.html")
        sys.exit(1)
    
    url = sys.argv[1]
    
    # Ensure URL ends with box.html
    if not url.endswith('box.html'):
        if url.endswith('/'):
            url += 'box.html'
        else:
            url += '/box.html'
    
    print(f"Scraping metadata from: {url}")
    
    scraper = NPBGamesScraper()
    success = scraper.scrape_single_game(url)
    scraper.close()
    
    if success:
        print("✅ Metadata scraping completed successfully!")
    else:
        print("❌ Metadata scraping failed")
        sys.exit(1) 