import requests
from bs4 import BeautifulSoup
import re
import time
import random
import sys
import sqlite3
from typing import Dict, Optional, List
from datetime import datetime
import pykakasi

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

def convert_japanese_date_to_iso(japanese_date: str) -> str:
    """Convert Japanese date format (1989年11月4日) to ISO format (1989-11-04)"""
    if not japanese_date:
        return None
    
    # Match pattern like "1989年11月4日"
    match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', japanese_date)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)  # Pad with leading zero if needed
        day = match.group(3).zfill(2)    # Pad with leading zero if needed
        return f"{year}-{month}-{day}"
    
    return None

def convert_position_to_abbreviation(position: str) -> str:
    """Convert full position name to standard baseball abbreviation"""
    if not position:
        return None
    
    position_lower = position.lower().strip()
    
    # Standard position mappings
    position_map = {
        'pitcher': 'P',
        'catcher': 'C',
        'first baseman': '1B',
        'second baseman': '2B',
        'third baseman': '3B',
        'shortstop': 'SS',
        'left fielder': 'LF',
        'center fielder': 'CF',
        'right fielder': 'RF',
        'outfielder': 'OF',
        'designated hitter': 'DH',
        'utility': 'UT',
        'infielder': 'IF',
        # Handle variations
        'first base': '1B',
        'second base': '2B',
        'third base': '3B',
        'left field': 'LF',
        'center field': 'CF',
        'right field': 'RF',
        'designated hitter': 'DH'
    }
    
    return position_map.get(position_lower, position)

def convert_japanese_position_to_abbreviation(japanese_position: str) -> str:
    """Convert Japanese position name to standard baseball abbreviation"""
    if not japanese_position:
        return None
    
    # Clean the position text
    position_clean = japanese_position.strip()
    
    # Simplified position mappings - only 4 positions: P, C, IF, OF
    japanese_position_map = {
        # Pitchers
        '投手': 'P',
        'ピッチャー': 'P',
        
        # Catchers
        '捕手': 'C',
        'キャッチャー': 'C',
        
        # Infielders (all infield positions map to IF)
        '一塁手': 'IF',
        '二塁手': 'IF',
        '三塁手': 'IF',
        '遊撃手': 'IF',
        '内野手': 'IF',
        'ファースト': 'IF',
        'セカンド': 'IF',
        'サード': 'IF',
        'ショート': 'IF',
        'インフィールダー': 'IF',
        
        # Outfielders (all outfield positions map to OF)
        '左翼手': 'OF',
        '中堅手': 'OF',
        '右翼手': 'OF',
        '外野手': 'OF',
        'レフト': 'OF',
        'センター': 'OF',
        'ライト': 'OF',
        'アウトフィールダー': 'OF',
        
        # Combined positions - map to primary position
        '投手・外野手': 'P',
        '投手・内野手': 'P',
        '投手・捕手': 'P',
        '捕手・内野手': 'C',
        '捕手・外野手': 'C',
        '内野手・外野手': 'IF',
        '一塁手・外野手': 'IF',
        '二塁手・外野手': 'IF',
        '三塁手・外野手': 'IF',
        '遊撃手・外野手': 'IF',
        
        # Utility positions - default to IF
        'ユーティリティ': 'IF',
        'ユーティリティー': 'IF',
        '多面手': 'IF',
        
        # Specific combinations - map to primary position
        '投手・一塁手': 'P',
        '投手・二塁手': 'P',
        '投手・三塁手': 'P',
        '投手・遊撃手': 'P',
        '捕手・一塁手': 'C',
        '捕手・二塁手': 'C',
        '捕手・三塁手': 'C',
        '捕手・遊撃手': 'C',
        
        # Outfield combinations - map to OF
        '左翼手・中堅手': 'OF',
        '左翼手・右翼手': 'OF',
        '中堅手・右翼手': 'OF',
        
        # Infield combinations - map to IF
        '一塁手・二塁手': 'IF',
        '一塁手・三塁手': 'IF',
        '一塁手・遊撃手': 'IF',
        '二塁手・三塁手': 'IF',
        '二塁手・遊撃手': 'IF',
        '三塁手・遊撃手': 'IF'
    }
    
    # Try exact match first
    if position_clean in japanese_position_map:
        return japanese_position_map[position_clean]
    
    # Try partial matches for complex combinations
    for jp_pos, eng_pos in japanese_position_map.items():
        if jp_pos in position_clean:
            return eng_pos
    
    # If no match found, return the original text
    print(f"⚠️  Unknown position format: '{position_clean}' - returning as-is")
    return position_clean

class PlayerParserDB:
    def __init__(self, db_path: str = r"C:\Users\pluck\Documents\yakyuu\yakyuu.db"):
        """Initialize the player parser with database connection"""
        self.base_url_jp = "https://npb.jp/bis/players/"
        self.base_url_en = "https://npb.jp/bis/eng/players/"
        self.db_path = db_path
        
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def get_players_needing_data(self) -> List[str]:
        """Get list of player IDs that need data (have player_id but no player_name)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT player_id 
                FROM players 
                WHERE player_name IS NULL OR player_name = ''
                ORDER BY player_id
            """)
            
            results = cursor.fetchall()
            return [row[0] for row in results]
            
        finally:
            conn.close()
    
    def parse_player_page(self, player_id: str) -> Optional[Dict]:
        """
        Parse player information from Japanese NPB page only
        Returns: Dictionary with player info or None if failed
        """
        url_jp = f"{self.base_url_jp}{player_id}.html"
        
        print(f"Parsing player {player_id}")
        print(f"Japanese URL: {url_jp}")
        
        try:
            # Add random sleep between requests
            sleep_time = random.uniform(1, 2)
            print(f"Sleeping {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            
            # Parse Japanese page for all data
            response_jp = requests.get(url_jp, headers=HEADERS, timeout=10)
            response_jp.raise_for_status()
            response_jp.encoding = 'utf-8'
            soup_jp = BeautifulSoup(response_jp.text, 'html.parser')
            
            # Parse player information
            player_data = {
                'player_id': player_id,
                'player_name': None,
                'player_name_en': None,
                'position': None,
                'bat': None,
                'throw': None,
                'height': None,
                'weight': None,
                'birthdate': None
            }
            
            # Extract player name from Japanese page
            name_element = soup_jp.find('li', id='pc_v_name')
            if name_element:
                player_data['player_name'] = name_element.get_text(strip=True)
            
            # Extract romaji name from hiragana reading
            # Look for the reading element (usually contains hiragana/katakana)
            reading_element = soup_jp.find('li', id='pc_v_kana')
            if reading_element:
                hiragana_name = reading_element.get_text(strip=True)
                print(f"DEBUG: Found kana: {hiragana_name}")
                # Convert hiragana to romaji (basic conversion)
                player_data['player_name_en'] = self.convert_hiragana_to_romaji(hiragana_name)
                print(f"DEBUG: Converted to: {player_data['player_name_en']}")
            else:
                print(f"DEBUG: No kana element found")
            
            # Extract all data from Japanese bio table
            bio_table = soup_jp.find('section', id='pc_bio')
            if bio_table:
                rows = bio_table.find_all('tr')
                for row in rows:
                    th = row.find('th')
                    td = row.find('td')
                    if th and td:
                        header = th.get_text(strip=True)
                        value = td.get_text(strip=True)
                        
                        if header == '投打':
                            # Parse "右投左打" format and convert to English letters
                            # Handle switch hitters: "両打" or "右投両打"
                            if '右投' in value:
                                player_data['throw'] = 'R'
                            elif '左投' in value:
                                player_data['throw'] = 'L'
                            
                            if '右打' in value:
                                player_data['bat'] = 'R'
                            elif '左打' in value:
                                player_data['bat'] = 'L'
                            elif '両打' in value:
                                player_data['bat'] = 'S'  # Switch hitter
                        
                        elif header == '身長／体重':
                            # Parse "172cm／75kg" format - store just the numbers
                            height_weight_match = re.search(r'(\d+)cm／(\d+)kg', value)
                            if height_weight_match:
                                player_data['height'] = height_weight_match.group(1)
                                player_data['weight'] = height_weight_match.group(2)
                        
                        elif header == '生年月日':
                            # Convert Japanese date to ISO format
                            player_data['birthdate'] = convert_japanese_date_to_iso(value)
            
            return player_data
            
        except requests.RequestException as e:
            print(f"❌ Request error for player {player_id}: {e}")
            return None
        except Exception as e:
            print(f"❌ Error parsing player {player_id}: {e}")
            return None
    
    def convert_hiragana_to_romaji(self, hiragana_text: str) -> str:
        """
        Convert hiragana/katakana text to romaji using pykakasi library
        Handle foreign players who already have English names in parentheses
        """
        if not hiragana_text:
            return None
        
        # Check if it contains English letters AND has parentheses with English names
        if re.search(r'[a-zA-Z]', hiragana_text):
            # Look for English names in parentheses (foreign players)
            english_match = re.search(r'[\(（]([A-Za-z\s\']+)[\)）]', hiragana_text)
            if english_match:
                english_name = english_match.group(1).strip()
                # Verify this is actually English (not just katakana that looks like English)
                if re.search(r'^[A-Za-z\s\']+$', english_name):
                    # Split and format as "Last Name, First Name"
                    name_parts = english_name.split()
                    if len(name_parts) >= 2:
                        last_name = name_parts[-1]  # Last part is usually the last name
                        first_name = ' '.join(name_parts[:-1])  # Everything else is first name
                        return f"{last_name}, {first_name}"
                    else:
                        return english_name
            # If no English parentheses found, fall through to pykakasi conversion
        
        # For Japanese players with parentheses, extract the main name (before parentheses)
        # This handles cases like "銀次（赤見内　銀次）" -> extract "銀次"
        if re.search(r'[\(（].*[\)）]', hiragana_text):
            # Extract the part before parentheses
            main_name = re.sub(r'[\(（].*$', '', hiragana_text).strip()
            if main_name:
                hiragana_text = main_name
            else:
                # If no main name before parentheses, extract the content inside parentheses
                # This handles cases like "（ちぇん・ぐぁんゆう）" -> extract "ちぇん・ぐぁんゆう"
                parentheses_match = re.search(r'[\(（]([^\)）]+)[\)）]', hiragana_text)
                if parentheses_match:
                    hiragana_text = parentheses_match.group(1).strip()
        
        try:
            # Initialize pykakasi converter
            kks = pykakasi.kakasi()
            
            # Convert to romaji
            result = kks.convert(hiragana_text)
            romaji = ''.join([item['hepburn'] for item in result])
            
            # Clean up the result - replace middle dot with space for better formatting
            romaji = romaji.replace('・', ' ')
            
            # Split into name parts and capitalize
            name_parts = [word.capitalize() for word in romaji.split()]
            
            # Format as "Last Name, First Name" (baseball standard)
            if len(name_parts) >= 2:
                last_name = name_parts[0]
                first_name = ' '.join(name_parts[1:])  # Handle multi-word first names
                return f"{last_name}, {first_name}"
            else:
                # Fallback for single names
                return ' '.join(name_parts)
            
        except Exception as e:
            print(f"Warning: pykakasi conversion failed for '{hiragana_text}': {e}")
            # Fallback to original text if conversion fails
            return hiragana_text
    
    def save_player_data(self, player_data: Dict) -> bool:
        """Save player data to database"""
        if not player_data:
            return False
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Update the existing player record with all data
            update_query = """
                UPDATE players 
                SET player_name = ?, player_name_en = ?, position = ?, bat = ?, throw = ?, 
                    height = ?, weight = ?, birthdate = ?
                WHERE player_id = ?
            """
            
            cursor.execute(update_query, (
                player_data['player_name'],
                player_data['player_name_en'],
                player_data['position'],
                player_data['bat'],
                player_data['throw'],
                player_data['height'],
                player_data['weight'],
                player_data['birthdate'],
                player_data['player_id']
            ))
            
            conn.commit()
            print(f"✅ Saved complete data for player {player_data['player_id']}: {player_data['player_name']} ({player_data['position']})")
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Database error saving player {player_data['player_id']}: {e}")
            return False
        finally:
            conn.close()
    
    def print_player_data(self, player_data: Dict):
        """Print player data in a formatted way"""
        if not player_data:
            print("No data to print")
            return
        
        print("\n" + "="*60)
        print(f"PLAYER DATA FOR ID: {player_data['player_id']}")
        print("="*60)
        print(f"Name (Japanese): {player_data['player_name'] or 'NOT FOUND'}")
        print(f"Name (English):  {player_data['player_name_en'] or 'NOT FOUND'}")
        print(f"Position:       {player_data.get('position', 'NOT FOUND')}")
        print(f"Bat:            {player_data['bat'] or 'NOT FOUND'}")
        print(f"Throw:          {player_data['throw'] or 'NOT FOUND'}")
        print(f"Height:         {player_data['height'] or 'NOT FOUND'}")
        print(f"Weight:         {player_data['weight'] or 'NOT FOUND'}")
        print(f"Birthdate:      {player_data['birthdate'] or 'NOT FOUND'}")
        print("="*60)
    
    def populate_all_players(self, limit: int = None):
        """Populate all players that need data"""
        players_needing_data = self.get_players_needing_data()
        
        if limit:
            players_needing_data = players_needing_data[:limit]
        
        print(f"Found {len(players_needing_data)} players needing data")
        
        if not players_needing_data:
            print("No players need data!")
            return
        
        successful = 0
        failed = 0
        
        for i, player_id in enumerate(players_needing_data, 1):
            print(f"\n--- Processing player {i}/{len(players_needing_data)} ---")
            
            player_data = self.parse_player_page(player_id)
            if player_data:
                self.print_player_data(player_data)
                if self.save_player_data(player_data):
                    successful += 1
                else:
                    failed += 1
            else:
                print(f"❌ Failed to parse player {player_id}")
                failed += 1
        
        print(f"\n=== FINAL SUMMARY ===")
        print(f"Successfully processed: {successful}")
        print(f"Failed: {failed}")
        print(f"Total: {len(players_needing_data)}")

def main():
    """Main function for database population"""
    parser = PlayerParserDB()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--test':
            # Test mode - just parse one player without saving
            test_player_id = sys.argv[2] if len(sys.argv) > 2 else "81985118"
            print(f"Testing with player ID: {test_player_id}")
            player_data = parser.parse_player_page(test_player_id)
            parser.print_player_data(player_data)
        elif sys.argv[1].isdigit():
            # Parse specific number of players
            limit = int(sys.argv[1])
            print(f"Populating first {limit} players that need data...")
            parser.populate_all_players(limit=limit)
        else:
            print("Usage:")
            print("  py player_parser_db.py --test [player_id]  # Test parsing without saving")
            print("  py player_parser_db.py [number]            # Parse specific number of players")
            print("  py player_parser_db.py                     # Parse all players needing data")
    else:
        # Parse all players that need data
        print("Populating all players that need data...")
        parser.populate_all_players()

if __name__ == "__main__":
    main() 