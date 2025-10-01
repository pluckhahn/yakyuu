import requests
from bs4 import BeautifulSoup
import re
import time
import random
import sys
from typing import Dict, Optional
from datetime import datetime

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

class PlayerParser:
    def __init__(self):
        """Initialize the player parser"""
        self.base_url_jp = "https://npb.jp/bis/players/"
        self.base_url_en = "https://npb.jp/bis/eng/players/"
    
    def parse_player_page(self, player_id: str) -> Optional[Dict]:
        """
        Parse player information from both Japanese and English NPB pages
        Returns: Dictionary with player info or None if failed
        """
        url_jp = f"{self.base_url_jp}{player_id}.html"
        url_en = f"{self.base_url_en}{player_id}.html"
        
        print(f"Parsing player {player_id}")
        print(f"Japanese URL: {url_jp}")
        print(f"English URL: {url_en}")
        
        try:
            # Add random sleep between requests
            sleep_time = random.uniform(2, 5)
            print(f"Sleeping {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            
            # Parse Japanese page for most data
            response_jp = requests.get(url_jp, headers=HEADERS, timeout=10)
            response_jp.raise_for_status()
            response_jp.encoding = 'utf-8'
            soup_jp = BeautifulSoup(response_jp.text, 'html.parser')
            
            # Parse English page for clean romaji name
            response_en = requests.get(url_en, headers=HEADERS, timeout=10)
            response_en.raise_for_status()
            response_en.encoding = 'utf-8'
            soup_en = BeautifulSoup(response_en.text, 'html.parser')
            
            # Parse player information
            player_data = {
                'player_id': player_id,
                'name': None,
                'name_en': None,
                'bat': None,
                'throw': None,
                'height': None,
                'weight': None,
                'birthdate': None
            }
            
            # Extract player name from Japanese page
            name_element = soup_jp.find('li', id='pc_v_name')
            if name_element:
                player_data['name'] = name_element.get_text(strip=True)
            
            # Extract romaji name from English page
            # Look for the player name in the specific HTML element
            name_element_en = soup_en.find('li', id='pc_v_name')
            if name_element_en:
                player_data['name_en'] = name_element_en.get_text(strip=True)
            
            # Extract position from English page bio table
            bio_table_en = soup_en.find('section', id='pc_bio')
            if bio_table_en:
                rows = bio_table_en.find_all('tr')
                for row in rows:
                    cells = row.find_all(['th', 'td'])
                    if len(cells) >= 2:
                        header = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        if header == 'Position':
                            player_data['position'] = value
            
            # Extract bat/throw info from the Japanese bio table
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
                            if '右投' in value:
                                player_data['throw'] = 'R'
                            elif '左投' in value:
                                player_data['throw'] = 'L'
                            
                            if '右打' in value:
                                player_data['bat'] = 'R'
                            elif '左打' in value:
                                player_data['bat'] = 'L'
                        
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
    
    def print_player_data(self, player_data: Dict):
        """Print player data in a formatted way"""
        if not player_data:
            print("No data to print")
            return
        
        print("\n" + "="*60)
        print(f"PLAYER DATA FOR ID: {player_data['player_id']}")
        print("="*60)
        print(f"Name (Japanese): {player_data['name'] or 'NOT FOUND'}")
        print(f"Name (English):  {player_data['name_en'] or 'NOT FOUND'}")
        print(f"Position:       {player_data.get('position', 'NOT FOUND')}")
        print(f"Bat:            {player_data['bat'] or 'NOT FOUND'}")
        print(f"Throw:          {player_data['throw'] or 'NOT FOUND'}")
        print(f"Height:         {player_data['height'] or 'NOT FOUND'}")
        print(f"Weight:         {player_data['weight'] or 'NOT FOUND'}")
        print(f"Birthdate:      {player_data['birthdate'] or 'NOT FOUND'}")
        print("="*60)
    
    def test_single_player(self, player_id: str):
        """Test parsing a single player"""
        print(f"Testing player parser with ID: {player_id}")
        player_data = self.parse_player_page(player_id)
        self.print_player_data(player_data)
        return player_data
    
    def test_multiple_players(self, player_ids: list, limit: int = 5):
        """Test parsing multiple players"""
        print(f"Testing player parser with {min(len(player_ids), limit)} players")
        
        results = []
        for i, player_id in enumerate(player_ids[:limit]):
            print(f"\n--- Player {i+1}/{min(len(player_ids), limit)} ---")
            player_data = self.parse_player_page(player_id)
            if player_data:
                self.print_player_data(player_data)
                results.append(player_data)
            else:
                print(f"❌ Failed to parse player {player_id}")
        
        print(f"\n=== SUMMARY ===")
        print(f"Successfully parsed: {len(results)}/{min(len(player_ids), limit)} players")
        return results

def main():
    """Main function for testing"""
    parser = PlayerParser()
    
    if len(sys.argv) > 1:
        # Test with provided player ID
        player_id = sys.argv[1]
        parser.test_single_player(player_id)
    else:
        # Test with a sample player ID (you can change this)
        sample_player_id = "81985118"  # Example from your manifesto
        print("No player ID provided, testing with sample ID...")
        parser.test_single_player(sample_player_id)

if __name__ == "__main__":
    main() 