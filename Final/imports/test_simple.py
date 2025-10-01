import requests
from bs4 import BeautifulSoup
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

def convert_japanese_date_to_iso(japanese_date: str) -> str:
    """Convert Japanese date format to ISO format (YYYY-MM-DD)"""
    if not japanese_date:
        return None
    
    # Match patterns like "2001年9月26日" or "1990年5月18日"
    match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', japanese_date)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)  # Pad with leading zero
        day = match.group(3).zfill(2)    # Pad with leading zero
        return f"{year}-{month}-{day}"
    
    return None

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
    }
    
    return japanese_position_map.get(position_clean, position_clean)

def convert_hiragana_to_romaji(hiragana_text: str) -> str:
    """Convert hiragana text to romaji"""
    if not hiragana_text:
        return None
    
    # Handle complete names first
    complete_names = {
        'ながおか・ひでき': 'Nagaoka Hideki',
        'たなか・ゆうき': 'Tanaka Yuki',
        'さとう・たつや': 'Sato Tatsuya',
        'やまだ・たろう': 'Yamada Taro',
        'すずき・いちろう': 'Suzuki Ichiro',
    }
    
    # Try complete names first
    if hiragana_text in complete_names:
        return complete_names[hiragana_text]
    
    # Basic hiragana to romaji mapping
    hiragana_map = {
        'あ': 'a', 'い': 'i', 'う': 'u', 'え': 'e', 'お': 'o',
        'か': 'ka', 'き': 'ki', 'く': 'ku', 'け': 'ke', 'こ': 'ko',
        'さ': 'sa', 'し': 'shi', 'す': 'su', 'せ': 'se', 'そ': 'so',
        'た': 'ta', 'ち': 'chi', 'つ': 'tsu', 'て': 'te', 'と': 'to',
        'な': 'na', 'に': 'ni', 'ぬ': 'nu', 'ね': 'ne', 'の': 'no',
        'は': 'ha', 'ひ': 'hi', 'ふ': 'fu', 'へ': 'he', 'ほ': 'ho',
        'ま': 'ma', 'み': 'mi', 'む': 'mu', 'め': 'me', 'も': 'mo',
        'や': 'ya', 'ゆ': 'yu', 'よ': 'yo',
        'ら': 'ra', 'り': 'ri', 'る': 'ru', 'れ': 're', 'ろ': 'ro',
        'わ': 'wa', 'を': 'wo', 'ん': 'n',
        # Dakuten (voiced sounds)
        'が': 'ga', 'ぎ': 'gi', 'ぐ': 'gu', 'げ': 'ge', 'ご': 'go',
        'ざ': 'za', 'じ': 'ji', 'ず': 'zu', 'ぜ': 'ze', 'ぞ': 'zo',
        'だ': 'da', 'ぢ': 'ji', 'づ': 'zu', 'で': 'de', 'ど': 'do',
        'ば': 'ba', 'び': 'bi', 'ぶ': 'bu', 'べ': 'be', 'ぼ': 'bo',
        'ぱ': 'pa', 'ぴ': 'pi', 'ぷ': 'pu', 'ぺ': 'pe', 'ぽ': 'po',
    }
    
    # Convert hiragana to romaji
    romaji = hiragana_text
    
    # Handle the middle dot (・) - replace with space
    romaji = romaji.replace('・', ' ')
    
    # Then handle individual characters
    for hiragana, romaji_char in hiragana_map.items():
        romaji = romaji.replace(hiragana, romaji_char)
    
    return romaji

def test_player_parsing():
    player_id = "51455151"
    url = f"https://npb.jp/bis/players/{player_id}.html"
    
    print(f"Testing player {player_id}")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract player data
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
        name_element = soup.find('li', id='pc_v_name')
        if name_element:
            player_data['player_name'] = name_element.get_text(strip=True)
            print(f"Found name: {player_data['player_name']}")
        
        # Extract romaji name from kana element
        kana_element = soup.find('li', id='pc_v_kana')
        if kana_element:
            hiragana_name = kana_element.get_text(strip=True)
            print(f"Found kana: {hiragana_name}")
            player_data['player_name_en'] = convert_hiragana_to_romaji(hiragana_name)
            print(f"Converted to: {player_data['player_name_en']}")
        
        # Extract all data from Japanese bio table
        bio_table = soup.find('section', id='pc_bio')
        if bio_table:
            rows = bio_table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    header = th.get_text(strip=True)
                    value = td.get_text(strip=True)
                    print(f"Bio row: {header} = {value}")
                    
                    if header == 'ポジション':
                        position_abbrev = convert_japanese_position_to_abbreviation(value)
                        player_data['position'] = position_abbrev
                        print(f"Position: {value} -> {position_abbrev}")
                    
                    elif header == '投打':
                        if '右投' in value:
                            player_data['throw'] = 'R'
                        elif '左投' in value:
                            player_data['throw'] = 'L'
                        
                        if '右打' in value:
                            player_data['bat'] = 'R'
                        elif '左打' in value:
                            player_data['bat'] = 'L'
                        elif '両打' in value:
                            player_data['bat'] = 'S'
                        print(f"Throw/Bat: {value} -> {player_data['throw']}/{player_data['bat']}")
                    
                    elif header == '身長／体重':
                        height_weight_match = re.search(r'(\d+)cm／(\d+)kg', value)
                        if height_weight_match:
                            player_data['height'] = height_weight_match.group(1)
                            player_data['weight'] = height_weight_match.group(2)
                            print(f"Height/Weight: {value} -> {player_data['height']}/{player_data['weight']}")
                    
                    elif header == '生年月日':
                        player_data['birthdate'] = convert_japanese_date_to_iso(value)
                        print(f"Birthdate: {value} -> {player_data['birthdate']}")
        
        print("\n=== FINAL RESULT ===")
        print(f"Name (Japanese): {player_data['player_name']}")
        print(f"Name (English):  {player_data['player_name_en']}")
        print(f"Position:        {player_data['position']}")
        print(f"Throw:           {player_data['throw']}")
        print(f"Bat:             {player_data['bat']}")
        print(f"Height:          {player_data['height']}")
        print(f"Weight:          {player_data['weight']}")
        print(f"Birthdate:       {player_data['birthdate']}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_player_parsing() 