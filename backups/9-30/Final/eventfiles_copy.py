import sys
import re
import requests
import sqlite3
from bs4 import BeautifulSoup
import time
import random

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

def extract_team_codes_from_url(url):
    """Extract game information from URL"""
    m = re.search(r'/scores/(\d{4})/(\d{4})/([a-z]+)-([a-z]+)-(\d{2})/', url)
    if m:
        year, mmdd, home_code, away_code, game_num = m.groups()
        return year, mmdd, home_code, away_code, game_num
    return None, None, None, None, None

def extract_player_id_from_link(link):
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

def map_inning_notation(japanese_text):
    """Map Japanese inning notation to internal format"""
    if not japanese_text:
        return None, None
    
    # Look for patterns like "1回表（巨人の攻撃）" or "1回表"
    if '回表' in japanese_text:
        inning_match = re.search(r'(\d+)回表', japanese_text)
        if inning_match:
            inning = int(inning_match.group(1))
            return f"{inning}T", "away"
    elif '回裏' in japanese_text:
        inning_match = re.search(r'(\d+)回裏', japanese_text)
        if inning_match:
            inning = int(inning_match.group(1))
            return f"{inning}B", "home"
    
    return None, None

def parse_count(count_text):
    """Parse ball/strike count from text like '1-2より' or '3-1' and return as string"""
    if not count_text or count_text.strip() == '':
        return None
    
    # Remove Japanese text like 'より' (from)
    count_text = count_text.replace('より', '').strip()
    
    parts = count_text.split('-')
    if len(parts) == 2:
        try:
            balls = int(parts[0])
            strikes = int(parts[1])
            return f"{balls}-{strikes}"
        except ValueError:
            return None
    return None

def parse_on_base(bases_text):
    """Parse on-base status from 塁上 column using a simple direct mapping"""
    if not bases_text:
        return None
    bases_text = bases_text.strip()
    # Direct mapping
    if '満塁' in bases_text:
        return '123B'
    elif '1・2塁' in bases_text:
        return '12B'
    elif '1・3塁' in bases_text:
        return '13B'
    elif '2・3塁' in bases_text:
        return '23B'
    elif '1塁' in bases_text:
        return '1B'
    elif '2塁' in bases_text:
        return '2B'
    elif '3塁' in bases_text:
        return '3B'
    return None

def parse_result(result_text):
    """Parse the result of a plate appearance with all required flags"""
    if not result_text:
        return {}
    
    result_text = result_text.strip()
    
    # Initialize result dictionary with all required fields
    result = {
        'h': 0, 'rbi': 0,
        '1b': 0, '2b': 0, '3b': 0, 'hr': 0,
        'gb': 0, 'fb': 0, 'k': 0, 'roe': 0,
        'bb': 0, 'hbp': 0, 'gdp': 0, 'sac': 0
    }
    
    # Common Japanese baseball terms
    if '三振' in result_text or 'K' in result_text or '振り逃げ' in result_text:
        result['k'] = 1
    elif '四球' in result_text or 'フォアボール' in result_text or 'BB' in result_text:
        result['bb'] = 1  # Walk
    elif '死球' in result_text or 'デッドボール' in result_text or 'HBP' in result_text:
        result['hbp'] = 1  # Hit by pitch
    elif '犠打' in result_text or 'SH' in result_text or '犠牲' in result_text:
        result['sac'] = 1  # Sacrifice bunt or general sacrifice
    elif '犠飛' in result_text or 'SF' in result_text:
        result['sac'] = 1  # Sacrifice fly
        result['rbi'] = 1
    elif '内野安打' in result_text:
        result['h'] = 1
        result['1b'] = 1
    elif '安打' in result_text or 'ヒット' in result_text:
        result['h'] = 1
        result['1b'] = 1
    elif '二塁打' in result_text or 'ツーベース' in result_text:
        result['h'] = 1
        result['2b'] = 1
    elif '三塁打' in result_text or 'スリーベース' in result_text:
        result['h'] = 1
        result['3b'] = 1
    elif '本塁打' in result_text or 'ホームラン' in result_text:
        result['h'] = 1
        result['hr'] = 1
        result['rbi'] = 1
    elif '併殺' in result_text or '併殺打' in result_text or 'DP' in result_text:
        result['gdp'] = 1  # Ground into double play
    elif 'ゴロ' in result_text:
        result['gb'] = 1
    elif 'フライ' in result_text:
        result['fb'] = 1
    
    # Check for errors independently (can combine with other flags)
    if 'エラー' in result_text:
        result['roe'] = 1
    elif '野選' in result_text:
        result['roe'] = 1
    
    # Check for RBI notation (打点)
    rbi_match = re.search(r'打点(\d+)', result_text)
    if rbi_match:
        result['rbi'] = int(rbi_match.group(1))
    
    return result

def parse_event_row(cells, game_id, current_pitcher_id, current_inning, current_team_code):
    """Parse a single event row from the table"""
    if len(cells) < 4:
        return None
    
    # Extract data from cells
    out_text = cells[0].get_text(strip=True) if len(cells) > 0 else ""
    bases_text = cells[1].get_text(strip=True) if len(cells) > 1 else ""
    batter_cell = cells[2] if len(cells) > 2 else None
    count_text = cells[3].get_text(strip=True) if len(cells) > 3 else ""
    result_text = cells[4].get_text(strip=True) if len(cells) > 4 else ""

    # Skip row if result_text contains '途中終了' or '途中交代'
    if '途中終了' in result_text or '途中交代' in result_text:
        return None

    # Extract batter ID
    batter_id = None
    if batter_cell:
        batter_link = batter_cell.find('a', href=True)
        if batter_link:
            batter_id = extract_player_id_from_link(batter_link)
    
    if not batter_id:
        return None
    
    # Parse out count
    out_count = 0
    if out_text:
        out_match = re.search(r'(\d+)アウト', out_text)
        if out_match:
            out_count = int(out_match.group(1))
    
    # Parse on-base status
    on_base = parse_on_base(bases_text)
    
    # Parse count
    count = parse_count(count_text)
    
    # Parse result
    result = parse_result(result_text)
    
    # Create event dictionary with all required fields
    event = {
        'game_id': game_id,
        'batter_player_id': batter_id,
        'pitcher_player_id': current_pitcher_id,
        'inning': current_inning,
        'team': current_team_code,
        'out': out_count,
        'on_base': on_base,
        'count': count,
        'h': result.get('h', 0),
        'rbi': result.get('rbi', 0),
        '1b': result.get('1b', 0),
        '2b': result.get('2b', 0),
        '3b': result.get('3b', 0),
        'hr': result.get('hr', 0),
        'gb': result.get('gb', 0),
        'fb': result.get('fb', 0),
        'k': result.get('k', 0),
        'roe': result.get('roe', 0),
        'bb': result.get('bb', 0),
        'hbp': result.get('hbp', 0),
        'gdp': result.get('gdp', 0),
        'sac': result.get('sac', 0)
    }
    
    return event

def parse_event_table(table, game_id, current_pitcher_id, current_inning, current_team_code):
    """Parse an event table and extract all events"""
    events = []
    rows = table.find_all('tr')
    
    for row in rows:
        cells = row.find_all(['th', 'td'])
        if len(cells) >= 3:  # Need at least out, bases, batter columns
            event = parse_event_row(cells, game_id, current_pitcher_id, current_inning, current_team_code)
            if event:
                events.append(event)
    
    return events

def classify_table(table):
    """Classify a table as EVENT, INNING, PITCHING, or UNKNOWN"""
    table_text = table.get_text(strip=True)
    
    # Check for inning indicators (e.g., "1回表（巨人の攻撃）")
    if '回表' in table_text or '回裏' in table_text:
        return 'INNING', table_text
    
    # Check for pitching indicators (e.g., "（先発投手） 中村祐")
    if '投手' in table_text:
        return 'PITCHING', table_text
    
    # Check if this table has event data (look for batter links)
    batter_links = table.find_all('a', href=True)
    if batter_links:
        # Additional check: make sure it's not just a pitcher announcement
        if not ('投手' in table_text and len(batter_links) == 1):
            return 'EVENT', table_text
    
    # Check for other potential inning indicators
    if '攻撃' in table_text and ('回' in table_text):
        return 'INNING', table_text
    
    # Check for inning indicators that might be embedded in other content
    if re.search(r'\d+回[表裏]', table_text):
        return 'INNING', table_text
    
    return 'UNKNOWN', table_text

def parse_pitching_announcement(table):
    """Parse pitcher announcement from table"""
    table_text = table.get_text(strip=True)

    # Look for starting pitcher announcements
    if '先発投手' in table_text:
        pitcher_links = table.find_all('a', href=True)
        for link in pitcher_links:
            if '/player' in link.get('href', ''):
                pitcher_id = extract_player_id_from_link(link)
                if pitcher_id:
                    return pitcher_id

    # Look for pitching change announcements
    if '投手交代' in table_text:
        pitcher_links = table.find_all('a', href=True)
        arrow_pos = table_text.find('→')
        for link in pitcher_links:
            link_text = link.get_text(strip=True)
            link_pos = table_text.find(link_text)
            if link_pos > arrow_pos:
                pitcher_id = extract_player_id_from_link(link)
                if pitcher_id:
                    return pitcher_id
        return None

    return None

def parse_playbyplay_from_url(url):
    """Parse play-by-play data from URL with proper header-based inning chronology"""
    response = requests.get(url, headers=HEADERS)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract game ID from URL
    year, mmdd, home_code, away_code, game_num = extract_team_codes_from_url(url)
    game_id = f"{year}/{mmdd}/{home_code}-{away_code}-{game_num}" if year else None
    
    if not game_id:
        print(f"Could not extract game ID from URL: {url}")
        return []
    
    print(f"Parsing play-by-play data from: {url}")
    print(f"Game ID: {game_id}")
    
    all_events = []
    
    # Find all headers and tables
    all_headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
    all_tables = soup.find_all('table')
    
    print(f"Found {len(all_headers)} headers and {len(all_tables)} tables")
    
    # Step 1: Collect all inning indicators from headers in chronological order
    print("\n=== COLLECTING INNING INDICATORS FROM HEADERS ===")
    inning_sequence = []
    for i, header in enumerate(all_headers):
        header_text = header.get_text(strip=True)
        inning, team = map_inning_notation(header_text)
        if inning:
            inning_sequence.append((i, inning, team))
            print(f"Header {i}: {inning} ({team})")
    
    # Step 2: Create chronological mapping of tables to innings based on header sequence
    print(f"\n=== MAPPING TABLES TO INNINGS BASED ON HEADER SEQUENCE ===")
    
    # Get all elements (headers and tables) in document order
    all_elements = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'table'])
    
    # Track current inning based on header sequence
    current_inning = None
    current_team = None
    inning_index = 0
    
    # Track pitchers separately for top and bottom innings
    top_inning_pitcher = None    # Pitcher for away team (top of inning)
    bottom_inning_pitcher = None # Pitcher for home team (bottom of inning)
    
    # Determine which team is batting based on inning
    def get_batting_team_code(inning, away_code, home_code):
        if inning and 'T' in inning:  # Top of inning = away team batting
            return away_code
        elif inning and 'B' in inning:  # Bottom of inning = home team batting
            return home_code
        return None
    
    print(f"\n=== PARSING ELEMENTS IN CHRONOLOGICAL ORDER ===")
    
    for i, element in enumerate(all_elements):
        element_name = getattr(element, 'name', 'unknown')
        element_text = element.get_text(strip=True)
        
        print(f"Element {i} ({element_name}): {element_text[:50]}...")
        
        if element_name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            # This is a header - check if it's an inning indicator
            inning, team = map_inning_notation(element_text)
            if inning:
                current_inning = inning
                current_team = team
                inning_index += 1
                print(f"  -> Updated inning: {current_inning} ({current_team})")
                
        elif element_name == 'table':
            # This is a table - classify and parse it
            table_type, _ = classify_table(element)
            
            print(f"  -> Table type: {table_type}, Current inning: {current_inning} ({current_team})")
            
            if table_type == 'PITCHING':
                new_pitcher_id = parse_pitching_announcement(element)
                if new_pitcher_id:
                    # Assign pitcher based on current inning context
                    if current_inning and 'T' in current_inning:  # Top of inning
                        top_inning_pitcher = new_pitcher_id
                        print(f"  -> New top inning pitcher: {new_pitcher_id}")
                    elif current_inning and 'B' in current_inning:  # Bottom of inning
                        bottom_inning_pitcher = new_pitcher_id
                        print(f"  -> New bottom inning pitcher: {new_pitcher_id}")
                    else:
                        print(f"  -> New pitcher (no inning context): {new_pitcher_id}")
                else:
                    print(f"  -> No pitcher ID found")
                
                # Pitching tables also contain the first event of the inning
                # Determine which pitcher to use for this event
                current_pitcher_id = None
                if current_inning and 'T' in current_inning:
                    current_pitcher_id = top_inning_pitcher
                elif current_inning and 'B' in current_inning:
                    current_pitcher_id = bottom_inning_pitcher
                
                # Parse the event part of the pitching table
                current_team_code = get_batting_team_code(current_inning, away_code, home_code)
                events = parse_event_table(element, game_id, current_pitcher_id, current_inning, current_team_code)
                if events:
                    all_events.extend(events)
                    for event in events:
                        print(f"  -> Event from pitching table: batter {event['batter_player_id']} (pitcher: {current_pitcher_id}, inning: {current_inning}, team: {current_team_code})")
                else:
                    print(f"  -> No event data in pitching table")
                    
            elif table_type == 'EVENT':
                # Determine which pitcher to use based on current inning
                current_pitcher_id = None
                if current_inning and 'T' in current_inning:
                    current_pitcher_id = top_inning_pitcher
                elif current_inning and 'B' in current_inning:
                    current_pitcher_id = bottom_inning_pitcher
                
                current_team_code = get_batting_team_code(current_inning, away_code, home_code)
                events = parse_event_table(element, game_id, current_pitcher_id, current_inning, current_team_code)
                if events:
                    all_events.extend(events)
                    for event in events:
                        print(f"  -> Event: batter {event['batter_player_id']} (pitcher: {current_pitcher_id}, inning: {current_inning}, team: {current_team_code})")
                else:
                    print(f"  -> No parseable data")
                    
            else:
                # Skip unknown tables
                print(f"  -> Skipping unknown table type")
                continue
    
    return all_events

def upsert_events(db_path, events):
    """Insert or update events in the database with new columns"""
    if not events:
        print("No events to insert")
        return
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # First, delete existing events for this game
    game_id = events[0]['game_id']
    cur.execute("DELETE FROM event WHERE game_id = ?", (game_id,))
    
    # Insert new events with all columns
    for event in events:
        # Include all database fields including new ones
        db_event = {k: v for k, v in event.items() 
                   if k in ['game_id', 'batter_player_id', 'pitcher_player_id', 'inning', 'team',
                           'out', 'on_base', 'count', 'h', 'rbi',
                           '1b', '2b', '3b', 'hr', 'gb', 'fb', 'k', 'roe',
                           'bb', 'hbp', 'gdp', 'sac']}
        
        # Quote column names that start with numbers
        quoted_columns = []
        for col in db_event.keys():
            if col in ['1b', '2b', '3b']:
                quoted_columns.append(f'"{col}"')
            else:
                quoted_columns.append(col)
        
        columns = ', '.join(quoted_columns)
        placeholders = ', '.join(['?'] * len(db_event))
        sql = f"INSERT INTO event ({columns}) VALUES ({placeholders})"
        
        cur.execute(sql, list(db_event.values()))
    
    conn.commit()
    conn.close()
    print(f"Inserted {len(events)} events for game {game_id}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: py eventfiles_copy.py <playbyplay_url> [<playbyplay_url2> ...] or py eventfiles_copy.py urls.txt')
        sys.exit(1)

    urls = []
    arg1 = sys.argv[1]
    if arg1.endswith('.txt'):
        # Batch mode: read URLs from file
        with open(arg1, encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip()]
    else:
        # Multiple URLs passed as arguments
        urls = sys.argv[1:]

    # Track results for summary
    successful_games = 0
    failed_games = []
    low_event_games = []
    
    for idx, url in enumerate(urls, 1):
        # Only show progress every 50 games or for problems
        if idx % 50 == 0 or idx == 1:
            print(f"[{idx}/{len(urls)}] Processing games...")
        
        events = parse_playbyplay_from_url(url)

        if events:
            if len(events) < 40:
                # Flag games with suspiciously few events
                game_id = events[0]['game_id'] if events else url.split('/')[-2]
                low_event_games.append((url, game_id, len(events)))
                print(f"⚠️  [{idx}/{len(urls)}] {game_id}: Only {len(events)} events found (< 40)")
            
            db_path = r"C:\Users\pluck\Documents\yakyuu\yakyuu.db"
            try:
                upsert_events(db_path, events)
                successful_games += 1
            except Exception as e:
                game_id = events[0]['game_id'] if events else url.split('/')[-2]
                failed_games.append((url, game_id, str(e)))
                print(f"❌ [{idx}/{len(urls)}] {game_id}: Database error - {e}")
        else:
            # No events found - this is a problem
            game_id = url.split('/')[-2]  # Extract game ID from URL
            failed_games.append((url, game_id, "No events found"))
            print(f"❌ [{idx}/{len(urls)}] {game_id}: No events found")
        
        # Sleep 1-2 seconds between parses
        if idx < len(urls):
            sleep_time = random.uniform(1, 2)
            print(f"💤 Sleeping for {sleep_time:.2f} seconds to be polite...")
            time.sleep(sleep_time)
    
    # Print final summary
    print("\n" + "="*80)
    print("BATCH PROCESSING SUMMARY")
    print("="*80)
    print(f"✅ Successfully processed: {successful_games}/{len(urls)} games")
    
    if failed_games:
        print(f"\n❌ Failed games ({len(failed_games)}):")
        for url, game_id, error in failed_games:
            print(f"   {game_id}: {error}")
    
    if low_event_games:
        print(f"\n⚠️  Games with < 40 events ({len(low_event_games)}):")
        for url, game_id, event_count in low_event_games:
            print(f"   {game_id}: {event_count} events")
    
    if not failed_games and not low_event_games:
        print("🎉 All games processed successfully with adequate event counts!")
    
    print("="*80) 