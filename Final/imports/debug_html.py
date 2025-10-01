import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; yakyuu.jp-scraper/1.0; +https://yakyuu.jp/; contact: lukas@yakyuu.jp; free service, not for resale)"
}

def debug_player_page(player_id):
    url = f"https://npb.jp/bis/players/{player_id}.html"
    
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    print(f"=== DEBUGGING PLAYER {player_id} ===")
    
    # Check all li elements with IDs
    print("\nAll li elements with IDs:")
    for li in soup.find_all('li', id=True):
        print(f"  {li.get('id')}: {li.get_text(strip=True)}")
    
    # Check bio section
    bio_section = soup.find('section', id='pc_bio')
    if bio_section:
        print("\nBio section found!")
        print("Bio section rows:")
        rows = bio_section.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th and td:
                print(f"  {th.get_text(strip=True)}: {td.get_text(strip=True)}")
    else:
        print("\nBio section NOT found!")
    
    # Look for any element containing position info
    print("\nSearching for position-related text:")
    for element in soup.find_all(['td', 'th', 'span', 'div']):
        text = element.get_text(strip=True)
        if any(pos in text for pos in ['投手', '捕手', '一塁', '二塁', '三塁', '遊撃', '外野', '内野']):
            print(f"  Found: {text}")
    
    # Look for name reading elements
    print("\nSearching for name reading elements:")
    for element in soup.find_all(['li', 'span', 'div']):
        text = element.get_text(strip=True)
        if any(char in text for char in ['あ', 'い', 'う', 'え', 'お', 'か', 'き', 'く', 'け', 'こ']):
            print(f"  Possible reading: {text}")

if __name__ == "__main__":
    debug_player_page("81985118") 