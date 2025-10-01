import requests
from bs4 import BeautifulSoup

def debug_html():
    url = "https://npb.jp/bis/players/81985118.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    print("=== HTML DEBUG ===")
    
    # Check bio section
    bio = soup.find('section', id='pc_bio')
    print(f"Bio section found: {bio is not None}")
    
    if bio:
        print("Bio section content:")
        rows = bio.find_all('tr')
        for row in rows:
            th = row.find('th')
            td = row.find('td')
            if th and td:
                print(f"  {th.get_text(strip=True)}: {td.get_text(strip=True)}")
    
    # Check name reading
    reading = soup.find('li', id='pc_v_name_kana')
    print(f"Name reading found: {reading is not None}")
    if reading:
        print(f"Reading text: {reading.get_text(strip=True)}")
    
    # Check all li elements with IDs
    li_elements = soup.find_all('li', id=True)
    print(f"Total li elements with IDs: {len(li_elements)}")
    for li in li_elements:
        print(f"  {li.get('id')}: {li.get_text(strip=True)}")

if __name__ == "__main__":
    debug_html() 