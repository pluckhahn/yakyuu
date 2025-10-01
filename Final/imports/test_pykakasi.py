import pykakasi

def test_pykakasi_conversion():
    """Test pykakasi for converting Japanese names to romaji"""
    
    # Initialize the converter
    kks = pykakasi.kakasi()
    
    # Test names (including the one from your parser)
    test_names = [
        "ながおか・ひでき",  # Nagaoka Hideki
        "たなか・ゆうき",   # Tanaka Yuki
        "さとう・たつや",   # Sato Tatsuya
        "やまだ・たろう",   # Yamada Taro
        "すずき・いちろう", # Suzuki Ichiro
        "たかはし・しんじ", # Takahashi Shinji
        "いとう・けん",     # Ito Ken
        "わだ・ゆうじ",     # Wada Yuji
        "こばやし・あきら", # Kobayashi Akira
        "なかむら・たかし", # Nakamura Takashi
    ]
    
    print("Testing pykakasi conversion:")
    print("=" * 50)
    
    for name in test_names:
        # Convert to romaji
        result = kks.convert(name)
        romaji = ''.join([item['hepburn'] for item in result])
        
        print(f"Original: {name}")
        print(f"Romaji:   {romaji}")
        print("-" * 30)

if __name__ == "__main__":
    test_pykakasi_conversion() 