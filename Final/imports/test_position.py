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

# Test the function
test_positions = [
    "Outfielder",
    "Pitcher", 
    "Catcher",
    "First Baseman",
    "Second Baseman",
    "Third Baseman",
    "Shortstop",
    "Left Fielder",
    "Center Fielder",
    "Right Fielder",
    "Designated Hitter",
    "Some Unknown Position"
]

print("Testing position conversion:")
for pos in test_positions:
    converted = convert_position_to_abbreviation(pos)
    print(f"'{pos}' -> '{converted}'") 