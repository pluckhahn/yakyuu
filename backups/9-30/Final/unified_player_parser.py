#!/usr/bin/env python3
"""
Unified Player Parser for NPB Database
Orchestrates the complete player data extraction and parsing workflow.

This script:
1. Runs player_extractor to get all unique player IDs from batting/pitching tables
2. Runs player_parser_db to populate personal information for all players
3. Provides a unified interface for the complete player parsing process
"""

import sys
import os

# Add the playerimports directory to the path so we can import the modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'playerimports'))

from player_extractor import PlayerExtractor
from player_parser_db import PlayerParserDB

def main():
    """Main function for unified player parsing workflow"""
    print("="*60)
    print("UNIFIED PLAYER PARSER - NPB Database")
    print("="*60)
    
    # Step 1: Extract unique player IDs from batting and pitching tables
    print("\n📋 STEP 1: Extracting unique player IDs...")
    extractor = PlayerExtractor()
    extractor.extract_and_insert_players()
    
    # Step 2: Parse personal information for all players
    print("\n🔍 STEP 2: Parsing player personal information...")
    parser = PlayerParserDB()
    parser.populate_all_players()
    
    print("\n" + "="*60)
    print("✅ UNIFIED PLAYER PARSING COMPLETED!")
    print("="*60)
    print("All unique players have been extracted and their personal")
    print("information has been populated in the database.")
    print("="*60)

if __name__ == "__main__":
    main() 