#!/usr/bin/env python3
"""
Fix DH Positions in Database
Updates existing batting records to convert '指' to 'DH'
"""

import sqlite3

def fix_dh_positions():
    """Update existing batting records to convert '指' to 'DH'"""
    db_path = "C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db"
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # First, let's see how many records have '指' or '(指)' as position
        cursor.execute("SELECT COUNT(*) FROM batting WHERE position = '指' OR position = '(指)'")
        count_before = cursor.fetchone()[0]
        print(f"Found {count_before} records with '指' or '(指)' position")
        
        if count_before == 0:
            print("No records to update!")
            return
        
        # Update all records with '指' or '(指)' to 'DH'
        cursor.execute("UPDATE batting SET position = 'DH' WHERE position = '指' OR position = '(指)'")
        
        # Check how many were updated
        updated_count = cursor.fetchone()[0] if cursor.fetchone() else 0
        print(f"Updated {cursor.rowcount} records from '指' to 'DH'")
        
        # Verify the update
        cursor.execute("SELECT COUNT(*) FROM batting WHERE position = '指' OR position = '(指)'")
        count_after = cursor.fetchone()[0]
        print(f"Records with '指' or '(指)' after update: {count_after}")
        
        cursor.execute("SELECT COUNT(*) FROM batting WHERE position = 'DH'")
        dh_count = cursor.fetchone()[0]
        print(f"Total records with 'DH' position: {dh_count}")
        
        # Commit the changes
        conn.commit()
        print("✅ Successfully updated DH positions in database!")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("="*50)
    print("FIXING DH POSITIONS IN DATABASE")
    print("="*50)
    fix_dh_positions()
    print("="*50) 