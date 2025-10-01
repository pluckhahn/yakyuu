import sqlite3
import sys

def add_position_column():
    """Add position column to players table"""
    db_path = r"C:\Users\pluck\Documents\yakyuu\yakyuu.db"
    
    try:
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if position column already exists
        cursor.execute("PRAGMA table_info(players)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'position' in columns:
            print("✅ Position column already exists in players table")
            return
        
        # Add position column
        print("Adding position column to players table...")
        cursor.execute("ALTER TABLE players ADD COLUMN position TEXT")
        
        # Commit changes
        conn.commit()
        print("✅ Successfully added position column to players table")
        
        # Show updated table structure
        cursor.execute("PRAGMA table_info(players)")
        columns = cursor.fetchall()
        print("\nUpdated players table structure:")
        for column in columns:
            print(f"  {column[1]} ({column[2]})")
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    add_position_column() 