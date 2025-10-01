import sqlite3

DB_PATH = 'C:\\Users\\pluck\\Documents\\yakyuu\\yakyuu.db'

tables = [
       'event',
]

def clear_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for table in tables:
        try:
            cur.execute(f'DELETE FROM {table}')
            print(f'Cleared table: {table}')
        except Exception as e:
            print(f'Error clearing {table}: {e}')
    conn.commit()
    conn.close()
    print('All specified tables cleared.')

if __name__ == '__main__':
    clear_tables() 