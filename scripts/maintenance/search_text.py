import sqlite3
import glob

dbs = glob.glob("output/databases/*/*.db")
print(f"Checking {len(dbs)} databases for the keywords...")

keywords = ["华胜天成", "恒为科技", "软通动力", "中国长城"]

found_count = 0
for db in dbs:
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='topics'")
        if not cursor.fetchone():
            continue
            
        cursor.execute("SELECT topic_id, type, group_id FROM topics")
        topics = cursor.fetchall()
        
        for topic_id, t_type, group_id in topics:
            text = None
            try:
                if t_type == 'talk':
                    cursor.execute("SELECT text FROM talks WHERE topic_id = ?", (topic_id,))
                    row = cursor.fetchone()
                    if row: text = row[0]
                elif t_type == 'q&a':
                    cursor.execute("SELECT text FROM answers WHERE topic_id = ?", (topic_id,))
                    row = cursor.fetchone()
                    if row: text = row[0]
                else:
                    cursor.execute("SELECT text FROM articles WHERE topic_id = ?", (topic_id,))
                    row = cursor.fetchone()
                    if row: text = row[0]
            except sqlite3.OperationalError:
                pass
                
            if text:
                matches = [k for k in keywords if k in text]
                if len(matches) > 0:
                    print(f"[{db}] Found topic {topic_id} (type {t_type}) matching: {matches}")
                    found_count += 1
                    
    except Exception as e:
        print(f"Error in {db}: {e}")

print(f"Total found: {found_count}")
