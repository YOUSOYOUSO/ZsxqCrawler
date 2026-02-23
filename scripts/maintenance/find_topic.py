import sqlite3
import glob

topic_id = 45811518848212420
dbs = glob.glob("output/databases/*/*.db")
print(f"Checking {len(dbs)} databases ...")
for db in dbs:
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        
        # Check if topics table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='topics'")
        if not cursor.fetchone():
            continue
            
        cursor.execute("SELECT group_id, type FROM topics WHERE topic_id = ?", (topic_id,))
        row = cursor.fetchone()
        if row:
            group_id, t_type = row
            print(f"Found in {db}: group_id={group_id}, type={t_type}")
            
            # Now fetch text based on type
            if t_type == 'talk':
                cursor.execute("SELECT text FROM talks WHERE topic_id = ?", (topic_id,))
            elif t_type == 'q&a':
                cursor.execute("SELECT text as text FROM answers WHERE topic_id = ?", (topic_id,))
                # also might have question text
            else:
                cursor.execute("SELECT text FROM articles WHERE topic_id = ?", (topic_id,))
            
            try:
                res = cursor.fetchone()
                if res:
                    print(f"Text: {res[0]}")
            except Exception as e:
                print(f"Could not fetch text from secondary table: {e}")
                
    except Exception as e:
        print(f"Error in {db}: {e}")
