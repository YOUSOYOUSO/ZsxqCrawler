def fix_file(filename, replacements):
    with open(filename, 'r', encoding='utf-8') as f:
        text = f.read()
    
    for old, new in replacements:
        if old in text:
            text = text.replace(old, new)
            print(f"Replaced in {filename}: {old.strip()}")
            
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(text)

fix_file("stock_analyzer.py", [
    ("'topic_id': topic_id,", "'topic_id': str(topic_id) if topic_id is not None else None,"),
    ("'topic_id': row['topic_id'],", "'topic_id': str(row['topic_id']) if row['topic_id'] is not None else None,"),
    ("'topic_id': item['topic_id'],", "'topic_id': str(item['topic_id']) if item['topic_id'] is not None else None,")
])

fix_file("global_analyzer.py", [
    ("'topic_id': topic_id,", "'topic_id': str(topic_id) if topic_id is not None else None,"),
    ('"topic_id": topic_id,', '"topic_id": str(topic_id) if topic_id is not None else None,')
])

fix_file("zsxq_columns_database.py", [
    ("'topic_id': row[0],", "'topic_id': str(row[0]) if row[0] is not None else None,"),
    ("'topic_id': row[1],", "'topic_id': str(row[1]) if row[1] is not None else None,")
])

fix_file("app/main.py", [
    ('"topic_id": topic[0],', '"topic_id": str(topic[0]) if topic[0] is not None else None,'),
    ('"topic_id": topic_id,', '"topic_id": str(topic_id) if topic_id is not None else None,')
])

fix_file("zsxq_database.py", [
    ('"topic_id": topic_row[0],', '"topic_id": str(topic_row[0]) if topic_row[0] is not None else None,'),
    ('"topic_id": topic[0],', '"topic_id": str(topic[0]) if topic[0] is not None else None,')
])
