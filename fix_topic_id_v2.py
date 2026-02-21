import re

def fix(filename):
    with open(filename, "r", encoding="utf-8") as f:
        text = f.read()

    # Replaces 'topic_id': <word>,
    text = re.sub(r"'topic_id':\s*([a-zA-Z0-9_]+),", r"'topic_id': str(\1) if \1 is not None else None,", text)
    # Replaces 'topic_id': <word>['topic_id'],
    text = re.sub(r"'topic_id':\s*([a-zA-Z0-9_]+)\['topic_id'\],", r"'topic_id': str(\1['topic_id']) if \1['topic_id'] is not None else None,", text)
    # Replaces "topic_id": <word>,
    text = re.sub(r'"topic_id":\s*([a-zA-Z0-9_]+),', r'"topic_id": str(\1) if \1 is not None else None,', text)
    # Replaces "topic_id": <word>[0],
    text = re.sub(r'"topic_id":\s*([a-zA-Z0-9_]+)\[0\],', r'"topic_id": str(\1[0]) if \1[0] is not None else None,', text)
    # Replaces 'topic_id': <word>[0],
    text = re.sub(r"'topic_id':\s*([a-zA-Z0-9_]+)\[0\],", r"'topic_id': str(\1[0]) if \1[0] is not None else None,", text)
    # Replaces 'topic_id': <word>[1],
    text = re.sub(r"'topic_id':\s*([a-zA-Z0-9_]+)\[1\],", r"'topic_id': str(\1[1]) if \1[1] is not None else None,", text)
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Processed {filename}")

files = ["stock_analyzer.py", "global_analyzer.py", "zsxq_columns_database.py", "main.py", "zsxq_database.py"]
for f in files:
    fix(f)
