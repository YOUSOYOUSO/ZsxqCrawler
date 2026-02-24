from modules.analyzers.global_analyzer import get_global_analyzer
import json

analyzer = get_global_analyzer()
sectors = analyzer.get_global_sector_heat()
print(json.dumps(sectors, indent=2, ensure_ascii=False))
