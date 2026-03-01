
from modules.analyzers.stock_analyzer import StockAnalyzer
import json

analyzer = StockAnalyzer("88888142214212")
text = """设备重大更新‼

存储大客户# 超级大单即将落地，单本轮订单将有望远大于去年全年水平，主因国产化率大幅提升！同时有望签订未来包产大订单！

核心标的：
1、拓荆科技（两存占比70%，PECVD高份额+GTC大会NV新方案将有望采用Hybrid honding方案）

2、中微公司（两存占比65%，上海区位优势明显）

3、微导纳米（两存占比80%，叠加太空光伏逻辑）

4、其他深度受益：北方华创、芯源微、华海清科、精智达等，重视板块行机遇！"""

results = analyzer.extract_stocks(text)
print(f"Extracted stocks: {json.dumps(results, ensure_ascii=False, indent=2)}")

# Check if 精智达 is in dictionary
print(f"Is '精智达' in name_to_code: {'精智达' in analyzer._name_to_code}")
print(f"Is '688627.SH' in stock_dict: {'688627.SH' in analyzer._stock_dict}")
