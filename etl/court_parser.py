"""
從資料夾名稱解析出：unit_norm, root_norm, level, county, district
"""
import re
from typing import Optional, Dict
from simple_court_mapping import SIMPLE_COURT_MAPPING

DISTRICT_COURT_MAPPING = {
    "臺灣臺北地方法院": {"county": "臺北市",  "district": "中正區"},
    "臺灣士林地方法院": {"county": "臺北市",  "district": "士林區"},
    "臺灣新北地方法院": {"county": "新北市",  "district": "土城區"},
    "臺灣桃園地方法院": {"county": "桃園市",  "district": "桃園區"},
    "臺灣新竹地方法院": {"county": "新竹縣",  "district": "竹北市"},
    "臺灣苗栗地方法院": {"county": "苗栗縣",  "district": "苗栗市"},
    "臺灣臺中地方法院": {"county": "臺中市",  "district": "西區"},
    "臺灣彰化地方法院": {"county": "彰化縣",  "district": "員林市"},
    "臺灣南投地方法院": {"county": "南投縣",  "district": "南投市"},
    "臺灣雲林地方法院": {"county": "雲林縣",  "district": "虎尾鎮"},
    "臺灣嘉義地方法院": {"county": "嘉義市",  "district": "東區"},
    "臺灣臺南地方法院": {"county": "臺南市",  "district": "安平區"},
    "臺灣橋頭地方法院": {"county": "高雄市",  "district": "橋頭區"},
    "臺灣高雄地方法院": {"county": "高雄市",  "district": "前金區"},
    "臺灣屏東地方法院": {"county": "屏東縣",  "district": "屏東市"},
    "臺灣臺東地方法院": {"county": "臺東縣",  "district": "臺東市"},
    "臺灣花蓮地方法院": {"county": "花蓮縣",  "district": "花蓮市"},
    "臺灣宜蘭地方法院": {"county": "宜蘭縣",  "district": "宜蘭市"},
    "臺灣基隆地方法院": {"county": "基隆市",  "district": "信義區"},
    "臺灣澎湖地方法院": {"county": "澎湖縣",  "district": "馬公市"},
    "福建金門地方法院": {"county": "金門縣",  "district": "金城鎮"},
    "福建連江地方法院": {"county": "連江縣",  "district": "南竿鄉"},
}


def parse_court_from_folder(folder_name: str) -> Optional[Dict[str, any]]:
    """
    從資料夾名稱解析法院資訊

    Args:
        folder_name: 例如 "臺灣高等法院民事", "三重簡易庭刑事"

    Returns:
        {
            "unit_norm": "臺灣新北地方法院",
            "root_norm": "臺灣新北地方法院",
            "county": "新北市",
            "district": None,
            "level": 3
        }
        若無法解析則回傳 None
    """
    # 移除案件類別（民事/刑事/行政/智慧財產/家事/少年及家事/少年），得到純法院名
    court_name = re.sub(r'(民事|刑事|行政|智慧財產|家事|少年及家事|少年)$', '', folder_name)

    # 1. 最高法院
    if "最高法院" in court_name:
        return {
            "unit_norm": "最高法院",
            "root_norm": "最高法院",
            "county": "臺北市",
            "district": None,
            "level": 1
        }

    # 2. 智慧財產及商業法院（專業法院，相當於高院層級）
    if "智慧財產及商業法院" in court_name:
        return {
            "unit_norm": "智慧財產及商業法院",
            "root_norm": "智慧財產及商業法院",
            "county": "臺北市",
            "district": None,
            "level": 2
        }

    # 3. 高等法院（含分院）
    if "高等法院" in court_name:
        # 判斷是否為分院
        branch_match = re.search(r'高等法院(.+)分院', court_name)
        if branch_match:
            branch = branch_match.group(1)  # 臺中 / 臺南 / 花蓮 / 高雄 / 金門
            county_map = {
                "臺中": "臺中市",
                "臺南": "臺南市",
                "花蓮": "花蓮縣",
                "高雄": "高雄市",
                "金門": "金門縣"
            }
            county = county_map.get(branch, "未知")
        else:
            county = "臺北市"  # 臺灣高等法院（本院）在台北

        root_norm = court_name.replace(branch + "分院", "") if branch_match else court_name
        return {
            "unit_norm": court_name,
            "root_norm": root_norm,
            "county": county,
            "district": None,
            "level": 2
        }

    # 3. 簡易庭（查 SIMPLE_COURT_MAPPING ）
    if "簡易庭" in court_name:
        # 移除括號內容（如 "(含埔里)"）
        simple_name = re.sub(r'\(含.+\)', '', court_name)
        mapping = SIMPLE_COURT_MAPPING.get(simple_name)

        if mapping:
            return {
                "unit_norm": mapping["parent_court"] + simple_name,
                "root_norm": mapping["parent_court"],
                "county": mapping["county"],
                "district": mapping["district"],
                "level": 4
            }
        else:
            print(f"警告：未找到簡易庭對應 - {simple_name}")
            return None

    # 4. 地方法院（查表）
    if "地方法院" in court_name:
        mapping = DISTRICT_COURT_MAPPING.get(court_name)
        if mapping:
            return {
                "unit_norm": court_name,
                "root_norm": court_name,
                "county": mapping["county"],
                "district": mapping["district"],
                "level": 3
            }
        else:
            print(f"警告：未找到地方法院對應 - {court_name}")
            return None

    # 無法解析
    print(f"警告：無法解析法院名稱 - {court_name}")
    return None


# 測試
if __name__ == "__main__":
    test_cases = [
        "臺灣高等法院民事",
        "臺灣新北地方法院刑事",
        "三重簡易庭民事",
        "最高法院刑事",
    ]

    for case in test_cases:
        result = parse_court_from_folder(case)
        print(f"\n{case}")
        print(f"  → {result}")
