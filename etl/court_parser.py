"""
從資料夾名稱解析出：unit_norm, root_norm, level, county, district, case_type
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

# 高等行政法院（含地方庭）city → county/district
HAC_COUNTY_MAPPING = {
    "臺北": {"county": "臺北市", "district": "中正區"},
    "臺中": {"county": "臺中市", "district": "西區"},
    "高雄": {"county": "高雄市", "district": "前金區"},
}


def parse_court_from_folder(folder_name: str) -> Optional[Dict[str, any]]:
    """
    從資料夾名稱解析法院資訊，並回傳 case_type。

    Args:
        folder_name: 例如 "臺灣高等法院民事", "三重簡易庭刑事",
                     "臺北高等行政法院 地方庭行政", "臺灣高雄少年及家事法院民事"

    Returns:
        {
            "unit_norm":  str,
            "root_norm":  str,
            "county":     str | None,
            "district":   str | None,
            "level":      int,
            "case_type":  str | None,  # 民事/刑事/行政/憲法；其他後綴（家事等）為 None
        }
        若無法解析則回傳 None
    """
    # 提取案件類別後綴；家事歸入民事（最高法院家事庭屬民事範疇）
    suffix_match = re.search(r'(民事|刑事|行政|憲法|家事)$', folder_name)
    raw_suffix = suffix_match.group(1) if suffix_match else None
    case_type = '民事' if raw_suffix == '家事' else raw_suffix
    court_name = folder_name[:-len(raw_suffix)] if raw_suffix else folder_name

    # ── 順序很重要：子字串必須先於父字串檢查 ──

    # 0. 憲法法庭
    if "憲法法庭" in court_name:
        return {
            "unit_norm": "憲法法庭",
            "root_norm": "憲法法庭",
            "county": "臺北市",
            "district": "中正區",
            "level": 0,
            "case_type": case_type,
        }

    # 1. 最高行政法院（必須在最高法院之前）
    if "最高行政法院" in court_name:
        return {
            "unit_norm": "最高行政法院",
            "root_norm": "最高行政法院",
            "county": "臺北市",
            "district": "中正區",
            "level": 1,
            "case_type": case_type,
        }

    # 2. 最高法院
    if "最高法院" in court_name:
        return {
            "unit_norm": "最高法院",
            "root_norm": "最高法院",
            "county": "臺北市",
            "district": "中正區",
            "level": 1,
            "case_type": case_type,
        }

    # 3. 智慧財產及商業法院（相當於高院層級）
    if "智慧財產及商業法院" in court_name:
        return {
            "unit_norm": "智慧財產及商業法院",
            "root_norm": "智慧財產及商業法院",
            "county": "新北市",
            "district": "板橋區",
            "level": 2,
            "case_type": case_type,
        }

    # 4. 高等行政法院地方庭（必須在高等行政法院和高等法院之前）
    #    資料夾格式：「臺北高等行政法院 地方庭行政」（地方庭前有空格）
    if "高等行政法院" in court_name and "地方庭" in court_name:
        city_match = re.search(r'(臺北|臺中|高雄)高等行政法院', court_name)
        if city_match:
            city = city_match.group(1)
            geo = HAC_COUNTY_MAPPING[city]
            return {
                "unit_norm": f"{city}高等行政法院地方庭",
                "root_norm": f"{city}高等行政法院",
                "county": geo["county"],
                "district": geo["district"],
                "level": 3,
                "case_type": case_type,
            }
        print(f"警告：無法解析高等行政法院地方庭城市 - {folder_name}")
        return None

    # 5. 高等行政法院（必須在高等法院之前）
    if "高等行政法院" in court_name:
        city_match = re.search(r'(臺北|臺中|高雄)高等行政法院', court_name)
        if city_match:
            city = city_match.group(1)
            geo = HAC_COUNTY_MAPPING[city]
            unit_norm = f"{city}高等行政法院"
            return {
                "unit_norm": unit_norm,
                "root_norm": unit_norm,
                "county": geo["county"],
                "district": geo["district"],
                "level": 2,
                "case_type": case_type,
            }
        print(f"警告：無法解析高等行政法院城市 - {folder_name}")
        return None

    # 6. 高等法院（含分院）
    if "高等法院" in court_name:
        branch_match = re.search(r'高等法院(.+)分院', court_name)
        if branch_match:
            branch = branch_match.group(1)
            county_map = {
                "臺中": "臺中市",
                "臺南": "臺南市",
                "花蓮": "花蓮縣",
                "高雄": "高雄市",
                "金門": "金門縣",
            }
            county = county_map.get(branch, "未知")
            root_norm = court_name.replace(branch + "分院", "")
        else:
            county = "臺北市"
            root_norm = court_name
        return {
            "unit_norm": court_name,
            "root_norm": root_norm,
            "county": county,
            "district": None,
            "level": 2,
            "case_type": case_type,
        }

    # 7. 少年及家事法院（必須在地方法院之前）
    if "少年及家事法院" in court_name:
        if court_name == "臺灣高雄少年及家事法院":
            return {
                "unit_norm": "臺灣高雄少年及家事法院",
                "root_norm": "臺灣高雄少年及家事法院",
                "county": "高雄市",
                "district": "楠梓區",
                "level": 3,
                "case_type": case_type,
            }
        print(f"警告：未支援的少年及家事法院 - {court_name}（請補充 parse_court_from_folder）")
        return None

    # 8. 簡易庭（查 SIMPLE_COURT_MAPPING）
    if "簡易庭" in court_name:
        simple_name = re.sub(r'\(含.+\)', '', court_name)
        mapping = SIMPLE_COURT_MAPPING.get(simple_name)
        if mapping:
            return {
                "unit_norm": mapping["parent_court"] + simple_name,
                "root_norm": mapping["parent_court"],
                "county": mapping["county"],
                "district": mapping["district"],
                "level": 4,
                "case_type": case_type,
            }
        print(f"警告：未找到簡易庭對應 - {simple_name}")
        return None

    # 9. 地方法院（查 DISTRICT_COURT_MAPPING）
    if "地方法院" in court_name:
        mapping = DISTRICT_COURT_MAPPING.get(court_name)
        if mapping:
            return {
                "unit_norm": court_name,
                "root_norm": court_name,
                "county": mapping["county"],
                "district": mapping["district"],
                "level": 3,
                "case_type": case_type,
            }
        print(f"警告：未找到地方法院對應 - {court_name}")
        return None

    print(f"警告：無法解析法院名稱 - {court_name}")
    return None


if __name__ == "__main__":
    test_cases = [
        "臺灣高等法院民事",
        "臺灣新北地方法院刑事",
        "三重簡易庭民事",
        "最高法院刑事",
        "最高法院家事",
        "最高行政法院行政",
        "臺北高等行政法院行政",
        "臺中高等行政法院 地方庭行政",
        "高雄高等行政法院 地方庭行政",
        "臺灣高雄少年及家事法院民事",
        "憲法法庭憲法",
        "智慧財產及商業法院行政",
    ]
    for case in test_cases:
        result = parse_court_from_folder(case)
        print(f"\n{case}")
        print(f"  → {result}")
