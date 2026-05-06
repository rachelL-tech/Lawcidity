"""
從資料夾名稱解析出：unit_norm, court_root_norm, level, county, district, case_type

命名慣例：
  court_root_norm  具體法院聚合名稱（如「臺灣高等法院」），供 citation parser / self_key 使用
  root_norm        通用7種分類（如「高等法院」），由 to_generic_root_norm(unit_norm) 計算，存入 DB
"""
import re
from typing import Optional, Dict
from court_mapping import SIMPLE_COURT_MAPPING, DISTRICT_COURT_MAPPING, HAC_COUNTY_MAPPING


def to_generic_root_norm(unit_norm: str) -> str:
    """
    將具體法院 unit_norm 轉換為 DB 用通用層級分類（decisions.root_norm / court_units.root_norm）

    回傳值：
        最高法院 / 最高行政法院 / 憲法法庭
        高等法院 / 高等行政法院 / 高等行政法院地方庭
        智財商業法院 / 少家法院
        地方法院 / 地方法院簡易庭
    """
    if "憲法法庭" in unit_norm:                              return "憲法法庭"
    if "最高行政法院" in unit_norm:                          return "最高行政法院"
    if "最高法院" in unit_norm:                             return "最高法院"
    if "智慧財產" in unit_norm:                             return "智財商業法院"
    if "簡易庭" in unit_norm:                               return "地方法院簡易庭"
    if "地方庭" in unit_norm:                               return "高等行政法院地方庭"
    if "行政法院" in unit_norm:                             return "高等行政法院"
    if "高等法院" in unit_norm:                             return "高等法院"
    if "少年" in unit_norm or "家事" in unit_norm:          return "少家法院"
    if "地方法院" in unit_norm:                             return "地方法院"
    return unit_norm  # fallback

def parse_court_from_folder(folder_name: str) -> Optional[Dict[str, any]]:
    """
    從資料夾名稱解析法院資訊，並回傳 case_type。

    Args:
        folder_name: 例如 "臺灣高等法院民事", "三重簡易庭刑事", "臺北高等行政法院 地方庭行政", "臺灣高雄少年及家事法院民事"

    Returns:
        {
            "unit_norm":       str,         # 精確名稱（自然鍵）
            "court_root_norm": str,         # 具體聚合名稱（citation parser / self_key 用）
            "county":          str | None,
            "district":        str | None,
            "level":           int,         # 0=憲法法庭 1=最高 2=高院 3=地院/地方庭 4=簡易庭
            "case_type":       str | None,  # 民事/刑事/行政/憲法；其他後綴（家事等）為 None
        }
        若無法解析則回傳 None
    """
    # 提取案件類別後綴；家事歸入民事（最高法院家事庭屬民事範疇）
    match = re.search(r'(民事|刑事|行政|憲法|家事)$', folder_name)
    if match:
        raw_case_type = match.group()
        case_type = '民事' if raw_case_type == '家事' else raw_case_type
        court_name = folder_name[:match.start()]
    else:
        case_type = None
        court_name = folder_name

    # ── 順序很重要：子字串必須先於父字串檢查 ──

    # 0. 憲法法庭
    if "憲法法庭" in court_name:
        return {
            "unit_norm":       "憲法法庭",
            "court_root_norm": "憲法法庭",
            "county":          "臺北市",
            "district":        "中正區",
            "level":           0,
            "case_type":       case_type,
        }

    # 1. 最高行政法院（必須在最高法院之前）
    if "最高行政法院" in court_name:
        return {
            "unit_norm":       "最高行政法院",
            "court_root_norm": "最高行政法院",
            "county":          "臺北市",
            "district":        "中正區",
            "level":           1,
            "case_type":       case_type,
        }

    # 2. 最高法院
    if "最高法院" in court_name:
        return {
            "unit_norm":       "最高法院",
            "court_root_norm": "最高法院",
            "county":          "臺北市",
            "district":        "中正區",
            "level":           1,
            "case_type":       case_type,
        }

    # 3. 智慧財產及商業法院（相當於高院層級）
    if "智慧財產及商業法院" in court_name:
        return {
            "unit_norm":       "智慧財產及商業法院",
            "court_root_norm": "智慧財產及商業法院",
            "county":          "新北市",
            "district":        "板橋區",
            "level":           2,
            "case_type":       case_type,
        }

    # 4. 高等行政法院地方庭（必須在高等行政法院和高等法院之前）
    #    資料夾格式：「臺北高等行政法院 地方庭行政」（地方庭前有空格）
    if "高等行政法院" in court_name and "地方庭" in court_name:
        city_match = re.search(r'(臺北|臺中|高雄)高等行政法院', court_name)
        if city_match:
            city = city_match.group(1)
            geo = HAC_COUNTY_MAPPING[city]
            return {
                "unit_norm":       f"{city}高等行政法院地方庭",
                "court_root_norm": f"{city}高等行政法院",
                "county":          geo["county"],
                "district":        geo["district"],
                "level":           3,
                "case_type":       case_type,
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
                "unit_norm":       unit_norm,
                "court_root_norm": unit_norm,
                "county":          geo["county"],
                "district":        geo["district"],
                "level":           2,
                "case_type":       case_type,
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
        else:
            county = "臺北市"
        return {
            "unit_norm":       court_name,
            "court_root_norm": court_name,
            "county":          county,
            "district":        None,
            "level":           2,
            "case_type":       case_type,
        }

    # 7. 少年及家事法院（必須在地方法院之前）
    if "少年及家事法院" in court_name:
        if court_name == "臺灣高雄少年及家事法院":
            return {
                "unit_norm":       "臺灣高雄少年及家事法院",
                "court_root_norm": "臺灣高雄少年及家事法院",
                "county":          "高雄市",
                "district":        "楠梓區",
                "level":           3,
                "case_type":       case_type,
            }
        print(f"警告：未支援的少年及家事法院 - {court_name}（請補充 parse_court_from_folder）")
        return None

    # 8. 簡易庭（查 SIMPLE_COURT_MAPPING）
    if "簡易庭" in court_name:
        simple_name = re.sub(r'\(含.+\)', '', court_name)
        mapping = SIMPLE_COURT_MAPPING.get(simple_name)
        if mapping:
            return {
                "unit_norm":       mapping["parent_court"] + simple_name,
                "court_root_norm": mapping["parent_court"],
                "county":          mapping["county"],
                "district":        mapping["district"],
                "level":           4,
                "case_type":       case_type,
            }
        print(f"警告：未找到簡易庭對應 - {simple_name}")
        return None

    # 9. 地方法院（查 DISTRICT_COURT_MAPPING）
    if "地方法院" in court_name:
        mapping = DISTRICT_COURT_MAPPING.get(court_name)
        if mapping:
            return {
                "unit_norm":       court_name,
                "court_root_norm": court_name,
                "county":          mapping["county"],
                "district":        mapping["district"],
                "level":           3,
                "case_type":       case_type,
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
        if result:
            generic = to_generic_root_norm(result["unit_norm"])
            print(f"\n{case}")
            print(f"  court_root_norm = {result['court_root_norm']}")
            print(f"  root_norm (generic) = {generic}")
            print(f"  level = {result['level']}")
        else:
            print(f"\n{case} → None")
