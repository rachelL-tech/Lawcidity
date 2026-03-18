"""
Phase 1: find_all_candidates — 純掃描，直接在 clean_text 上跑，不做任何過濾。
Phase 2: filter_candidates  — 8 條 rule pipeline，決定 accept/reject。

設計原則：
- 不呼叫 preprocess_text；text_cleaner 已合併折行、壓縮空白
- Phase 1：本院無法解析 → 不產出 candidate（取代舊 R010），其餘無過濾
- Phase 2：R007→R006→R001→R002→R009→R003→R011→R005，第一條命中即 reject
- ACCEPT_RE 統一用於 R002/R009/R005（取代舊的窄 _CITE_INTENT_RE）
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Set

# ═══════════════════════════════════════════════════════════════════════════════
# RE & 常數（從 citation_parser.py 獨立，含擴充）
# ═══════════════════════════════════════════════════════════════════════════════

# ─── 法院引用 RE ─────────────────────────────────────────────────────────────

ANY_COURT_CITATION = re.compile(
    r'(?<!原審)'
    r'((?:最高(?:行政)?法院|憲法法庭|本院|'
    r'(?:臺灣|台灣|福建)[\u4e00-\u9fff]*?法院|'
    r'[\u4e00-\u9fff]+高等行政法院(?:地方庭)?|'
    r'北高行|中高行|高高行)'
    r'(?:[\u4e00-\u9fff]+分院)?)'
    r'(?:[\u4e00-\u9fff]{0,8}(?:庭|法庭))?'
    r'(?:(?:刑事|民事|行政)?大法庭)?'
    r'(?:（[^）\r\n]{0,10}）\s*)?'
    r'\s*(\d{2,3})\s*年\s*度?\s*'
    r'([\u4e00-\u9fff]{1,10}?)\s*字?\s*第\s*(\d+)\s*號'
    r'(?:\s*(?:民事|刑事|行政)?\s*(判決|裁定|判例|裁判|理由))?'
)

ABBR_CITATION = re.compile(
    r'[、，及與暨或,]\s*(\d{2,3})\s*年\s*度?\s*([\u4e00-\u9fff]{1,10}?)\s*字?\s*第\s*(\d+)\s*號'
    r'(?:\s*(?:民事|刑事|行政)?\s*(判決|裁定|判例|裁判|理由))?'
)

TARGET_COURTS: Set[str] = {
    '最高法院', '最高行政法院', '憲法法庭',
    '台灣高等法院', '台灣高等法院台中分院', '台灣高等法院台南分院',
    '台灣高等法院高雄分院', '台灣高等法院花蓮分院', '福建高等法院金門分院',
    '台北高等行政法院', '台北高等行政法院地方庭',
    '台中高等行政法院', '台中高等行政法院地方庭',
    '高雄高等行政法院', '高雄高等行政法院地方庭',
    '台灣台北地方法院', '台灣新北地方法院', '台灣士林地方法院',
    '台灣桃園地方法院', '台灣新竹地方法院', '台灣苗栗地方法院',
    '台灣台中地方法院', '台灣彰化地方法院', '台灣南投地方法院',
    '台灣雲林地方法院', '台灣嘉義地方法院', '台灣台南地方法院',
    '台灣高雄地方法院', '台灣橋頭地方法院', '台灣屏東地方法院',
    '台灣台東地方法院', '台灣花蓮地方法院', '台灣宜蘭地方法院',
    '台灣基隆地方法院', '台灣澎湖地方法院',
    '福建金門地方法院', '福建連江地方法院',
}

# ─── Authority RE ────────────────────────────────────────────────────────────

RESOLUTION_RE = re.compile(
    r'(最高法院|本院)'
    r'(\d{2,3})年度?'
    r'第(\d+)次'
    r'(民庭|刑庭|民事庭|刑事庭|民刑庭|民刑事庭|民刑事庭總會|刑事庭總會|民事庭總會)'
    r'(?:庭長)?'
    r'(?:會議)?決議'
)

ADMIN_RESOLUTION_RE = re.compile(
    r'(最高行政法院|改制前行政法院|本院)'
    r'(\d{2,3})年度?'
    r'(\d{1,2})月份?'
    r'(?:第(\d+)次)?'
    r'(?:[、及]\d{2,3}年\d{1,2}月份?(?:第\d+次)?)*'
    r'(?:庭長(?:法官|評事))?聯席會議'
    r'決議'
)

GRAND_INTERP_RE = re.compile(
    r'(?:(?:司法院(?:大法官(?:會議)?)?|大法官(?:會議)?))?釋字第\s*(\d+)\s*號'
)

ABBR_GRAND_INTERP_RE = re.compile(
    r'[、及與暨或,]\s*第\s*(\d+)\s*號'
)

CONFERENCE_RE = re.compile(
    r'(?:'
    r'(?:(?:臺灣)?高等行政法院(?:及[\u4e00-\u9fff]{2,25}庭)?)'
    r'|(?:司法院[\u4e00-\u9fff]{0,15})'
    r'|(?:(?:臺灣)?高等法院(?:暨所屬法院)?)'
    r'|(?:本院(?:暨所屬法院)?)'
    r')?'
    r'(?:民國)?(\d{2,3})年'
    r'[\u4e00-\u9fff\d年月日]{0,30}?'
    r'法律座談會'
    r'(?:'
    r'[\u4e00-\u9fff\s]{0,40}?'
    r'(?:提案第?\s*|第\s*)'
    r'([一二三四五六七八九十百\d]+)'
    r'\s*號?'
    r')?'
)

_AGENCY_OPINION_RE = re.compile(
    r'(?:'
    r'(?:民國\s*)?(\d{2,3})年'
    r'|司法院[\u4e00-\u9fff]{0,5}廳?'
    r'|(?:臺灣)?高等(?:行政)?法院'
    r')'
    r'(?:(?!研討)[\u4e00-\u9fff\d（）()第號期屆年月日、，\s]){5,80}?'
    r'研審小組'
    r'(?:[\u4e00-\u9fff\d（）()第號期屆年月日、，\s]{0,80}?)?'
    r'(?:研審)?意見'
)

# ─── 工具函式 ────────────────────────────────────────────────────────────────

_COURT_ABBR = {
    '北高行': '台北高等行政法院',
    '中高行': '台中高等行政法院',
    '高高行': '高雄高等行政法院',
}


def _normalize_court(court: str) -> str:
    court = court.replace('臺', '台').strip()
    return _COURT_ABBR.get(court, court)


def _normalize_doc_type(raw_doc_type: Optional[str]) -> Optional[str]:
    if raw_doc_type in ('裁判', '理由'):
        return None
    return raw_doc_type


def _infer_organizer(raw_match: str) -> str:
    if '行政法院' in raw_match:
        return '高等行政法院'
    if '司法院' in raw_match:
        return '司法院'
    return '高等法院'


_CN_DIGITS = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
              '六': 6, '七': 7, '八': 8, '九': 9}


def cn_to_int(s: str) -> int:
    if s.isdigit():
        return int(s)
    total, cur = 0, 0
    for c in s:
        if c == '十':
            total += (cur or 1) * 10
            cur = 0
        elif c == '百':
            total += (cur or 1) * 100
            cur = 0
        elif c in _CN_DIGITS:
            cur = _CN_DIGITS[c]
    return total + cur


# ─── 文書結構 RE ─────────────────────────────────────────────────────────────

_PARA_START_RE = re.compile(
    r'\r\n[ \t　]{0,4}'
    r'(?='
    r'[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁戊己庚辛壬癸'
    r'㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩'
    r'①②③④⑤⑥⑦⑧⑨⑩'
    r'⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽'
    r'⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑'
    r']'
    r'|[（(][一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁戊己庚辛壬癸]'
    r'|[1-9][0-9]*[.、 　]'
    r'|[１-９][０-９]*[.、 　]'
    r')'
)

_REASON_SECTION_RE = re.compile(
    r'\r\n[ \t　]{0,4}'
    r'(?:[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁]+[、：,，])?'
    r'[ \t　]{0,4}'
    r'(?:(?:犯罪)?事實(?:及|與))?理由(?:要領)?'
    r'[ \t　：:\r\n]'
)

_SUB_CLAUSE_RE = re.compile(
    r'(?:(?:。|\r\n|[：:])[\uf000-\uffff\u3000-\u303f\u3200-\u32ff\u2460-\u24ff\t 　]{0,6})'
    r'((?:再|復|又|次|末|且)按|惟(?:按|查|依)?|又(?!按)|再者|所謂|另(?![行有外附])|按(?!照)|參酌|參以|觀諸|參諸)'
    r'[：:，,「]?'
)

# ─── Filter RE ───────────────────────────────────────────────────────────────

_CITE_CLOSING_RE = re.compile(r'意旨|參照|見解|供參|同此')

_PRIOR_CASE_RE = re.compile(
    # ── 案件結果 ──
    r'(?:確定|執行|終結)在案'
    r'|裁定駁回'
    r'|判決駁回'
    r'|駁回(?:上訴|抗告)?確定'
    r'|上訴(?:駁回|不受理)'
    r'|(?:判決|裁定|處分)確定|廢棄|發回|緩刑'
    r'|(?:此|業|嗣)經'
    r'|業據'
    # ── 程序動作 ──
    r'|不服'
    r'|(?:提起|撤回)上訴'
    r'|提起公訴'
    r'|言詞辯論終結'
    r'|移送(?:執行|併辦)|前依'
    # ── 進行中 ──
    r'|(?:審理|受理)中'
    r'|事件終結前'
    # ── 事實認定 ──
    r'|(?:判決|裁定)認定|即堪認定'
    # ── 文書/附件 ──
    r'|(?:判決|裁定)所載'
    r'|(?:如)?附表(?:所示|編號)?'
    # ── 量刑 ──
    r'|(?:判處|處有期徒刑|處拘役)'
)

_EVIDENCE_CITE_RE = re.compile(
    # ── 卷宗引用 ──
    r'[（( ](?:見)?(?:本院|偵查|原審|審理|上訴|抗告).{0,5}卷'
    r'|見本院'
    r'|見外放'
    r'|見偵查?卷'
    r'|刑事(?:卷宗?|判決書)'
    r'|偵(?:查影卷|字卷|查卷)'
    r'|他字卷|執行卷'
    r'|卷.{0,2}第\d'                        # 卷第X / 卷一第X / 卷（一）第X
    # ── 文書/證物 ──
    r'|判決書[，。（(第]'
    r'|鑑定(?:報告|函)'
    r'|起訴書'
    r'|(?:言詞辯論|準備程序)筆錄'
    r'|光碟'
    # ── 查閱/核閱 ──
    r'|查閱|核閱|調閱'
    r'|此經'
    r'|業據'
    # ── 在卷/附卷/存卷 ──
    r'|卷宗(?:可[稽查參])?'
    r'|在卷(?:可[稽查參])?'
    r'|存卷(?:可[稽查參])?'
    r'|附卷(?:可[稽查參考])?'
    r'|有.{0,15}(?:在|附)卷'               # 有…在卷/附卷
    # ── 可信/可憑類 ──
    r'|可查|可稽|可憑|可按|可佐'
    r'|足稽|足據'
    r'|無訛'
    r'|為證[，。；\s]'
    # ── 不爭執 ──
    r'|兩造(?:所)?不爭(?:執)?'
)


# ─── RawCandidate ─────────────────────────────────────────────────────────────

@dataclass
class RawCandidate:
    citation_type: str              # "decision" | "authority"
    court: str                      # 已 resolve + normalize（本院已展開）
    raw_match: str
    match_start: int                # clean_text 直接 offset
    match_end: int
    # decision only
    jyear: Optional[int] = None
    jcase_norm: Optional[str] = None
    jno: Optional[int] = None
    # authority only
    auth_type: Optional[str] = None
    auth_key: Optional[str] = None
    display: Optional[str] = None
    # shared
    doc_type: Optional[str] = None
    is_abbreviated: bool = False
    chain_court_source: Optional[str] = None   # debug: "本院" 等原始名稱
    needs_intent_signal: bool = False           # 釋字 / 憲法法庭


# ─── 工具 ─────────────────────────────────────────────────────────────────────

def _abbr_raw_and_start(abbr_match: re.Match) -> tuple[str, int]:
    """從 ABBR_CITATION match 取出去除前導分隔符的 raw_match 和起點 offset。"""
    full = abbr_match.group(0)     # e.g. "、113年度台上字第1號"
    raw = full[1:].lstrip()        # e.g. "113年度台上字第1號"
    start = abbr_match.start() + len(full) - len(raw)
    return raw, start


def _jcase_norm(raw: str) -> str:
    return raw.replace(' ', '').replace('臺', '台')


# ─── Phase 1 主函式 ───────────────────────────────────────────────────────────

def find_all_candidates(
    clean_text: str,
    *,
    court_root_norm: Optional[str] = None,
    target_courts: Set[str] = TARGET_COURTS,
) -> List[RawCandidate]:
    """
    直接在 clean_text 上掃描，回傳所有 RawCandidate，不做任何過濾。
    本院無法解析時不產出 candidate。
    """
    candidates: List[RawCandidate] = []
    _scan_decisions(clean_text, candidates, court_root_norm=court_root_norm, target_courts=target_courts)
    _scan_authorities(clean_text, candidates, court_root_norm=court_root_norm)
    return candidates


# ─── Decision 掃描（狀態機）────────────────────────────────────────────────────

def _scan_decisions(
    clean_text: str,
    candidates: List[RawCandidate],
    *,
    court_root_norm: Optional[str],
    target_courts: Set[str],
) -> None:
    current_court: Optional[str] = None
    chain_court_source: Optional[str] = None   # 本院 chain 時記錄原始名稱
    pos = 0

    # doc_type 回填：鏈中前幾筆沒有 doc_type，等後筆確認再回填
    pending_idxs: List[int] = []
    pending_court: Optional[str] = None
    pending_last_end: int = 0

    def _flush_pending(doc_type: Optional[str]) -> None:
        nonlocal pending_idxs, pending_court
        if not doc_type:
            return
        for i in pending_idxs:
            if candidates[i].doc_type is None:
                candidates[i].doc_type = doc_type
        pending_idxs.clear()
        pending_court = None

    def _check_chain_break(between_text: str, new_court: str) -> bool:
        """鏈是否斷掉（換法院或遇到句號等終止符）。"""
        if pending_court != new_court:
            return True
        return any(ch in between_text for ch in ("。", "；", "！", "？", "\r\n"))

    while pos < len(clean_text):
        # ① 省略引用：只在 chain 進行中（current_court 有值）才嘗試
        if current_court is not None:
            abbr = ABBR_CITATION.match(clean_text, pos)
            if abbr:
                if current_court in target_courts:
                    raw, mstart = _abbr_raw_and_start(abbr)
                    doc_type = _normalize_doc_type(abbr.group(4))

                    if pending_idxs and _check_chain_break(
                        clean_text[pending_last_end:abbr.start()], current_court
                    ):
                        pending_idxs.clear()
                        pending_court = None

                    c = RawCandidate(
                        citation_type="decision",
                        court=current_court,
                        raw_match=raw,
                        match_start=mstart,
                        match_end=abbr.end(),
                        jyear=int(abbr.group(1)),
                        jcase_norm=_jcase_norm(abbr.group(2)),
                        jno=int(abbr.group(3)),
                        doc_type=doc_type,
                        is_abbreviated=True,
                        chain_court_source=chain_court_source,
                    )
                    candidates.append(c)
                    _flush_pending(doc_type)
                    if doc_type is None:
                        pending_idxs.append(len(candidates) - 1)
                        pending_court = current_court
                    pending_last_end = abbr.end()

                pos = abbr.end()
                continue

        # ② 具名 citation
        full = ANY_COURT_CITATION.search(clean_text, pos)
        if full is None:
            break

        raw_court_str = full.group(1)
        resolved = _normalize_court(raw_court_str)

        if resolved == '本院':
            if court_root_norm:
                current_court = _normalize_court(court_root_norm)
                chain_court_source = '本院'
            else:
                # 解析不出 → 不產出 candidate，chain 中斷
                current_court = None
                chain_court_source = None
                pos = full.end()
                continue
            # 解析成功：fall through 繼續產出 candidate
        else:
            current_court = resolved
            chain_court_source = None

        if current_court in target_courts:
            raw_jcase = full.group(3)
            jcase = _jcase_norm(raw_jcase)
            doc_type = _normalize_doc_type(full.group(5))
            is_const = jcase in ('憲判', '憲判字')

            if pending_idxs and _check_chain_break(
                clean_text[pending_last_end:full.start()], current_court
            ):
                pending_idxs.clear()
                pending_court = None

            c = RawCandidate(
                citation_type="decision",
                court=current_court,
                raw_match=full.group(0),
                match_start=full.start(),
                match_end=full.end(),
                jyear=int(full.group(2)),
                jcase_norm=jcase,
                jno=int(full.group(4)),
                doc_type='憲判字' if is_const else doc_type,
                is_abbreviated=False,
                chain_court_source=chain_court_source,
                needs_intent_signal=is_const,
            )
            candidates.append(c)
            _flush_pending(doc_type)
            if doc_type is None:
                pending_idxs.append(len(candidates) - 1)
                pending_court = current_court
            pending_last_end = full.end()

            # 憲判字縮寫鏈：「第X號、第Y號」（無年份）
            if is_const:
                no_pos = full.end()
                while True:
                    am = ABBR_GRAND_INTERP_RE.match(clean_text, no_pos)
                    if not am:
                        break
                    candidates.append(RawCandidate(
                        citation_type="decision",
                        court=current_court,
                        raw_match=am.group(0),
                        match_start=am.start(),
                        match_end=am.end(),
                        jyear=int(full.group(2)),
                        jcase_norm=jcase,
                        jno=int(am.group(1)),
                        doc_type='憲判字',
                        is_abbreviated=True,
                        chain_court_source=chain_court_source,
                        needs_intent_signal=True,
                    ))
                    no_pos = am.end()
                pos = no_pos
                continue

        pos = full.end()


# ─── Authority 掃描（各類型獨立掃描）─────────────────────────────────────────

def _scan_authorities(
    clean_text: str,
    candidates: List[RawCandidate],
    *,
    court_root_norm: Optional[str],
) -> None:
    _scan_resolutions(clean_text, candidates, court_root_norm=court_root_norm)
    _scan_admin_resolutions(clean_text, candidates, court_root_norm=court_root_norm)
    _scan_grand_interps(clean_text, candidates)
    _scan_conferences(clean_text, candidates)
    _scan_agency_opinions(clean_text, candidates)


def _scan_resolutions(
    clean_text: str,
    candidates: List[RawCandidate],
    *,
    court_root_norm: Optional[str],
) -> None:
    for m in RESOLUTION_RE.finditer(clean_text):
        court_g = m.group(1)
        jyear = int(m.group(2))
        seq_no = int(m.group(3))
        court_type = m.group(4)

        if court_g == '本院':
            if not court_root_norm:
                continue
            court_g = _normalize_court(court_root_norm)

        candidates.append(RawCandidate(
            citation_type="authority",
            court=court_g,
            raw_match=m.group(0),
            match_start=m.start(),
            match_end=m.end(),
            auth_type="resolution",
            auth_key=f"{court_type}|{jyear}|{seq_no}",
            display=f"{court_g}{jyear}年度第{seq_no}次{court_type}會議決議",
        ))


def _scan_admin_resolutions(
    clean_text: str,
    candidates: List[RawCandidate],
    *,
    court_root_norm: Optional[str],
) -> None:
    for m in ADMIN_RESOLUTION_RE.finditer(clean_text):
        court_g = m.group(1)
        jyear = int(m.group(2))
        month = int(m.group(3))
        seq_no = int(m.group(4)) if m.group(4) else None

        if court_g == '本院':
            if not court_root_norm:
                continue
            court_g = _normalize_court(court_root_norm)

        auth_key = f"{court_g}|{jyear}|{month}" + (f"|{seq_no}" if seq_no else "")
        if seq_no:
            display = f"{court_g}{jyear}年{month}月份第{seq_no}次聯席會議決議"
        else:
            display = f"{court_g}{jyear}年{month}月份聯席會議決議"

        candidates.append(RawCandidate(
            citation_type="authority",
            court=court_g,
            raw_match=m.group(0),
            match_start=m.start(),
            match_end=m.end(),
            auth_type="admin_resolution",
            auth_key=auth_key,
            display=display,
        ))


def _scan_grand_interps(
    clean_text: str,
    candidates: List[RawCandidate],
) -> None:
    for m in GRAND_INTERP_RE.finditer(clean_text):
        no = m.group(1)
        candidates.append(RawCandidate(
            citation_type="authority",
            court="司法院大法官",
            raw_match=m.group(0),
            match_start=m.start(),
            match_end=m.end(),
            auth_type="grand_interp",
            auth_key=f"釋字|{no}",
            display=f"司法院大法官釋字第{no}號",
            needs_intent_signal=True,
        ))
        # 縮寫鏈：「第XXX號、第YYY號」繼承同一釋字類型
        pos = m.end()
        while True:
            am = ABBR_GRAND_INTERP_RE.match(clean_text, pos)
            if not am:
                break
            abbr_no = am.group(1)
            candidates.append(RawCandidate(
                citation_type="authority",
                court="司法院大法官",
                raw_match=am.group(0),
                match_start=am.start(),
                match_end=am.end(),
                auth_type="grand_interp",
                auth_key=f"釋字|{abbr_no}",
                display=f"司法院大法官釋字第{abbr_no}號",
                needs_intent_signal=True,
            ))
            pos = am.end()


def _scan_conferences(
    clean_text: str,
    candidates: List[RawCandidate],
) -> None:
    for m in CONFERENCE_RE.finditer(clean_text):
        year = int(m.group(1))
        no_raw = m.group(2)
        org = _infer_organizer(m.group(0))
        no_int = cn_to_int(no_raw) if no_raw else None
        auth_key = f"{org}|{year}|{no_int}" if no_int is not None else f"{org}|{year}"
        display = f"{org}{year}年法律座談會" + (f"提案第{no_int}號" if no_int is not None else "")

        candidates.append(RawCandidate(
            citation_type="authority",
            court=org,
            raw_match=m.group(0),
            match_start=m.start(),
            match_end=m.end(),
            auth_type="conference",
            auth_key=auth_key,
            display=display,
        ))


def _scan_agency_opinions(
    clean_text: str,
    candidates: List[RawCandidate],
) -> None:
    for m in _AGENCY_OPINION_RE.finditer(clean_text):
        raw_match = m.group(0)

        year_str = m.group(1)
        if not year_str:
            yr_m = re.search(r'(\d{2,3})年', raw_match)
            year_str = yr_m.group(1) if yr_m else None
        year = int(year_str) if year_str else None

        no_matches = re.findall(r'第(\d+)號', raw_match)
        no = no_matches[-1] if no_matches else None

        auth_key = '研審小組'
        if year:
            auth_key += f'|{year}'
        if no:
            auth_key += f'|{no}'

        display = raw_match[:60] + ('…' if len(raw_match) > 60 else '')

        candidates.append(RawCandidate(
            citation_type="authority",
            court="司法院",
            raw_match=raw_match,
            match_start=m.start(),
            match_end=m.end(),
            auth_type="agency_opinion",
            auth_key=auth_key,
            display=display,
        ))


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 2: filter_candidates
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Regex constants ──────────────────────────────────────────────────────────

# 嚴格 accept signal（R002 本院 / R009 地方法院）
ACCEPT_STRICT_RE = re.compile(
    r'考諸|參照|可參|同旨|意旨|同此'
    r'|指明|載明|闡釋|闡示|闡述|明示|係指|揭示'
    r'|可知|參酌|供參'
)

# 寬鬆 accept signal（R005a rescue / R010 authority）
ACCEPT_RE = re.compile(
    r'參照|意旨|可參|供參|同旨|見解|同此|揆諸|考諸'
    r'|指明|載明|闡釋|闡示|闡述|明示|揭示'
    r'|係指|所謂|係就|係以'
    r'|可知|參酌|釋明'
    r'|判決理由|理由書略以|要旨參|解釋在案|解釋文著有明文|解釋認'
    r'|依據?[憲司臺台最福]'
)

_PARTY_SUBJ = r'(?:再審|反訴|反請求)?(?:原告|被告|檢察官|上訴人|被上訴人|抗告人|聲請人|異議人)'

PARTY_CLAIM_RE = re.compile(
    # ── 意旨類（上訴/抗告/再審/聲請 + 意旨略以/主張/略謂/固以）──
    r'(?:上訴|抗告|再審|聲請刑事補償|聲請|異議|公訴)意旨(?:略以|主張|略謂|固以)'
    # ── 當事人主語 + 動詞 ──
    r'|' + _PARTY_SUBJ +
    r'(?:(?:雖|固)?(?:主張|辯稱|抗辯|則以|答辯|略以|所指|稱|謂)'
    r'|(?:雖|固)以'
    r'|所[舉引])'
    # ── 當事人主語 + 於文書（聲請書/起訴書/上訴狀等）──
    r'|' + _PARTY_SUBJ +
    r'於[^\r\n]{0,15}(?:書|狀)'
    # ── 動詞短語類（段落標題式）──
    r'|(?:主張|答辯|抗辯)略(?:以|謂)'
    r'|主張要旨'
    r'|上訴理由'
)

# QUOTED_REASONING_RE = re.compile(
#     r'原確定判決已論明'
#     r'|原判決說明|原判決認[為定]'
#     r'|原裁定略以|原裁定認定'
#     r'|原審(?:認[為定]?|見解|說明|略以)'
# )

# TURNING_RE = re.compile(r'惟|然[按查]?|但(?!書)')

_BLOCK_RESCUE_RE = re.compile(r'云云|等情(?!況|形)|等語|等等')

# 段落序號前綴：偵測 PARTY_CLAIM_RE 是否出現在段落標題中（有序號 → 不允許 TURNING_RE rescue）
# 偵測 PARTY_CLAIM_RE 是否為段落標題（不允許 TURNING_RE rescue）：
# 從當前行起點到 party keyword 前，只有「序號 + 空白」→ 標題型
# 「三、經查，原告主張」→ 前綴含「經查，」→ 不 match
_HEADING_PREFIX_RE = re.compile(
    r'(?:[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁]+[、：,，]'
    r'|[㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑①②③④⑤⑥⑦⑧⑨⑩])'
    r'[ \t　]*$'   # 序號後只允許空白
)

# ─── FilterContext ─────────────────────────────────────────────────────────────

@dataclass
class FilterContext:
    clean_text: str
    reason_pos: int          # 理由段起點（找不到 = 0，不過濾）
    zhengben_pos: int        # 正本證明與原本無異 起點（找不到 = len(text)）
    self_key: Optional[tuple]        # (court, jyear, jcase_norm, jno)
    court_root_norm: Optional[str]


def make_filter_context(
    clean_text: str,
    *,
    self_key: Optional[tuple] = None,
    court_root_norm: Optional[str] = None,
) -> FilterContext:
    # zhengben_pos：只用明確 footer 標記
    zhengben_pos = clean_text.find('以上正本證明與原本無異')
    if zhengben_pos == -1:
        zhengben_pos = len(clean_text)

    # reason_pos：理由段第一個命中
    m = _REASON_SECTION_RE.search(clean_text)
    reason_pos = m.start() if m else 0

    return FilterContext(
        clean_text=clean_text,
        reason_pos=reason_pos,
        zhengben_pos=zhengben_pos,
        self_key=self_key,
        court_root_norm=court_root_norm,
    )


# ─── Rule helpers ──────────────────────────────────────────────────────────────

def _post_sentence(clean_text: str, pos: int, cap: int = 200) -> str:
    """pos 之後到最近的 。（含）或 \\r\\n（不含）為止，cap 字上限。"""
    raw = clean_text[pos: pos + cap]
    end = len(raw)
    for sep, inc in (('。', 1), ('\r\n', 0)):
        idx = raw.find(sep)
        if idx != -1 and idx + inc < end:
            end = idx + inc
    return raw[:end]


def _clause_window(clean_text: str, match_start: int, match_end: int) -> str:
    """
    往前：截到第二近的標點（，/、/；/。/\\r\\n），多含一個子句。
    往後：截到最近的句號（。）或換行，確保能抓到句尾的 accept signal。
    """
    _SEPS = ('，', '、', '；', '。', '\r\n')

    # 往前：收集所有 separator 的「截斷位置」（separator 之後的第一個字）
    pre_start = max(0, match_start - 200)
    pre = clean_text[pre_start: match_start]
    cuts: list[int] = []
    for sep in _SEPS:
        idx = 0
        while True:
            pos = pre.find(sep, idx)
            if pos == -1:
                break
            cuts.append(pos + len(sep))
            idx = pos + len(sep)
    cuts.sort(reverse=True)  # 離 match 最近的在前
    if len(cuts) >= 2:
        pre_start = pre_start + cuts[1]
    # 0 or 1 個 separator → 用完整 200 字

    # 往後：截到最近的 。 或 \r\n
    post_cap = 200
    raw = clean_text[match_end: match_end + post_cap]
    end = len(raw)
    for sep in ('。', '\r\n'):
        idx = raw.find(sep)
        if idx != -1 and idx < end:
            end = idx

    return clean_text[pre_start: match_end + end]


def _accept_window(clean_text: str, match_start: int, match_end: int) -> str:
    """R002/R009/R010 共用的局部 context 窗口。
    前：最多 400 字，截斷到最近的 。/\\r\\n（取最靠近 match_start 的那個）。
    後：到最近的 。/\\r\\n（取最靠近 match_end 的那個），上限 200 字。
    """
    pre_start = max(0, match_start - 400)
    pre = clean_text[pre_start: match_start]
    cut = -1
    for sep in ('。', '\r\n'):
        idx = pre.rfind(sep)
        if idx != -1:
            pos = idx + len(sep)
            if pos > cut:
                cut = pos
    if cut != -1:
        pre_start = pre_start + cut
    post = _post_sentence(clean_text, match_end)
    return clean_text[pre_start: match_end + len(post)]

def _prev_heading_pos(clean_text: str, pos: int) -> int:
    """pos 之前最近的標題起點（_PARA_START_RE 或 \r\n）。"""
    window = clean_text[max(0, pos - 600): pos]
    last_para = None
    for m in _PARA_START_RE.finditer(window):
        last_para = m
    if last_para is not None:
        return max(0, pos - 600) + last_para.end()
    nl = window.rfind('\r\n')
    if nl != -1:
        return max(0, pos - 600) + nl + 2
    return max(0, pos - 600)


# ─── 8 Filter rules ────────────────────────────────────────────────────────────

def _r007_after_zhengben(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    if c.match_start >= ctx.zhengben_pos:
        return "R007_after_zhengben"
    return None


def _r006_before_reason(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    if ctx.reason_pos > 0 and c.match_start < ctx.reason_pos:
        return "R006_before_reason"
    return None


def _r001_self_citation(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    if c.citation_type != "decision" or ctx.self_key is None:
        return None
    court, jyear, jcase_norm, jno = ctx.self_key
    if (c.court == court and c.jyear == jyear
            and c.jcase_norm == jcase_norm and c.jno == jno):
        return "R001_self_citation"
    return None


def _r002_ben_yuan_intent(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    """本院 resolved citations 需有 ACCEPT_STRICT_RE signal（clause_window 範圍內）。"""
    if c.chain_court_source != '本院':
        return None
    window = _clause_window(ctx.clean_text, c.match_start, c.match_end)
    if ACCEPT_STRICT_RE.search(window):
        return None
    return "R002_ben_yuan_missing_intent"


def _r009_district_court_intent(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    """地方法院 decision 需有 ACCEPT_STRICT_RE signal（clause_window 範圍內）。"""
    if c.citation_type != "decision" or '地方法院' not in c.court:
        return None
    window = _clause_window(ctx.clean_text, c.match_start, c.match_end)
    if ACCEPT_STRICT_RE.search(window):
        return None
    return "R009_district_court_missing_intent"


def _r003_prior_case(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    """程序史引用：clause_window 有 PRIOR_CASE_RE，但 after 有 _CITE_CLOSING_RE 則放行。"""
    if c.citation_type != "decision" or c.jcase_norm in ('憲判', '憲判字'):
        return None
    window = _clause_window(ctx.clean_text, c.match_start, c.match_end)
    if not _PRIOR_CASE_RE.search(window):
        return None
    after = _post_sentence(ctx.clean_text, c.match_end)
    if _CITE_CLOSING_RE.search(after):
        return None
    return "R003_prior_case"


def _r011_evidence_cite(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    """卷證引用：clause_window 有 EVIDENCE_CITE_RE，但 after 有 _CITE_CLOSING_RE 則放行。"""
    if c.citation_type != "decision" or c.jcase_norm in ('憲判', '憲判字'):
        return None
    window = _clause_window(ctx.clean_text, c.match_start, c.match_end)
    if not _EVIDENCE_CITE_RE.search(window):
        return None
    after = _post_sentence(ctx.clean_text, c.match_end)
    if _CITE_CLOSING_RE.search(after):
        return None
    return "R011_evidence_cite"


def _r005_context_check(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    """
    Party-claim context check。

    PARTY_CLAIM_RE 命中 → ACCEPT_RE rescue（但 _BLOCK_RESCUE_RE 可擋）→ reject
    """
    heading_pos = _prev_heading_pos(ctx.clean_text, c.match_start)
    section_before = ctx.clean_text[heading_pos: c.match_start]

    pm = PARTY_CLAIM_RE.search(section_before)
    if not pm:
        return None

    # ACCEPT_RE rescue，但 _BLOCK_RESCUE_RE 可擋
    pre3 = ctx.clean_text[max(0, c.match_start - 3): c.match_start]
    after_sent = _post_sentence(ctx.clean_text, c.match_end)
    if ACCEPT_RE.search(pre3) or ACCEPT_RE.search(after_sent):
        after_long = _post_sentence(ctx.clean_text, c.match_end, cap=500)
        if not _BLOCK_RESCUE_RE.search(after_long):
            return None

    return "R005a_party_context"


def _r010_authority_intent(c: RawCandidate, ctx: FilterContext) -> Optional[str]:
    """釋字/憲法法庭需要 ACCEPT_RE signal（clause_window 範圍內）。"""
    if not c.needs_intent_signal:
        return None
    window = _clause_window(ctx.clean_text, c.match_start, c.match_end)
    if ACCEPT_RE.search(window):
        return None
    return "R010_authority_missing_intent"


_RULES = [
    _r007_after_zhengben,
    _r006_before_reason,
    _r001_self_citation,
    _r003_prior_case,
    _r011_evidence_cite,
    _r002_ben_yuan_intent,
    _r009_district_court_intent,
    _r005_context_check,
    _r010_authority_intent,
]


# ─── Phase 2 主函式 ────────────────────────────────────────────────────────────

def filter_candidates(
    candidates: List[RawCandidate],
    ctx: FilterContext,
) -> tuple[List[RawCandidate], List[tuple[RawCandidate, str]]]:
    """
    對每筆 candidate 跑 rule pipeline，回傳 (accepted, rejected)。
    rejected: List[(candidate, reject_reason)]
    """
    accepted: List[RawCandidate] = []
    rejected: List[tuple[RawCandidate, str]] = []

    for c in candidates:
        reason = None
        for rule in _RULES:
            reason = rule(c, ctx)
            if reason is not None:
                break
        if reason is None:
            accepted.append(c)
        else:
            rejected.append((c, reason))

    return accepted, rejected


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 3: build_snippets
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class CitationResult:
    citation_type: str          # "decision" | "authority"
    court: str
    raw_match: str
    match_start: int
    match_end: int
    snippet: str
    # decision only
    jyear: Optional[int] = None
    jcase_norm: Optional[str] = None
    jno: Optional[int] = None
    doc_type: Optional[str] = None
    is_abbreviated: bool = False
    target_case_type: Optional[str] = None  # 民事|刑事|行政|憲法，從 raw_match 單字推斷
    # authority only
    auth_type: Optional[str] = None
    auth_key: Optional[str] = None
    display: Optional[str] = None
    needs_intent_signal: bool = False

    def to_dict(self) -> dict:
        """轉換為 ingest_decisions.py 相容的 dict 格式（dict key 與舊版 extract_citations 一致）。"""
        d: dict = {
            "citation_type": self.citation_type,
            "court": self.court,
            "raw_match": self.raw_match,
            "match_start": self.match_start,
            "match_end": self.match_end,
            "snippet": self.snippet,
        }
        if self.citation_type == "decision":
            d.update({
                "jyear": self.jyear,
                "jcase_norm": self.jcase_norm,
                "jno": self.jno,
                "doc_type": self.doc_type,
                "target_case_type": self.target_case_type,
            })
        else:
            d.update({
                "auth_type": self.auth_type,
                "auth_key": self.auth_key,
                "display": self.display,
            })
        return d


_TRAILING_DELIM_RE = re.compile(r'(?:[。，,]|\r\n|[ \t\u3000])+')

def find_snippet_start(clean_text: str, match_start: int, para_cap: int = 1000) -> int:
    """
    向前找 snippet 起點。

    1. anchor = max(_SUB_CLAUSE_RE, 參照）) — 離 match 最近的語意起點
    2. anchor 與 match 之間有 _PARA_START_RE 或 \\r\\n → 用離 match 最近的
    3. 沒 anchor → last _PARA_START_RE → fallback（句號 → \\r\\n → window_start）
    """
    window_start = max(0, match_start - para_cap)
    window = clean_text[window_start: match_start]
    wlen = len(window)

    # fallback：預先算好
    period = window.rfind('。')
    nl_fb = window.rfind('\r\n')
    if period != -1:
        fallback = period + 1
    elif nl_fb != -1:
        fallback = nl_fb + 2
    else:
        fallback = 0

    # _SUB_CLAUSE_RE
    last_sub = None
    for m in _SUB_CLAUSE_RE.finditer(window):
        last_sub = m
    sub_pos = last_sub.start(1) if last_sub is not None else None

    # 參照）/參照)
    ref_close_pos = None
    for pat in ('參照）', '參照)'):
        idx = window.rfind(pat)
        if idx != -1:
            p = idx + len(pat)
            m = _TRAILING_DELIM_RE.match(window, p)
            if m:
                p = m.end()
            if ref_close_pos is None or p > ref_close_pos:
                ref_close_pos = p

    # 1. anchor = 離 match 最近的語意起點
    anchors = [p for p in (sub_pos, ref_close_pos) if p is not None]
    if anchors:
        anchor = max(anchors)
        # 2. anchor 與 match 之間，離 match 最近的 _PARA_START_RE 或 \r\n
        segment = window[anchor:]
        last_break = None
        for m in _PARA_START_RE.finditer(segment):
            last_break = anchor + m.start() + 2
        nl_idx = segment.rfind('\r\n')
        if nl_idx != -1:
            nl_pos = anchor + nl_idx + 2
            if nl_pos < wlen and (last_break is None or nl_pos > last_break):
                last_break = nl_pos
        if last_break is not None:
            return window_start + last_break
        return window_start + anchor

    # 3. 沒 anchor → last _PARA_START_RE
    last_para = None
    for m in _PARA_START_RE.finditer(window):
        last_para = m
    if last_para is not None:
        return window_start + last_para.start() + 2

    return window_start + fallback


def find_snippet_end(
    clean_text: str,
    match_start: int,
    match_end: int,
) -> int:
    """
    向後找 snippet 終點（統一邏輯，不分 decision/authority）。

    1. match_start 前 1 字有 ( / （ → 找匹配 ) / ） → 停
    2. 否則搜尋整段剩餘文本，取 min(最近 。, 最近 \\r\\n)
    3. 都找不到 → 文末
    """
    rest = clean_text[match_end:]

    # 1. Bracket enclosure：citation 被 (...) / （...） 包圍
    if match_start > 0 and clean_text[match_start - 1] in '(（':
        fw = rest.find('）')
        hw = rest.find(')')
        parens = [p for p in (fw, hw) if p != -1]
        if parens:
            return match_end + min(parens) + 1

    # 2. min(最近句號, 最近換行)
    period = rest.find('。')
    nl = rest.find('\r\n')
    ends = []
    if period != -1:
        end_pos = match_end + period + 1          # 含句號
        # 句號後緊接閉引號時延伸一字元（如「有別。」）
        if end_pos < len(clean_text) and clean_text[end_pos] in '」』"':
            end_pos += 1
        ends.append(end_pos)
    if nl != -1:
        ends.append(match_end + nl)               # 不含 \r\n
    if ends:
        return min(ends)

    # 3. 文末
    return len(clean_text)


_CASE_TYPE_SINGLE = [('民', '民事'), ('刑', '刑事'), ('行', '行政'), ('憲', '憲法')]


def _extract_target_case_type(raw_match: str) -> Optional[str]:
    """從 raw_match 本身推斷 target case_type。"""
    for kw, result in _CASE_TYPE_SINGLE:
        if kw in raw_match:
            return result
    return None


def build_snippets(
    accepted: List[RawCandidate],
    clean_text: str,
) -> List[CitationResult]:
    """
    對每筆 accepted candidate 切出 snippet，回傳 CitationResult list。
    """
    results: List[CitationResult] = []
    for c in accepted:
        snippet_start = find_snippet_start(clean_text, c.match_start)
        snippet_end = find_snippet_end(clean_text, c.match_start, c.match_end)
        snippet = clean_text[snippet_start:snippet_end]

        results.append(CitationResult(
            citation_type=c.citation_type,
            court=c.court,
            raw_match=c.raw_match,
            match_start=c.match_start,
            match_end=c.match_end,
            snippet=snippet,
            jyear=c.jyear,
            jcase_norm=c.jcase_norm,
            jno=c.jno,
            doc_type=c.doc_type,
            is_abbreviated=c.is_abbreviated,
            target_case_type=_extract_target_case_type(c.raw_match),
            auth_type=c.auth_type,
            auth_key=c.auth_key,
            display=c.display,
            needs_intent_signal=c.needs_intent_signal,
        ))
    return results


# ─── 完整 pipeline ────────────────────────────────────────────────────────────

def extract_citations_next(
    clean_text: str,
    *,
    court_root_norm: Optional[str] = None,
    self_key: Optional[tuple] = None,
) -> List[CitationResult]:
    """
    Phase 1 → Phase 2 → Phase 3 完整 pipeline。

    等同舊版 extract_citations，但：
    - 直接在 clean_text 上 regex（不呼叫 preprocess_text）
    - snippet 邏輯更簡潔（無 citation boundary advancement）
    """
    candidates = find_all_candidates(clean_text, court_root_norm=court_root_norm)
    ctx = make_filter_context(
        clean_text,
        self_key=self_key,
        court_root_norm=court_root_norm,
    )
    accepted, _ = filter_candidates(candidates, ctx)
    return build_snippets(accepted, clean_text)
