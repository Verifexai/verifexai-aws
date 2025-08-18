from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime, date

from aws.common.utilities.logger_manager import LoggerManager, PARSING


@dataclass(frozen=True)
class HebrewYMD:
    year: int   # e.g., 5785
    month: int  # 1..13  (1=Tishrei ... 13=Adar II in leap year)
    day: int    # 1..30 (by month)

class HebrewDateUtil:
    """
    Parse a Hebrew date string (e.g., 'ג באב , תשפ"ה') and return
    a Gregorian datetime at midnight.

    Example:
        HebrewDateUtil.parse('ג באב , תשפ"ה')  # -> 2025-07-28 00:00:00
    """

    # --- month names (Tishrei = 1) ---
    _MONTHS = {
        "תשרי": 1,
        "חשון": 2, "חשוון": 2, "מרחשון": 2, "מרחשוון": 2,
        "כסלו": 3,
        "טבת": 4,
        "שבט": 5,
        # Adar handled contextually below
        "ניסן": 7,
        "אייר": 8, "איר": 8,
        "סיון": 9, "סיוון": 9,
        "תמוז": 10,
        "אב": 11,
        "אלול": 12,
        # explicit Adar forms
        "אדר א": 6, "אדר א׳": 6, "אדר ראשון": 6,
        "אדר ב": 13, "אדר ב׳": 13, "אדר שני": 13,
        # plain Adar (decided by leap/common year)
        "אדר": None,
    }

    # gematria map (incl. finals)
    _GEM = {
        'א':1,'ב':2,'ג':3,'ד':4,'ה':5,'ו':6,'ז':7,'ח':8,'ט':9,
        'י':10,'כ':20,'ך':20,'ל':30,'מ':40,'ם':40,'נ':50,'ן':50,
        'ס':60,'ע':70,'פ':80,'ף':80,'צ':90,'ץ':90,
        'ק':100,'ר':200,'ש':300,'ת':400,
    }

    # strip punctuation (incl. gershayim/geresh and backslash from escaped quotes)
    _STRIP = ''.join(['\u05F3','\u05F4', "'", '"', '־', '-', ',', '.', ':', '(', ')', '[', ']', '\\'])

    # Hebrew epoch in Rata Die (fixed day count; RD 1 = 0001-01-01 Gregorian)
    # Chosen to match common civil-date mappings for Israel calendars; validated vs 5785.
    _HEBREW_EPOCH_RD = -1373427

    # ------------ PUBLIC API ------------

    @classmethod
    def parse(cls, hebrew_str: str) -> date:
        ymd = cls._parse_hebrew_string(hebrew_str)
        g = cls._hebrew_to_gregorian(ymd.year, ymd.month, ymd.day)
        return date(g.year, g.month, g.day)

    # ------------ PARSING ------------

    @classmethod
    def _parse_hebrew_string(cls, s: str) -> HebrewYMD:
        s = cls._normalize_spaces(s)
        s_clean = cls._strip_punct(s)
        tokens = cls._smart_tokens(s_clean)

        m_idx, m_len = cls._find_month(tokens)
        if m_idx is None:
            LoggerManager.get_module_logger(PARSING).error(f"לא נמצא חודש חוקי: {s!r}")
            return None

        month_tokens = tokens[m_idx:m_idx+m_len]
        month_name = ' '.join(month_tokens)
        remaining = tokens[:m_idx] + tokens[m_idx+m_len:]

        # day: try first or last remaining token
        day = None
        if remaining:
            day = cls._parse_day_token(remaining[0])
            if day is not None:
                remaining = remaining[1:]
        if day is None and remaining:
            d2 = cls._parse_day_token(remaining[-1])
            if d2 is not None:
                day = d2
                remaining = remaining[:-1]
        if day is None:
            day = 1

        # year: take the LAST non-preposition token only (avoid picking up stray 'ב', 'ל', etc.)
        year = cls._parse_year_tokens(remaining)
        if year is None:
            LoggerManager.get_module_logger(PARSING).error(f"לא נמצאה שנה חוקית: {s!r}")
            return None

        month = cls._month_name_to_number(month_name, year)
        if not (1 <= day <= 30):
            LoggerManager.get_module_logger(PARSING).error(f"יום לא חוקי: {day}")
            return None

        return HebrewYMD(year, month, day)

    @classmethod
    def _normalize_spaces(cls, s: str) -> str:
        s = s.replace('\u200f', ' ').replace('\u200e', ' ')
        return re.sub(r'\s+', ' ', s.strip())

    @classmethod
    def _strip_punct(cls, s: str) -> str:
        return s.translate({ord(c): None for c in cls._STRIP})

    @classmethod
    def _smart_tokens(cls, s: str) -> list[str]:
        # Split leading prepositions from month words: "בתשרי" -> ["ב", "תשרי"]
        out = []
        for p in s.split():
            if len(p) > 1 and p[0] in "בכלמוהש" and cls._looks_like_month(p[1:]):
                out.extend([p[0], p[1:]])
            else:
                out.append(p)
        return out

    @classmethod
    def _looks_like_month(cls, token: str) -> bool:
        t = token.replace("ׁ", "").replace("ּ", "")
        for cand in (t, t + " א", t + " ב"):
            if cand in cls._MONTHS:
                return True
        return False

    @classmethod
    def _find_month(cls, tokens: list[str]):
        # two-token month (e.g., "אדר ב׳")
        for i in range(len(tokens) - 1):
            cand = tokens[i] + " " + tokens[i + 1]
            if cls._month_key_or_none(cand):
                return i, 2
        # single-token (skip lone prepositions)
        for i, tok in enumerate(tokens):
            if len(tok) == 1 and tok in "בכלמוהש":
                if i + 1 < len(tokens) and cls._month_key_or_none(tokens[i + 1]):
                    return i + 1, 1
                continue
            if cls._month_key_or_none(tok):
                return i, 1
        return None, 0

    @classmethod
    def _month_key_or_none(cls, token: str) -> str | None:
        t = token.strip().replace("חשוון", "חשון").replace("מרחשוון", "מרחשון")
        t = re.sub(r"\s+", " ", t)
        return t if t in cls._MONTHS else None

    @classmethod
    def _month_name_to_number(cls, month_token: str, heb_year: int) -> int:
        key = cls._month_key_or_none(month_token)
        if key is None:
            raise ValueError(f"חודש לא חוקי: {month_token!r}")
        explicit = cls._MONTHS[key]
        if explicit is not None:
            # If written as Adar I/II but year is common, coerce to single Adar (month 6)
            if explicit in (6, 13) and not cls._is_hebrew_leap(heb_year):
                return 6
            return explicit
        # Plain "אדר"
        return 13 if cls._is_hebrew_leap(heb_year) else 6

    @classmethod
    def _parse_day_token(cls, token: str) -> int | None:
        if token.isdigit():
            d = int(token)
            return d if 1 <= d <= 30 else None
        v = cls._gematria_value(token)
        return v if 1 <= v <= 30 else None

    @classmethod
    def _parse_year_tokens(cls, tokens: list[str]) -> int | None:
        if not tokens:
            return None
        # Drop stray single-letter prepositions and standalone thousands 'ה'
        filtered = [t for t in tokens if not (len(t) == 1 and t in set("בכלמוהש"))]
        if not filtered:
            return None
        last = filtered[-1]
        if last.isdigit():
            y = int(last)
            return y if 3000 <= y <= 9000 else None
        # Gematria of last token only (avoid absorbing earlier tokens like 'ב')
        y = cls._gematria_value(last)
        if y < 1000:
            y += 5000  # common 5000-prefix heuristic (e.g., תשפ"ה -> 5785)
        return y if 3000 <= y <= 9000 else None

    @classmethod
    def _gematria_value(cls, text: str) -> int:
        t = re.sub(r"[\u0591-\u05C7]", "", text)  # strip niqqud/te'amim
        t = cls._strip_punct(t)
        return sum(cls._GEM.get(ch, 0) for ch in t)

    # ------------ HEBREW CALENDAR MATH ------------

    @staticmethod
    def _is_hebrew_leap(year: int) -> bool:
        # Leap years: years 3,6,8,11,14,17,19 of the 19-year cycle
        return ((7 * year) + 1) % 19 < 7

    @classmethod
    def _hebrew_elapsed_days(cls, year: int) -> int:
        months = (235 * ((year - 1)//19)) + (12 * ((year - 1) % 19)) + (((((year - 1) % 19) * 7) + 1) // 19)
        parts = 204 + (793 * (months % 1080))
        hours = 5 + (12 * months) + (793 * (months // 1080)) + (parts // 1080)
        parts = parts % 1080
        day = 1 + (29 * months) + (hours // 24)
        hours = hours % 24

        # Dechiyyot (postponements)
        if hours >= 18 or (hours == 18 and parts > 0):
            day += 1
        weekday = day % 7  # 0=Sunday
        if weekday in (0, 3, 5):  # Sunday, Wednesday, Friday
            day += 1
            weekday = day % 7
        prev_leap = ((7 * (year - 1) + 1) % 19) < 7
        if (not prev_leap) and (weekday == 2) and ((hours, parts) >= (9, 204)):
            day += 1
        elif prev_leap and (weekday == 1) and ((hours, parts) >= (15, 589)):
            day += 1
        return day

    @classmethod
    def _days_in_hebrew_year(cls, year: int) -> int:
        return cls._hebrew_elapsed_days(year + 1) - cls._hebrew_elapsed_days(year)

    @classmethod
    def _heshvan_kislev_lengths(cls, year: int) -> tuple[int, int]:
        length = cls._days_in_hebrew_year(year)
        if length in (353, 383):  # deficient
            return 29, 29
        if length in (355, 385):  # complete
            return 30, 30
        return 29, 30            # regular

    @classmethod
    def _days_in_hebrew_month(cls, year: int, month: int) -> int:
        # 1=Tishrei, 2=Heshvan, 3=Kislev, 4=Tevet, 5=Shevat,
        # 6=Adar (common) / Adar I (leap),
        # 7=Nisan, 8=Iyar, 9=Sivan, 10=Tamuz, 11=Av, 12=Elul, 13=Adar II (leap)
        if month in (1, 5, 7, 9, 11):    # Tishrei, Shevat, Nisan, Sivan, Av
            return 30
        if month in (4, 8, 10, 12):      # Tevet, Iyar, Tamuz, Elul
            return 29
        if month == 2:                   # Heshvan
            return cls._heshvan_kislev_lengths(year)[0]
        if month == 3:                   # Kislev
            return cls._heshvan_kislev_lengths(year)[1]
        if month == 6:                   # Adar / Adar I
            return 30 if cls._is_hebrew_leap(year) else 29
        if month == 13:                  # Adar II
            return 29 if cls._is_hebrew_leap(year) else 0
        return 29

    # ------------ FIXED↔GREGORIAN ------------

    @staticmethod
    def _is_greg_leap(y: int) -> bool:
        return (y % 4 == 0) and ((y % 100 != 0) or (y % 400 == 0))

    @classmethod
    def _fixed_from_gregorian(cls, y: int, m: int, d: int) -> int:
        return (365*(y-1) + (y-1)//4 - (y-1)//100 + (y-1)//400
                + (367*m - 362)//12
                + (0 if m <= 2 else (-1 if cls._is_greg_leap(y) else -2))
                + d)

    @classmethod
    def _gregorian_from_fixed(cls, n: int) -> date:
        d0 = n - 1
        n400, d1 = divmod(d0, 146097)
        n100 = min(d1 // 36524, 3)
        d2 = d1 - 36524*n100
        n4, d3 = divmod(d2, 1461)
        n1 = min(d3 // 365, 3)
        year = 400*n400 + 100*n100 + 4*n4 + n1 + 1

        day_of_year = n - cls._fixed_from_gregorian(year, 1, 1) + 1
        prior = 0 if n < cls._fixed_from_gregorian(year, 3, 1) else (1 if cls._is_greg_leap(year) else 2)
        month = (12*(day_of_year + prior) + 373) // 367
        day = n - cls._fixed_from_gregorian(year, month, 1) + 1
        return date(year, month, day)

    @classmethod
    def _hebrew_to_gregorian(cls, hyear: int, hmonth: int, hday: int) -> date:
        rd = cls._HEBREW_EPOCH_RD + cls._hebrew_elapsed_days(hyear)
        for m in range(1, hmonth):
            rd += cls._days_in_hebrew_month(hyear, m)
        rd += (hday - 1)
        return cls._gregorian_from_fixed(rd)
