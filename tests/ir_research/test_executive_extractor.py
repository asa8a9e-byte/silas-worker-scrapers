# tests/ir_research/test_executive_extractor.py
from ir_research.executive_extractor import extract_executives_from_text


def test_extract_executives_finds_representative():
    text = """
    【役員の状況】
    代表取締役社長  山田 太郎  1960年4月2日生
    取締役副社長   鈴木 一郎  1965年8月15日生
    常務取締役     佐藤 花子  1970年1月10日生
    社外取締役     田中 健一  1955年3月20日生
    """
    execs = extract_executives_from_text(text)
    assert len(execs) >= 3
    assert any(e.name == "山田 太郎" and "代表取締役" in e.title for e in execs)


def test_extract_executives_assigns_role_category():
    text = (
        "代表取締役  山田 太郎 CEO\n"
        + "\n" * 80
        + "取締役  鈴木 花子 CHRO 人事担当\n"
        + "\n" * 80
        + "取締役  佐藤 一郎 CIO デジタル推進"
    )
    execs = extract_executives_from_text(text)
    ceo = next((e for e in execs if "山田" in e.name), None)
    hr = next((e for e in execs if "鈴木" in e.name), None)
    assert ceo is not None and ceo.role_category == "経営"
    assert hr is not None and hr.role_category == "人事"


def test_extract_executives_empty_text():
    execs = extract_executives_from_text("")
    assert execs == []
