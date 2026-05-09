# ============ pdf_report.py (UNICODE FIX VERSION) ============

from __future__ import annotations

import os

from datetime import datetime, timezone

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
    Image,
)

from zoneinfo import ZoneInfo


# Unicode font (kiril + o‘zbekcha)
pdfmetrics.registerFont(
    UnicodeCIDFont("HeiseiKakuGo-W5")
)

tashkent_tz = ZoneInfo("Asia/Tashkent")


def _safe(val) -> str:
    text = str(val or "")

    replacements = {
        "o‘": "o'",
        "g‘": "g'",
        "O‘": "O'",
        "G‘": "G'",
        "’": "'",
        "`": "'",
        "ʻ": "'",
        "ʼ": "'",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    return (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def classify_activity_by_percentile(users: list) -> list:
    """
    - Yuqori 10% -> Faol
    - Keyingi 20% -> Yaxshi
    - Keyingi 30% -> O'rtacha
    - Qolgan -> Qoniqarli
    """

    if not users:
        return users

    sorted_users = sorted(
        users,
        key=lambda x: x["msg_count"],
        reverse=True
    )

    total_users = len(sorted_users)

    faol_limit = max(1, int(total_users * 0.1))
    yaxshi_limit = faol_limit + max(1, int(total_users * 0.2))
    ortacha_limit = yaxshi_limit + max(1, int(total_users * 0.3))

    for idx, user in enumerate(sorted_users):

        if idx < faol_limit:
            user["category"] = "Faol"

        elif idx < yaxshi_limit:
            user["category"] = "Yaxshi"

        elif idx < ortacha_limit:
            user["category"] = "O'rtacha"

        else:
            user["category"] = "Qoniqarli"

    return sorted_users


def build_pdf_report(stats: dict, period_label: str, file_path: str):

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    doc = SimpleDocTemplate(
        file_path,
        pagesize=A4,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
    )

    styles = getSampleStyleSheet()

    style_link = ParagraphStyle(
        "Link",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontName="HeiseiKakuGo-W5",
        fontSize=12,
        textColor=colors.HexColor("#0f7fa8"),
        leading=16,
        spaceAfter=4,
    )

    style_ad = ParagraphStyle(
        "Ad",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontName="HeiseiKakuGo-W5",
        fontSize=11,
        textColor=colors.HexColor("#c62828"),
        leading=15,
        spaceAfter=6,
    )

    style_title = ParagraphStyle(
        "TitleCenter",
        parent=styles["Title"],
        alignment=TA_CENTER,
        fontName="HeiseiKakuGo-W5",
        fontSize=18,
        textColor=colors.HexColor("#123b5d"),
        leading=22,
        spaceAfter=8,
    )

    style_group_title = ParagraphStyle(
        "GroupTitle",
        parent=styles["Title"],
        alignment=TA_CENTER,
        fontName="HeiseiKakuGo-W5",
        fontSize=16,
        textColor=colors.HexColor("#1a5490"),
        leading=20,
        spaceAfter=6,
    )

    style_info = ParagraphStyle(
        "Info",
        parent=styles["Normal"],
        alignment=TA_LEFT,
        fontName="HeiseiKakuGo-W5",
        fontSize=10,
        textColor=colors.black,
        leading=14,
    )

    story = []

    # ===== LOGO =====

    possible_logo_paths = [
        "logo.png",
        "logo.jpg",
        "logo.jpeg",
        "banner.png",
        "banner.jpg",
        "banner.jpeg",
    ]

    logo_path = next(
        (p for p in possible_logo_paths if os.path.exists(p)),
        None
    )

    if logo_path:

        page_width = A4[0]

        usable_width = (
            page_width
            - doc.leftMargin
            - doc.rightMargin
        )

        img = Image(
            logo_path,
            width=usable_width,
            height=None
        )

        img.preserveAspectRatio = True
        img.hAlign = "CENTER"

        max_height = 120 * mm

        if img.drawHeight > max_height:
            img.drawHeight = max_height

        story.append(img)
        story.append(Spacer(1, 6))

    # ===== KANAL =====

    story.append(
        Paragraph(
            "https://t.me/Tarixaudiokurs",
            style_link
        )
    )

    story.append(
        Paragraph(
            (
                "Natija kerak bo‘lsa, "
                "bugunoq kursimizga qo‘shiling! "
                "Murojaat uchun: @Fazliddin_Burxonov"
            ),
            style_ad,
        )
    )

    # ===== GURUH =====

    group_name = stats.get("group_name", "Guruh")
    group_id = stats.get("group_id", "")

    story.append(
        Paragraph(
            f"🏢 {group_name}",
            style_group_title,
        )
    )

    if group_id:
        story.append(
            Paragraph(
                f"Guruh ID: <b>{group_id}</b>",
                style_info,
            ),
        )

    # ===== TITLE =====

    story.append(
        Paragraph(
            f"So‘nggi {period_label} bo‘yicha faollik natijalari",
            style_title,
        )
    )

    start_text = (
        stats["start_dt"]
        .astimezone(tashkent_tz)
        .strftime("%Y-%m-%d %H:%M:%S UTC+5")
    )

    end_text = (
        stats["end_dt"]
        .astimezone(tashkent_tz)
        .strftime("%Y-%m-%d %H:%M:%S UTC+5")
    )

    total_messages = stats["total_messages"]

    users = classify_activity_by_percentile(
        stats["users"]
    )

    story.append(
        Paragraph(
            f"Boshlanish vaqti: <b>{start_text}</b>",
            style_info,
        )
    )

    story.append(
        Paragraph(
            f"Tugash vaqti: <b>{end_text}</b>",
            style_info,
        )
    )

    story.append(
        Paragraph(
            f"Jami xabarlar soni: <b>{total_messages}</b>",
            style_info,
        )
    )

    story.append(
        Paragraph(
            f"Faol foydalanuvchilar soni: <b>{len(users)}</b>",
            style_info,
        )
    )

    story.append(Spacer(1, 10))

    # ===== TABLE =====

    data = [[
        "No",
        "Ism",
        "Xabarlar",
        "Ulush %",
        "Toifa",
    ]]

    for idx, user in enumerate(users, start=1):

        data.append([
            str(idx),
            _safe(user["full_name"]),
            str(user["msg_count"]),
            str(user["share_percent"]),
            _safe(user["category"]),
        ])

    if len(data) == 1:
        data.append([
            "-",
            "Ma'lumot topilmadi",
            "-",
            "-",
            "-"
        ])

    table = Table(
        data,
        repeatRows=1,
        colWidths=[
            12 * mm,
            90 * mm,
            28 * mm,
            28 * mm,
            32 * mm
        ]
    )

    table.setStyle(TableStyle([

        (
            "BACKGROUND",
            (0, 0),
            (-1, 0),
            colors.HexColor("#1f4e78")
        ),

        (
            "TEXTCOLOR",
            (0, 0),
            (-1, 0),
            colors.white
        ),

        (
            "FONTNAME",
            (0, 0),
            (-1, -1),
            "HeiseiKakuGo-W5"
        ),

        (
            "GRID",
            (0, 0),
            (-1, -1),
            0.4,
            colors.grey
        ),

        (
            "FONTSIZE",
            (0, 0),
            (-1, -1),
            8.8
        ),

        (
            "VALIGN",
            (0, 0),
            (-1, -1),
            "MIDDLE"
        ),

        (
            "ROWBACKGROUNDS",
            (0, 1),
            (-1, -1),
            [
                colors.whitesmoke,
                colors.HexColor("#eef5fb")
            ]
        ),

        (
            "TOPPADDING",
            (0, 0),
            (-1, -1),
            5
        ),

        (
            "BOTTOMPADDING",
            (0, 0),
            (-1, -1),
            5
        ),

    ]))

    story.append(table)

    story.append(Spacer(1, 10))

    # ===== FOOTER =====

    story.append(
        Paragraph(
            (
                "PDF yaratilgan vaqt: "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
            ),
            style_info,
        )
    )

    doc.build(story)
