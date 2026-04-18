from __future__ import annotations

import os

from datetime import datetime, timezone

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
    Image,
)
from zoneinfo import ZoneInfo

tashkent_tz = ZoneInfo("Asia/Tashkent")


def _safe(val) -> str:
    return str(val or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def classify_activity_by_percentile(users: list) -> list:
    """
    Foydalanuvchilarni xabarlar soni bo'yicha saralab,
    umumiy foydalanuvchilar sonining:
    - Yuqori 10% -> Faol
    - Keyingi 20% -> Yaxshi
    - Keyingi 30% -> O'rtacha
    - Qolgan 40% -> Qoniqarli
    """
    if not users:
        return users
    
    # Xabarlar soni bo'yicha saralash (kamayish tartibida)
    sorted_users = sorted(users, key=lambda x: x["msg_count"], reverse=True)
    total_users = len(sorted_users)
    
    # Foydalanuvchilar sonining foizlariga qarab chegaralarni hisoblash
    faol_limit = max(1, int(total_users * 0.1))  # Yuqori 10% (kamida 1 ta)
    yaxshi_limit = faol_limit + max(1, int(total_users * 0.2))  # Keyingi 20%
    ortacha_limit = yaxshi_limit + max(1, int(total_users * 0.3))  # Keyingi 30%
    
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
        fontName="Helvetica-Bold",
        fontSize=12,
        textColor=colors.HexColor("#0f7fa8"),
        leading=16,
        spaceAfter=4,
    )

    style_ad = ParagraphStyle(
        "Ad",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=colors.HexColor("#c62828"),
        leading=15,
        spaceAfter=6,
    )

    style_title = ParagraphStyle(
        "TitleCenter",
        parent=styles["Title"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=18,
        textColor=colors.HexColor("#123b5d"),
        leading=22,
        spaceAfter=8,
    )

    style_info = ParagraphStyle(
        "Info",
        parent=styles["Normal"],
        alignment=TA_LEFT,
        fontName="Helvetica",
        fontSize=10,
        textColor=colors.black,
        leading=14,
    )

    style_box_title = ParagraphStyle(
        "BoxTitle",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=11,
        textColor=colors.white,
        leading=14,
    )

    style_box_value = ParagraphStyle(
        "BoxValue",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=14,
        textColor=colors.white,
        leading=18,
    )

    story = []

    # ===== BANNER / LOGO =====
    possible_logo_paths = [
        "logo.png",
        "logo.jpg",
        "logo.jpeg",
        "banner.png",
        "banner.jpg",
        "banner.jpeg",
    ]
    logo_path = next((p for p in possible_logo_paths if os.path.exists(p)), None)

    if logo_path:
        page_width = A4[0]
        usable_width = page_width - doc.leftMargin - doc.rightMargin

        img = Image(logo_path, width=usable_width, height=None)
        img.preserveAspectRatio = True
        img.hAlign = "CENTER"
        
        max_height = 85 * mm
        if img.drawHeight > max_height:
            img.drawHeight = max_height
        
        story.append(img)
        story.append(Spacer(1, 6))

    # ===== KANAL LINKI =====
    story.append(
        Paragraph("https://t.me/Tarixaudiokurs", style_link)
    )

    # ===== REKLAMA MATNI =====
    story.append(
        Paragraph(
            "Natija kerak bo‘lsa, bugunoq kursimizga qo‘shiling! "
            "Murojaat uchun: @Fazliddin_Burxonov",
            style_ad,
        )
    )

    # ===== ASOSIY SARLAVHA =====
    story.append(
        Paragraph(
            f"So‘nggi {period_label} bo‘yicha faollik natijalari",
            style_title,
        )
    )

    start_text = stats["start_dt"].astimezone(tashkent_tz).strftime("%Y-%m-%d %H:%M:%S UTC+5")
    end_text = stats["end_dt"].astimezone(tashkent_tz).strftime("%Y-%m-%d %H:%M:%S UTC+5")
    total_messages = stats["total_messages"]
    users = stats["users"]

    # Foydalanuvchilar sonining foizlariga qarab kategoriyalarni belgilash
    users = classify_activity_by_percentile(users)

    story.append(Paragraph(f"Boshlanish vaqti: <b>{start_text}</b>", style_info))
    story.append(Paragraph(f"Tugash vaqti: <b>{end_text}</b>", style_info))
    story.append(Paragraph(f"Jami xabarlar soni: <b>{total_messages}</b>", style_info))
    story.append(Paragraph(f"Faol foydalanuvchilar soni: <b>{len(users)}</b>", style_info))
    story.append(Spacer(1, 8))

    # ===== TOIFALAR BO'YICHA SUMMARY =====
    category_counts = {
        "Faol": 0,
        "Yaxshi": 0,
        "O'rtacha": 0,
        "Qoniqarli": 0,
    }

    for u in users:
        cat = u.get("category", "Qoniqarli")
        if cat in category_counts:
            category_counts[cat] += 1

    # Kategoriya foizlarini hisoblash
    total_users = len(users)
    faol_percent = (category_counts["Faol"] / total_users * 100) if total_users else 0
    yaxshi_percent = (category_counts["Yaxshi"] / total_users * 100) if total_users else 0
    ortacha_percent = (category_counts["O'rtacha"] / total_users * 100) if total_users else 0
    qoniqarli_percent = (category_counts["Qoniqarli"] / total_users * 100) if total_users else 0

    summary_data = [
        [
            Paragraph("Faol", style_box_title),
            Paragraph("Yaxshi", style_box_title),
            Paragraph("O'rtacha", style_box_title),
            Paragraph("Qoniqarli", style_box_title),
        ],
        [
            Paragraph(f"{category_counts['Faol']} ({faol_percent:.1f}%)", style_box_value),
            Paragraph(f"{category_counts['Yaxshi']} ({yaxshi_percent:.1f}%)", style_box_value),
            Paragraph(f"{category_counts['O\'rtacha']} ({ortacha_percent:.1f}%)", style_box_value),
            Paragraph(f"{category_counts['Qoniqarli']} ({qoniqarli_percent:.1f}%)", style_box_value),
        ]
    ]

    summary_table = Table(summary_data, colWidths=[43 * mm, 43 * mm, 43 * mm, 43 * mm])
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, 1), colors.HexColor("#0b8f55")),
        ("BACKGROUND", (1, 0), (1, 1), colors.HexColor("#1e88e5")),
        ("BACKGROUND", (2, 0), (2, 1), colors.HexColor("#f9a825")),
        ("BACKGROUND", (3, 0), (3, 1), colors.HexColor("#8d6e63")),
        ("BOX", (0, 0), (-1, -1), 0.8, colors.white),
        ("INNERGRID", (0, 0), (-1, -1), 0.8, colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 10))

    # ===== TOP 3 (ranglar bilan) =====
    if users:
        top3 = users[:3]
        top3_rows = [[
            Paragraph("<b>TOP</b>", styles["Normal"]),
            Paragraph("<b>Ism</b>", styles["Normal"]),
            Paragraph("<b>Xabarlar</b>", styles["Normal"]),
            Paragraph("<b>Ulush %</b>", styles["Normal"]),
            Paragraph("<b>Toifa</b>", styles["Normal"]),
        ]]
        medals = ["🥇 1", "🥈 2", "🥉 3"]

        for i, u in enumerate(top3):
            top3_rows.append([
                medals[i],
                _safe(u["full_name"]),
                str(u["msg_count"]),
                str(u["share_percent"]),
                _safe(u["category"]),
            ])

        top3_table = Table(top3_rows, colWidths=[18 * mm, 90 * mm, 28 * mm, 28 * mm, 32 * mm])
        
        # TOP 3 jadvaliga ranglar qo'shish (Batafsil jadvaldagi kabi)
        top3_style = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#123b5d")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
        ]
        
        # Har bir qatorga kategoriyasiga qarab rang berish
        for row_idx, u in enumerate(top3, start=1):
            cat = u["category"]
            if cat == "Faol":
                bg = colors.HexColor("#d9f2e3")
            elif cat == "Yaxshi":
                bg = colors.HexColor("#dbeafe")
            elif cat == "O'rtacha":
                bg = colors.HexColor("#fff4cc")
            else:
                bg = colors.HexColor("#f3e5dc")
            top3_style.append(("BACKGROUND", (0, row_idx), (-1, row_idx), bg))
        
        top3_table.setStyle(TableStyle(top3_style))
        story.append(Paragraph("<b>Eng faol 3 ishtirokchi</b>", styles["Heading3"]))
        story.append(top3_table)
        story.append(Spacer(1, 10))

    # ===== ASOSIY JADVAL =====
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
        data.append(["-", "Ma'lumot topilmadi", "-", "-", "-"])

    table = Table(
        data,
        repeatRows=1,
        colWidths=[12 * mm, 90 * mm, 28 * mm, 28 * mm, 32 * mm]
    )

    base_style = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e78")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 8.8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#eef5fb")]),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]

    # Toifa ustuniga rang berish (5-ustun, indeks 4)
    for row_idx, user in enumerate(users, start=1):
        cat = user["category"]
        if cat == "Faol":
            bg = colors.HexColor("#d9f2e3")
        elif cat == "Yaxshi":
            bg = colors.HexColor("#dbeafe")
        elif cat == "O'rtacha":
            bg = colors.HexColor("#fff4cc")
        else:
            bg = colors.HexColor("#f3e5dc")
        base_style.append(("BACKGROUND", (4, row_idx), (4, row_idx), bg))

    table.setStyle(TableStyle(base_style))
    story.append(Paragraph("<b>Batafsil jadval</b>", styles["Heading3"]))
    story.append(table)
    story.append(Spacer(1, 10))

    # ===== FOOTER =====
    story.append(
        Paragraph(
            f"PDF yaratilgan vaqt: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            styles["Italic"],
        )
    )

    doc.build(story)
