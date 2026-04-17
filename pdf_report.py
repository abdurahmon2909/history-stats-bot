from __future__ import annotations

import os
from datetime import datetime

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


def _safe(val) -> str:
    return str(val or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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

        # Banner balandligi: kerak bo'lsa o'zgartirasiz
        banner_height = 85 * mm

        img = Image(logo_path, width=usable_width, height=banner_height)
        img.hAlign = "CENTER"
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

    start_text = stats["start_dt"].strftime("%Y-%m-%d %H:%M:%S UTC")
    end_text = stats["end_dt"].strftime("%Y-%m-%d %H:%M:%S UTC")
    total_messages = stats["total_messages"]
    users = stats["users"]

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

    summary_data = [
        [
            Paragraph("Faol", style_box_title),
            Paragraph("Yaxshi", style_box_title),
            Paragraph("O'rtacha", style_box_title),
            Paragraph("Qoniqarli", style_box_title),
        ],
        [
            Paragraph(str(category_counts["Faol"]), style_box_value),
            Paragraph(str(category_counts["Yaxshi"]), style_box_value),
            Paragraph(str(category_counts["O'rtacha"]), style_box_value),
            Paragraph(str(category_counts["Qoniqarli"]), style_box_value),
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

    # ===== TOP 3 =====
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

        top3_table = Table(top3_rows, colWidths=[18 * mm, 74 * mm, 28 * mm, 28 * mm, 30 * mm])
        top3_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#123b5d")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#fff3cd")),
            ("BACKGROUND", (0, 2), (-1, 2), colors.HexColor("#e8f0fe")),
            ("BACKGROUND", (0, 3), (-1, 3), colors.HexColor("#fbe9e7")),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
        ]))
        story.append(Paragraph("<b>Eng faol 3 ishtirokchi</b>", styles["Heading3"]))
        story.append(top3_table)
        story.append(Spacer(1, 10))

    # ===== ASOSIY JADVAL =====
    data = [[
        "No",
        "Ism",
        "Username",
        "Xabarlar",
        "Ulush %",
        "Toifa",
    ]]

    for idx, user in enumerate(users, start=1):
        data.append([
            str(idx),
            _safe(user["full_name"]),
            f"@{_safe(user['username'])}" if user["username"] else "-",
            str(user["msg_count"]),
            str(user["share_percent"]),
            _safe(user["category"]),
        ])

    if len(data) == 1:
        data.append(["-", "Ma'lumot topilmadi", "-", "-", "-", "-"])

    table = Table(
        data,
        repeatRows=1,
        colWidths=[12 * mm, 58 * mm, 36 * mm, 22 * mm, 22 * mm, 28 * mm]
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
        base_style.append(("BACKGROUND", (5, row_idx), (5, row_idx), bg))

    table.setStyle(TableStyle(base_style))
    story.append(Paragraph("<b>Batafsil jadval</b>", styles["Heading3"]))
    story.append(table)
    story.append(Spacer(1, 10))

    # ===== FOOTER =====
    story.append(
        Paragraph(
            f"PDF yaratilgan vaqt: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            styles["Italic"],
        )
    )

    doc.build(story)
