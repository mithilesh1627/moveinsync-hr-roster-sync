"""Generate MoveInSync Technical Test Report Word Document (.docx)
Combines existing evidence, original screenshots, exact payloads, SQL queries,
benchmark results, and automated test verifications into an enterprise-grade document.
"""

import os
import sys
import docx
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import qn, nsdecls

# Palette
COLOR_PRIMARY_HEX = "1B365D"       # Deep Navy
COLOR_SECONDARY_HEX = "2B6CB0"     # Steel Blue
COLOR_ACCENT_HEX = "319795"        # Teal
COLOR_DARK_TEXT_HEX = "2D3748"     # Charcoal
COLOR_LIGHT_BG_HEX = "F7FAFC"      # Off-white / light slate
COLOR_ALT_ROW_HEX = "F1F5F9"       # Light gray-blue
COLOR_BORDER_HEX = "CBD5E1"        # Slate border
COLOR_PASS_BG_HEX = "E6FFFA"       # Light teal green
COLOR_PASS_TEXT_HEX = "234E52"     # Dark teal green
COLOR_WARN_BG_HEX = "FEFCBF"       # Light yellow
COLOR_WARN_TEXT_HEX = "744210"     # Dark amber

PRIMARY_RGB = RGBColor(0x1B, 0x36, 0x5D)
SECONDARY_RGB = RGBColor(0x2B, 0x6C, 0xB0)
DARK_TEXT_RGB = RGBColor(0x2D, 0x37, 0x48)
MUTED_TEXT_RGB = RGBColor(0x71, 0x80, 0x96)


def set_cell_background(cell, fill_hex):
    tcPr = cell._element.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:val'), 'clear')
    shd.set(qn('w:color'), 'auto')
    shd.set(qn('w:fill'), fill_hex)
    tcPr.append(shd)


def set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
    tcPr = cell._element.get_or_add_tcPr()
    tcMar = OxmlElement('w:tcMar')
    for m, val in [('w:top', top), ('w:bottom', bottom), ('w:left', left), ('w:right', right)]:
        node = OxmlElement(m)
        node.set(qn('w:w'), str(val))
        node.set(qn('w:type'), 'dxa')
        tcMar.append(node)
    tcPr.append(tcMar)


def set_cell_borders(cell, top=None, bottom=None, left=None, right=None):
    tcPr = cell._element.get_or_add_tcPr()
    tcBorders = OxmlElement('w:tcBorders')
    borders = {'w:top': top, 'w:bottom': bottom, 'w:left': left, 'w:right': right}
    for border_name, border_style in borders.items():
        if border_style:
            b = OxmlElement(border_name)
            b.set(qn('w:val'), border_style.get('val', 'single'))
            b.set(qn('w:sz'), str(border_style.get('sz', 4)))
            b.set(qn('w:space'), '0')
            b.set(qn('w:color'), border_style.get('color', COLOR_BORDER_HEX))
            tcBorders.append(b)
        else:
            b = OxmlElement(border_name)
            b.set(qn('w:val'), 'none')
            tcBorders.append(b)
    tcPr.append(tcBorders)


def add_page_number_to_run(run):
    fldChar1 = OxmlElement('w:fldChar')
    fldChar1.set(qn('w:fldCharType'), 'begin')
    instrText = OxmlElement('w:instrText')
    instrText.set(qn('xml:space'), 'preserve')
    instrText.text = 'PAGE'
    fldChar2 = OxmlElement('w:fldChar')
    fldChar2.set(qn('w:fldCharType'), 'separate')
    fldChar3 = OxmlElement('w:fldChar')
    fldChar3.set(qn('w:fldCharType'), 'end')
    r = run._r
    r.append(fldChar1)
    r.append(instrText)
    r.append(fldChar2)
    r.append(fldChar3)


def add_heading_1(doc, text):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(18)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.keep_with_next = True
    run = p.add_run(text)
    run.font.name = 'Calibri'
    run.font.size = Pt(16)
    run.font.bold = True
    run.font.color.rgb = PRIMARY_RGB
    return p


def add_heading_2(doc, text):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(14)
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.keep_with_next = True
    run = p.add_run(text)
    run.font.name = 'Calibri'
    run.font.size = Pt(13)
    run.font.bold = True
    run.font.color.rgb = SECONDARY_RGB
    return p


def add_heading_3(doc, text):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(10)
    p.paragraph_format.space_after = Pt(2)
    p.paragraph_format.keep_with_next = True
    run = p.add_run(text)
    run.font.name = 'Calibri'
    run.font.size = Pt(11)
    run.font.bold = True
    run.font.color.rgb = DARK_TEXT_RGB
    return p


def add_body_p(doc, text, bold_prefix=""):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.line_spacing = 1.15
    if bold_prefix:
        r_pre = p.add_run(bold_prefix)
        r_pre.font.name = 'Calibri'
        r_pre.font.size = Pt(10)
        r_pre.font.bold = True
        r_pre.font.color.rgb = PRIMARY_RGB
    run = p.add_run(text)
    run.font.name = 'Calibri'
    run.font.size = Pt(10)
    run.font.color.rgb = DARK_TEXT_RGB
    return p


def add_bullet_p(doc, text, bold_prefix=""):
    p = doc.add_paragraph(style='List Bullet')
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(3)
    p.paragraph_format.line_spacing = 1.15
    if bold_prefix:
        r_pre = p.add_run(bold_prefix)
        r_pre.font.name = 'Calibri'
        r_pre.font.size = Pt(10)
        r_pre.font.bold = True
        r_pre.font.color.rgb = DARK_TEXT_RGB
    run = p.add_run(text)
    run.font.name = 'Calibri'
    run.font.size = Pt(10)
    run.font.color.rgb = DARK_TEXT_RGB
    return p


def add_callout(doc, text, title="TECHNICAL GUARANTEE & INVARIANT"):
    tbl = doc.add_table(rows=1, cols=1)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    cell = tbl.cell(0, 0)
    cell.width = Inches(6.5)
    set_cell_background(cell, COLOR_LIGHT_BG_HEX)
    set_cell_margins(cell, top=120, bottom=120, left=180, right=140)
    set_cell_borders(cell, left={'val': 'single', 'sz': 24, 'color': COLOR_PRIMARY_HEX},
                           top={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                           bottom={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                           right={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX})
    
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(2)
    run_t = p.add_run(f"📌 {title}: ")
    run_t.font.name = 'Calibri'
    run_t.font.size = Pt(9.5)
    run_t.font.bold = True
    run_t.font.color.rgb = PRIMARY_RGB
    
    run_c = p.add_run(text)
    run_c.font.name = 'Calibri'
    run_c.font.size = Pt(9.5)
    run_c.font.italic = True
    run_c.font.color.rgb = DARK_TEXT_RGB
    doc.add_paragraph().paragraph_format.space_after = Pt(2)


def add_code_block(doc, code_str, title=""):
    tbl = doc.add_table(rows=1, cols=1)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    cell = tbl.cell(0, 0)
    cell.width = Inches(6.5)
    set_cell_background(cell, "F8F9FA")
    set_cell_margins(cell, top=100, bottom=100, left=150, right=150)
    set_cell_borders(cell, left={'val': 'single', 'sz': 6, 'color': COLOR_BORDER_HEX},
                           top={'val': 'single', 'sz': 6, 'color': COLOR_BORDER_HEX},
                           bottom={'val': 'single', 'sz': 6, 'color': COLOR_BORDER_HEX},
                           right={'val': 'single', 'sz': 6, 'color': COLOR_BORDER_HEX})
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after = Pt(0)
    p.paragraph_format.line_spacing = 1.05
    
    if title:
        rt = p.add_run(f"// {title}\n")
        rt.font.name = 'Consolas'
        rt.font.size = Pt(8.5)
        rt.font.bold = True
        rt.font.color.rgb = SECONDARY_RGB

    run = p.add_run(code_str.strip())
    run.font.name = 'Consolas'
    run.font.size = Pt(8.5)
    run.font.color.rgb = RGBColor(0x1A, 0x20, 0x2C)
    doc.add_paragraph().paragraph_format.space_after = Pt(2)


def add_image_with_caption(doc, img_path, caption_text, figure_num, width_in=5.8):
    if not os.path.exists(img_path):
        add_body_p(doc, f"[Image file missing at {img_path}]", bold_prefix="ERROR: ")
        return
    p_img = doc.add_paragraph()
    p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_img.paragraph_format.space_before = Pt(6)
    p_img.paragraph_format.space_after = Pt(2)
    p_img.paragraph_format.keep_with_next = True
    run_img = p_img.add_run()
    run_img.add_picture(img_path, width=Inches(width_in))
    
    p_cap = doc.add_paragraph()
    p_cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_cap.paragraph_format.space_before = Pt(2)
    p_cap.paragraph_format.space_after = Pt(8)
    run_cpre = p_cap.add_run(f"Figure {figure_num}: ")
    run_cpre.font.name = 'Calibri'
    run_cpre.font.size = Pt(9)
    run_cpre.font.bold = True
    run_cpre.font.color.rgb = PRIMARY_RGB
    run_c = p_cap.add_run(caption_text)
    run_c.font.name = 'Calibri'
    run_c.font.size = Pt(9)
    run_c.font.italic = True
    run_c.font.color.rgb = MUTED_TEXT_RGB


def add_metadata_table(doc, rows_data, col_widths=[2.0, 4.5]):
    tbl = doc.add_table(rows=len(rows_data), cols=len(col_widths))
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    for r_idx, row in enumerate(rows_data):
        for c_idx, val in enumerate(row):
            cell = tbl.cell(r_idx, c_idx)
            cell.width = Inches(col_widths[c_idx])
            set_cell_margins(cell, top=60, bottom=60, left=100, right=100)
            if c_idx == 0:
                set_cell_background(cell, COLOR_ALT_ROW_HEX)
                set_cell_borders(cell, top={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                       bottom={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                       left={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                       right={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX})
            else:
                set_cell_background(cell, "FFFFFF")
                set_cell_borders(cell, top={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                       bottom={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                       left={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                       right={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX})
            p = cell.paragraphs[0]
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after = Pt(0)
            p.paragraph_format.line_spacing = 1.05
            run = p.add_run(str(val))
            run.font.name = 'Calibri'
            run.font.size = Pt(9.5)
            if c_idx == 0:
                run.font.bold = True
                run.font.color.rgb = PRIMARY_RGB
            else:
                run.font.color.rgb = DARK_TEXT_RGB
    doc.add_paragraph().paragraph_format.space_after = Pt(2)


def add_styled_table(doc, headers, data_rows, col_widths=None):
    tbl = doc.add_table(rows=len(data_rows) + 1, cols=len(headers))
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl.autofit = False
    
    # Headers
    for c_idx, h in enumerate(headers):
        cell = tbl.cell(0, c_idx)
        if col_widths and c_idx < len(col_widths):
            cell.width = Inches(col_widths[c_idx])
        set_cell_background(cell, COLOR_PRIMARY_HEX)
        set_cell_margins(cell, top=90, bottom=90, left=100, right=100)
        set_cell_borders(cell, top={'val': 'single', 'sz': 6, 'color': COLOR_PRIMARY_HEX},
                               bottom={'val': 'single', 'sz': 6, 'color': COLOR_PRIMARY_HEX},
                               left={'val': 'single', 'sz': 4, 'color': COLOR_PRIMARY_HEX},
                               right={'val': 'single', 'sz': 4, 'color': COLOR_PRIMARY_HEX})
        p = cell.paragraphs[0]
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after = Pt(0)
        run = p.add_run(h)
        run.font.name = 'Calibri'
        run.font.size = Pt(9.5)
        run.font.bold = True
        run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)

    # Data Rows
    for r_idx, row in enumerate(data_rows):
        bg = COLOR_ALT_ROW_HEX if r_idx % 2 == 1 else "FFFFFF"
        for c_idx, val in enumerate(row):
            cell = tbl.cell(r_idx + 1, c_idx)
            if col_widths and c_idx < len(col_widths):
                cell.width = Inches(col_widths[c_idx])
            set_cell_background(cell, bg)
            set_cell_margins(cell, top=60, bottom=60, left=90, right=90)
            set_cell_borders(cell, top={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                   bottom={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                   left={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX},
                                   right={'val': 'single', 'sz': 4, 'color': COLOR_BORDER_HEX})
            p = cell.paragraphs[0]
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after = Pt(0)
            p.paragraph_format.line_spacing = 1.05
            run = p.add_run(str(val))
            run.font.name = 'Calibri'
            run.font.size = Pt(9)
            if "PASS" in str(val):
                run.font.bold = True
                run.font.color.rgb = RGBColor(0x23, 0x4E, 0x52)
            elif "NOT VERIFIED" in str(val):
                run.font.bold = True
                run.font.color.rgb = RGBColor(0x74, 0x42, 0x10)
            elif "FAIL" in str(val):
                run.font.bold = True
                run.font.color.rgb = RGBColor(0xC5, 0x30, 0x30)
            else:
                run.font.color.rgb = DARK_TEXT_RGB
    doc.add_paragraph().paragraph_format.space_after = Pt(3)


def main():
    doc = docx.Document()
    
    # Page setup
    for sec in doc.sections:
        sec.top_margin = Inches(0.8)
        sec.bottom_margin = Inches(0.8)
        sec.left_margin = Inches(0.8)
        sec.right_margin = Inches(0.8)
        sec.different_first_page_header_footer = True
        
        # Setup Header
        header = sec.header
        hp = header.paragraphs[0]
        hp.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        hrun = hp.add_run("MoveInSync HR Roster Sync — Technical Test & Validation Report")
        hrun.font.name = 'Calibri'
        hrun.font.size = Pt(8.5)
        hrun.font.color.rgb = MUTED_TEXT_RGB
        
        # Setup Footer
        footer = sec.footer
        fp = footer.paragraphs[0]
        fp.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        frun1 = fp.add_run("CONFIDENTIAL & PROPRIETARY — MOVEINSYNC ENGINEERING | Page ")
        frun1.font.name = 'Calibri'
        frun1.font.size = Pt(8.5)
        frun1.font.color.rgb = MUTED_TEXT_RGB
        add_page_number_to_run(fp.add_run())

    # =========================================================================
    # COVER PAGE
    # =========================================================================
    p_cov_pre = doc.add_paragraph()
    p_cov_pre.paragraph_format.space_before = Pt(36)
    p_cov_pre.paragraph_format.space_after = Pt(12)
    r_kicker = p_cov_pre.add_run("ENTERPRISE BACKEND ENGINEERING VALIDATION REPORT")
    r_kicker.font.name = 'Calibri'
    r_kicker.font.size = Pt(11)
    r_kicker.font.bold = True
    r_kicker.font.color.rgb = SECONDARY_RGB

    p_cov_title = doc.add_paragraph()
    p_cov_title.paragraph_format.space_before = Pt(0)
    p_cov_title.paragraph_format.space_after = Pt(8)
    r_title = p_cov_title.add_run("MoveInSync HR Roster Sync — Backend Functional Testing and Validation Report")
    r_title.font.name = 'Calibri'
    r_title.font.size = Pt(24)
    r_title.font.bold = True
    r_title.font.color.rgb = PRIMARY_RGB

    p_cov_sub = doc.add_paragraph()
    p_cov_sub.paragraph_format.space_before = Pt(0)
    p_cov_sub.paragraph_format.space_after = Pt(24)
    r_sub = p_cov_sub.add_run("Comprehensive Verification of Idempotency, Out-of-Order Version Fencing, Cross-Domain Field Ownership, Crash Recovery, Safety Brakes, and 200,000-Row Scale Performance")
    r_sub.font.name = 'Calibri'
    r_sub.font.size = Pt(13)
    r_sub.font.color.rgb = DARK_TEXT_RGB

    # Cover Meta Box
    add_metadata_table(doc, [
        ["Project Case Study", "Case Study 5: Idempotent, Out-of-Order, Self-Healing HR Roster Sync"],
        ["Target System", "Python 3.13, FastAPI, SQLAlchemy 2.0 Async, PostgreSQL 16 (Alpine Container)"],
        ["Primary Author & Role", "Principal Backend Engineer, Distributed Systems & Database Reliability Architect"],
        ["Evaluation Date", "October 31, 2026"],
        ["Document Classification", "Formal Technical Interview & Engineering Evaluation Deliverable"],
        ["Automated Test Coverage", "152 of 152 Automated Tests Passing (100% Pass Rate across Unit, Integration, & Scale)"],
        ["Scale Benchmark SLA", "200,000 Records: Processed in 30.24s (SLA < 60.00s) | Peak Memory: 244.92 MB (SLA < 256 MB)"],
        ["Status", "PASSED — Production Ready Release Candidate"]
    ], col_widths=[2.2, 4.3])

    doc.add_page_break()

    # =========================================================================
    # TABLE OF CONTENTS & EXECUTIVE SUMMARY
    # =========================================================================
    add_heading_1(doc, "Executive Summary")
    add_body_p(doc, 
        "This report provides rigorous, evidence-based engineering documentation for the MoveInSync HR Roster Sync engine. "
        "The system solves the challenging problem of synchronizing core workforce data across disparate enterprise domains "
        "(HR identity services vs. Transport Operations logistics) while handling out-of-order event streams, network retries, "
        "partial failures, corrupted nightly exports, and concurrent database operations without external message brokers (pure PostgreSQL 16)."
    )

    add_body_p(doc, 
        "Every claim in this report is strictly grounded in verifiable technical artifacts: original local interactive demonstration logs, "
        "database shell verifications, unaltered screenshots, automated test suites (152 passed tests), and full 200,000-record scale benchmarks. "
        "In compliance with Phase 4 accuracy rules, sensitive secrets have been redacted, exact identifiers are maintained, and untested "
        "third-party network hops are explicitly identified as requiring verification."
    )

    add_heading_2(doc, "High-Level Validation Metrics Summary")
    add_styled_table(doc, 
        ["Validation Dimension", "Documented Count", "Evidence Type", "Conformance Status"],
        [
            ["Locally Demonstrated Test Cases", "6 Scenarios", "Swagger UI Executions & Interactive psql Shells", "100% PASS (6/6)"],
            ["Scale Benchmark Performance", "1 Workload (200k Rows)", "VS Code Shell Benchmark Execution & Profiling", "100% PASS (1/1)"],
            ["Automated Test-Verified Scenarios", "8 Core Invariants", "Pytest Suite (152 Async Tests in 95.39s)", "100% PASS (8/8)"],
            ["Unverified Enterprise Integrations", "1 Integration Path", "Third-Party HR Vendor Webhooks over WAN", "NOT VERIFIED (1/1)"],
            ["Original Screenshots Retained", "8 High-Res Images", "Full Swagger & psql Terminal Visuals", "RETAINED (8/8)"],
            ["Sensitive Secret Redactions", "Bearer JWTs & Secrets", "Sanitized via [REDACTED] Placeholders", "100% COMPLIANT"]
        ],
        col_widths=[2.2, 1.3, 2.0, 1.0]
    )

    add_callout(doc, 
        "Core Rule 5 Categorization: This document explicitly distinguishes between locally demonstrated interactive tests "
        "(retaining original screenshots), previously reported benchmark profiling (demonstrating 200k rows in 30.24s), "
        "automated test-verified reliability invariants (152 passed tests covering real process terminations and split commits), "
        "and unverified third-party enterprise integrations.",
        title="VERIFICATION INTEGRITY DECLARATION"
    )

    # Table of Contents
    add_heading_1(doc, "Table of Contents")
    toc_items = [
        "1. System Architecture & Foundational Reliability Invariants",
        "2. Locally Demonstrated Test Cases (Original Evidence & Screenshots)",
        "   TC-001: Initial Employee Ingestion & Office Code Validation",
        "   TC-002: Duplicate Event Replay — Idempotency Validation",
        "   TC-003: Out-of-Order Version Ingestion — Monotonic Version Fencing (v3 Advance)",
        "   TC-004: Stale Event Rejection — Inversion Protection (v2 Drop)",
        "   TC-005: Transport Administrator Mutation via Dedicated Domain Endpoint",
        "   TC-006: Cross-Domain Field Ownership Protection (HR Update Preserves Transport Attributes)",
        "3. Previously Reported Scale & Performance Benchmark Results",
        "   TC-007: 200,000-Record Streaming Reconciliation Benchmark & Memory Profile",
        "4. Automated Test-Verified Edge Cases & Reliability Invariants",
        "   TC-008: Safety Brake Exact Boundary Enforcement (29.99%, 30.00%, 30.01%)",
        "   TC-009: Operator Authorization & Safety Brake Override Workflow",
        "   TC-010: Snapshot Malformed Row & Cross-Chunk Duplicate Quarantine",
        "   TC-011: Transactional Outbox Commit & Real PostgreSQL Backend Crash Rollback",
        "   TC-012: Ambiguous Commit Outcome Recovery & Resumption without Duplicate Events",
        "   TC-013: Downstream Cab Booking Cancellation & Seat Release on Deactivation",
        "   TC-014: Office Transfer Conflict Flagging for Operations Review",
        "   TC-015: Strict Multi-Tenant Isolation Protection across Operations",
        "5. Functionality Requiring Further Verification & Documentation Gaps",
        "   TC-016: Live Third-Party HR Vendor Webhook Network Ingestion",
        "6. Comprehensive Traceability Matrix & Final Audit Summary"
    ]
    for item in toc_items:
        add_body_p(doc, item)

    doc.add_page_break()

    # =========================================================================
    # SECTION 1: SYSTEM ARCHITECTURE
    # =========================================================================
    add_heading_1(doc, "1. System Architecture & Reliability Invariants")
    add_body_p(doc, 
        "The MoveInSync HR Roster Sync service is constructed on a resilient backend stack comprising Python 3.13, "
        "FastAPI (asynchronous REST controllers), SQLAlchemy 2.0 Async ORM, and PostgreSQL 16. The service is strictly "
        "self-contained without dependencies on external distributed message brokers such as Apache Kafka, RabbitMQ, or Redis. "
        "All ordering, idempotency, event staging, and transactional outbox guarantees are enforced within PostgreSQL's relational engine."
    )

    add_heading_2(doc, "1.1 Component Architecture & Transaction Boundaries")
    add_bullet_p(doc, "High-Throughput Ingestion Controller: Handles event batches via POST /api/v1/roster/events. Uses sha256 payloads and idempotency keys to ensure exactly-once semantics.", bold_prefix="1. Ingestion Pipeline: ")
    add_bullet_p(doc, "Cross-Domain Separation: Employees are split into HR-owned identity fields (name, email, department, status) and Transport-owned fields (pickup points, shift timings, special assistance). Neither domain can mutate the other's fields.", bold_prefix="2. Domain Ownership Invariant: ")
    add_bullet_p(doc, "Monotonic Version Fencing: Every employee record maintains a source_version. Any incoming event with a version <= current database version is discarded or marked stale.", bold_prefix="3. Version Fencing: ")
    add_bullet_p(doc, "Streaming Nightly Reconciliation: Diffing 200,000 records without loading them into Python memory. Ingests via asyncpg binary COPY, calculates diffs via SQL joins, and applies changes in bounded batches.", bold_prefix="4. Reconciliation Engine: ")
    add_bullet_p(doc, "Safety Brake Circuit Breaker: Automatically halts deactivations if missing employees exceed 30.00% or if malformed rows/duplicate IDs make the snapshot unreliable.", bold_prefix="5. Safety Brake: ")
    add_bullet_p(doc, "Transactional Outbox Pattern: Every employee mutation inserts an outbox event in the EXACT SAME PostgreSQL transaction. Background publisher polls with SKIP LOCKED.", bold_prefix="6. Transactional Outbox: ")

    add_heading_2(doc, "1.2 Database Schema & Key Invariants")
    add_body_p(doc, "PostgreSQL 16 enforces structural integrity through composite unique constraints and index optimizations:")
    add_code_block(doc, 
"""-- Core Schema Invariants (PostgreSQL 16)
CREATE TABLE employees (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    employee_id VARCHAR(64) NOT NULL,
    first_name VARCHAR(128) NOT NULL,
    last_name VARCHAR(128) NOT NULL,
    email VARCHAR(255) NOT NULL,
    office_code VARCHAR(32) NOT NULL REFERENCES offices(office_code),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    source_version BIGINT NOT NULL DEFAULT 1,
    source_timestamp TIMESTAMPTZ NOT NULL,
    record_hash VARCHAR(64) NOT NULL,
    -- Transport Domain Fields
    preferred_pickup_point_id VARCHAR(64),
    shift_timing VARCHAR(64),
    special_assistance_required BOOLEAN NOT NULL DEFAULT FALSE,
    special_assistance_notes TEXT,
    transport_review_required BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_employee_id UNIQUE (tenant_id, employee_id)
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_outbox_poll ON outbox_events(status, next_retry_at) WHERE status IN ('PENDING', 'FAILED');""",
        title="Schema Invariants: Composite Unique Key & Outbox Index"
    )

    doc.add_page_break()

    # =========================================================================
    # SECTION 2: LOCALLY DEMONSTRATED TEST CASES
    # =========================================================================
    add_heading_1(doc, "2. Locally Demonstrated Test Cases (Original Evidence & Screenshots)")
    add_body_p(doc, 
        "This section documents the functional test cases originally executed and captured in the core documentation "
        "('Ingest.docx', 'Ordering, Idempotency.docx', 'Ownership Test.docx'). All original screenshots, payloads, HTTP responses, "
        "and database terminal queries are preserved in full fidelity."
    )

    # -------------------------------------------------------------------------
    # TC-001
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-001: Initial Employee Ingestion & Office Code Validation")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-001"],
        ["Test Case Name", "Initial Employee Ingestion with Foreign Key Office Validation"],
        ["Priority & Category", "Critical | Functional / API / Database"],
        ["Verification Method", "Locally Demonstrated (Interactive Swagger UI & psql Terminal)"],
        ["Tenant & Employee ID", "tenant_id: 'tenant-alpha' | employee_id: 'EMP1001'"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that an HR service client can authenticate, validate active office locations, and successfully ingest an initial employee creation event (version 1).", bold_prefix="Objective: ")
    add_body_p(doc, "The HR service generates an authentication token and submits a batch event containing a single 'CREATE' record for employee 'EMP1001' assigned to office 'BLR_01'.", bold_prefix="Scenario: ")
    
    add_body_p(doc, "Authentication Request (POST /api/v1/auth/token):", bold_prefix="Step 1: ")
    add_code_block(doc, 
"""{
  "tenant_id": "tenant-alpha",
  "client_id": "hr-service",
  "client_secret": "[REDACTED]"
}""", title="POST /api/v1/auth/token")

    add_body_p(doc, "Pre-requisite Office Verification in PostgreSQL (psql shell):", bold_prefix="Step 2: ")
    add_code_block(doc, 
"""moveinsync_roster=# SELECT tenant_id, office_code, name, city, is_active FROM offices;
  tenant_id   | office_code |          name          |   city    | is_active                                    
--------------+-------------+------------------------+-----------+-----------
 tenant-alpha | BLR_01      | Bangalore Headquarters | Bangalore | t
(1 row)""", title="Database Verification: Office Validation")

    add_body_p(doc, "Roster Ingestion Payload (POST /api/v1/roster/events):", bold_prefix="Step 3: ")
    add_code_block(doc, 
"""curl -X 'POST' 'http://127.0.0.1:8000/api/v1/roster/events' \\
  -H 'accept: */*' \\
  -H 'Authorization: Bearer [REDACTED_JWT_TOKEN]' \\
  -H 'Content-Type: application/json' \\
  -d '{
  "events": [
    {
      "event_id": "evt-demo-001",
      "event_type": "CREATE",
      "employee_id": "EMP1001",
      "source_version": 1,
      "source_timestamp": "2026-10-31T10:00:00Z",
      "first_name": "Aarav",
      "last_name": "Sharma",
      "email": "aarav.sharma@example.com",
      "phone": "+919876543210",
      "office_code": "BLR_01",
      "department": "Engineering",
      "manager_id": "MGR100",
      "status": "ACTIVE"
    }
  ]
}'""", title="POST /api/v1/roster/events (Create Event)")

    add_body_p(doc, "PostgreSQL Database Row Inspection:", bold_prefix="Step 4: ")
    add_code_block(doc, 
"""moveinsync_roster=# SELECT tenant_id, employee_id, first_name, last_name, office_code, source_version, status
FROM employees WHERE employee_id = 'EMP1001';
  tenant_id   | employee_id | first_name | last_name | office_code | source_version | status 
--------------+-------------+------------+-----------+-------------+----------------+--------
 tenant-alpha | EMP1001     | Aarav      | Sharma    | BLR_01      |              1 | ACTIVE
(1 row)""", title="psql Shell: Employee Record EMP1001 Created")

    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\core_function_test_docs\extracted_images\Ingest_image2.png",
        "PostgreSQL Interactive psql Shell — Office Code Verification and EMP1001 Record Inspection",
        figure_num=1, width_in=5.8
    )

    add_styled_table(doc,
        ["Inspection Point", "Expected Outcome", "Actual Measured Behavior", "Status"],
        [
            ["Office Validation", "office_code 'BLR_01' exists & active", "Matched 'Bangalore Headquarters'", "PASS"],
            ["HTTP Status Code", "200 OK", "200 OK", "PASS"],
            ["Database Persistence", "EMP1001 stored at version 1", "Stored with status 'ACTIVE', v1", "PASS"],
            ["Hash Calculation", "Canonical record_hash stored", "SHA-256 hash stored in DB", "PASS"]
        ], col_widths=[1.8, 2.0, 2.0, 0.7]
    )
    add_body_p(doc, "Proves fundamental ingestion and relational integrity. Inactive or non-existent office codes are rejected before persistence.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-002
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-002: Duplicate Event Replay — Idempotency Validation")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-002"],
        ["Test Case Name", "Duplicate Event Ingestion with Identical Payload (No-Op Invariant)"],
        ["Priority & Category", "Critical | Functional / Idempotency"],
        ["Verification Method", "Locally Demonstrated (Swagger UI Execution & Terminal Output)"],
        ["Event ID & Employee ID", "event_id: 'evt-demo-001' | employee_id: 'EMP1001'"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that resending an identical event payload produces a safe, idempotent no-op response without mutating database state or incrementing version counters.", bold_prefix="Objective: ")
    add_body_p(doc, "The HR ingestion service resends the exact payload for 'evt-demo-001' (version 1, EMP1001).", bold_prefix="Scenario: ")

    add_body_p(doc, "Server Response for Duplicate Payload:", bold_prefix="Actual Response (HTTP 200 OK): ")
    add_code_block(doc, 
"""{
  "tenant_id": "tenant-alpha",
  "total_received": 1,
  "accepted_count": 0,
  "stale_count": 0,
  "duplicate_count": 1,
  "rejected_count": 0,
  "results": [
    {
      "record_index": 0,
      "employee_id": "EMP1001",
      "status": "duplicate_noop",
      "source_version": 1,
      "message": "Duplicate event with identical payload is a no-op"
    }
  ],
  "errors": []
}""", title="HTTP 200 Response: duplicate_noop")

    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\core_function_test_docs\extracted_images\Ingest_image1.png",
        "Swagger UI Response — Duplicate Event Replay Returns Status duplicate_noop (accepted_count: 0, duplicate_count: 1)",
        figure_num=2, width_in=5.6
    )

    add_styled_table(doc,
        ["Inspection Point", "Expected Outcome", "Actual Measured Behavior", "Status"],
        [
            ["HTTP Status Code", "200 OK", "200 OK", "PASS"],
            ["Duplicate Detection", "duplicate_count: 1, accepted: 0", "duplicate_count: 1, accepted: 0", "PASS"],
            ["Status Flag", "'duplicate_noop'", "'duplicate_noop'", "PASS"],
            ["Database State", "EMP1001 remains at version 1", "Preserved; zero extra DB writes", "PASS"]
        ], col_widths=[1.8, 2.0, 2.0, 0.7]
    )
    add_body_p(doc, "In distributed HR integrations, network retries and webhook resends are frequent. Idempotent no-op processing prevents redundant disk I/O and spurious downstream notifications.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-003
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-003: Out-of-Order Version Ingestion — Monotonic Version Fencing (v3 Advance)")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-003"],
        ["Test Case Name", "Out-of-Order Event Ingestion (Version 3 Arrives Ahead of Version 2)"],
        ["Priority & Category", "High | Functional / Ordering / Versioning"],
        ["Verification Method", "Locally Demonstrated (Swagger UI Execution)"],
        ["Source Version", "Current DB Version: 1 | Incoming Version: 3"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that the service successfully applies an update whose source_version jumps ahead of current database version (e.g., version 3 arriving before version 2).", bold_prefix="Objective: ")
    add_body_p(doc, "HR integration sends an UPDATE event with source_version: 3 and department: 'Artificial Intelligence' with idempotency key 'demo-batch-002'.", bold_prefix="Scenario: ")

    add_body_p(doc, "Request Payload:", bold_prefix="Request: ")
    add_code_block(doc, 
"""curl -X 'POST' 'http://127.0.0.1:8000/api/v1/roster/events' \\
  -H 'accept: */*' \\
  -H 'X-Idempotency-Key: demo-batch-002' \\
  -H 'Authorization: Bearer [REDACTED_JWT_TOKEN]' \\
  -H 'Content-Type: application/json' \\
  -d '{
  "events": [
    {
      "event_id": "evt-demo-003",
      "event_type": "UPDATE",
      "employee_id": "EMP1001",
      "source_version": 3,
      "source_timestamp": "2026-10-31T12:00:00Z",
      "first_name": "Aarav",
      "last_name": "Sharma",
      "email": "aarav.updated@example.com",
      "office_code": "BLR_01",
      "department": "Artificial Intelligence",
      "status": "ACTIVE"
    }
  ]
}'""", title="POST /api/v1/roster/events (Version 3 Update)")

    add_body_p(doc, "Actual Server Response:", bold_prefix="Response (HTTP 200 OK): ")
    add_code_block(doc, 
"""{
  "tenant_id": "tenant-alpha",
  "total_received": 1,
  "accepted_count": 1,
  "stale_count": 0,
  "duplicate_count": 0,
  "rejected_count": 0,
  "results": [
    {
      "record_index": 0,
      "employee_id": "EMP1001",
      "status": "accepted",
      "source_version": 3,
      "message": "Employee updated successfully"
    }
  ],
  "errors": []
}""", title="HTTP 200 Response: accepted (Version 3)")

    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\core_function_test_docs\extracted_images\Ordering_Idempotency_image1.png",
        "Swagger UI Execution — Out-of-Order Version 3 Ingestion Accepted and Database Version Advanced",
        figure_num=3, width_in=5.4
    )

    add_styled_table(doc,
        ["Inspection Point", "Expected Outcome", "Actual Measured Behavior", "Status"],
        [
            ["HTTP Status Code", "200 OK", "200 OK", "PASS"],
            ["Accepted Count", "1", "1", "PASS"],
            ["Database Version", "source_version updated from 1 to 3", "Advanced to 3 in employees table", "PASS"],
            ["Department Mutation", "'Artificial Intelligence'", "'Artificial Intelligence'", "PASS"]
        ], col_widths=[1.8, 2.0, 2.0, 0.7]
    )
    add_body_p(doc, "Enterprise message buses and distributed systems frequently deliver events out-of-order. The engine handles version jumps gracefully without blocking on missing intermediate events.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-004
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-004: Stale Event Rejection — Inversion Protection (v2 Drop)")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-004"],
        ["Test Case Name", "Stale Event Ingestion (Version 2 Arrives After Version 3 Already Committed)"],
        ["Priority & Category", "Critical | Functional / Ordering / Version Fencing"],
        ["Verification Method", "Locally Demonstrated (Swagger UI Execution & psql Output)"],
        ["Source Version", "Current DB Version: 3 | Incoming Version: 2 (Stale)"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that when a delayed, older event arrives after a newer version has already been committed, the service discards the older event and refuses to overwrite newer data.", bold_prefix="Objective: ")
    add_body_p(doc, "HR integration sends delayed event 'evt-demo-002' with source_version: 2 and department: 'Operations' with idempotency key 'demo-batch-003'. Current database version is already 3.", bold_prefix="Scenario: ")

    add_body_p(doc, "Request Payload (POST /api/v1/roster/events):", bold_prefix="Request: ")
    add_code_block(doc, 
"""curl -X 'POST' 'http://127.0.0.1:8000/api/v1/roster/events' \\
  -H 'accept: */*' \\
  -H 'X-Idempotency-Key: demo-batch-003' \\
  -H 'Authorization: Bearer [REDACTED_JWT_TOKEN]' \\
  -H 'Content-Type: application/json' \\
  -d '{
  "events": [
    {
      "event_id": "evt-demo-002",
      "event_type": "UPDATE",
      "employee_id": "EMP1001",
      "source_version": 2,
      "source_timestamp": "2026-10-31T11:00:00Z",
      "first_name": "Aarav",
      "last_name": "Sharma",
      "email": "aarav.old@example.com",
      "office_code": "BLR_01",
      "department": "Operations",
      "status": "ACTIVE"
    }
  ]
}'""", title="POST /api/v1/roster/events (Stale Version 2)")

    add_body_p(doc, "Actual Server Response (HTTP 200 OK):", bold_prefix="Response: ")
    add_code_block(doc, 
"""{
  "tenant_id": "tenant-alpha",
  "total_received": 1,
  "accepted_count": 0,
  "stale_count": 1,
  "duplicate_count": 0,
  "rejected_count": 0,
  "results": [
    {
      "record_index": 0,
      "employee_id": "EMP1001",
      "status": "stale_ignored",
      "source_version": 2,
      "message": "Stale version 2 ignored; current version is 3"
    }
  ],
  "errors": []
}""", title="HTTP 200 Response: stale_ignored")

    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\core_function_test_docs\extracted_images\Ordering_Idempotency_image2.png",
        "Swagger UI Response — Stale Event Rejected (stale_count: 1, message: 'Stale version 2 ignored; current version is 3')",
        figure_num=4, width_in=5.4
    )

    add_body_p(doc, "PostgreSQL Database Verification Query:", bold_prefix="Database Verification: ")
    add_code_block(doc, 
"""moveinsync_roster=# SELECT employee_id, email, department, office_code, source_version, status 
FROM employees WHERE employee_id = 'EMP1001';
 employee_id |           email           |       department        | office_code | source_version | status 
-------------+---------------------------+-------------------------+-------------+----------------+--------
 EMP1001     | aarav.updated@example.com | Artificial Intelligence | BLR_01      |              3 | ACTIVE
(1 row)""", title="Database Shell: Record Preserved at Version 3")

    add_styled_table(doc,
        ["Inspection Point", "Expected Outcome", "Actual Measured Behavior", "Status"],
        [
            ["HTTP Status Code", "200 OK", "200 OK", "PASS"],
            ["Stale Flag", "stale_count: 1, accepted_count: 0", "stale_count: 1, accepted_count: 0", "PASS"],
            ["Rejection Reason", "'Stale version 2 ignored; current version is 3'", "Exact match in result message", "PASS"],
            ["Database Isolation", "Record remains at version 3 with 'Artificial Intelligence'", "Confirmed; version 2 changes dropped", "PASS"]
        ], col_widths=[1.8, 2.0, 2.0, 0.7]
    )
    add_body_p(doc, "Prevents time-inversion anomalies. A slow network path delivering a stale department change can never corrupt newer authoritative data.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-005
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-005: Transport Administrator Mutation via Dedicated Domain Endpoint")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-005"],
        ["Test Case Name", "Transport Logistics Field Update via Dedicated Transport API"],
        ["Priority & Category", "High | Functional / Field Ownership / Authorization"],
        ["Verification Method", "Locally Demonstrated (Swagger UI Execution)"],
        ["Authenticated Role", "transport_admin (tenant-alpha)"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that authorized transport administrators can modify transport-owned fields (pickup points, shift timings, special assistance) on existing employee records.", bold_prefix="Objective: ")
    add_body_p(doc, "Transport administrator authenticates and calls PUT /api/v1/transport/employees/EMP1001.", bold_prefix="Scenario: ")

    add_body_p(doc, "Request Payload:", bold_prefix="Request (PUT /api/v1/transport/employees/EMP1001): ")
    add_code_block(doc, 
"""curl -X 'PUT' 'http://127.0.0.1:8000/api/v1/transport/employees/EMP1001' \\
  -H 'accept: */*' \\
  -H 'Authorization: Bearer [REDACTED_JWT_TOKEN]' \\
  -H 'Content-Type: application/json' \\
  -d '{
  "preferred_pickup_point_id": "PICKUP_BLR_001",
  "shift_timing": "09:00-18:00",
  "special_assistance_required": false,
  "special_assistance_notes": null
}'""", title="PUT /api/v1/transport/employees/EMP1001")

    add_body_p(doc, "Actual Server Response:", bold_prefix="Response (HTTP 200 OK): ")
    add_code_block(doc, 
"""{
  "tenant_id": "tenant-alpha",
  "employee_id": "EMP1001",
  "first_name": "Aarav",
  "last_name": "Sharma",
  "email": "aarav.updated@example.com",
  "phone": "+919876543210",
  "office_code": "BLR_01",
  "department": "Artificial Intelligence",
  "manager_id": "MGR100",
  "status": "ACTIVE",
  "preferred_pickup_point_id": "PICKUP_BLR_001",
  "pickup_geofence": null,
  "special_assistance_required": false,
  "special_assistance_notes": null,
  "shift_timing": "09:00-18:00",
  "source_version": 3,
  "transport_review_required": false,
  "created_at": "2026-10-30T23:12:30.855150Z",
  "updated_at": "2026-10-30T23:46:36.441483Z"
}""", title="HTTP 200 Response: Transport Attributes Updated")

    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\core_function_test_docs\extracted_images\Ownership_Test_image1.png",
        "Swagger UI Response — Transport Operations Admin Successfully Mutates Transport-Owned Attributes",
        figure_num=5, width_in=5.4
    )

    add_styled_table(doc,
        ["Inspection Point", "Expected Outcome", "Actual Measured Behavior", "Status"],
        [
            ["HTTP Status Code", "200 OK", "200 OK", "PASS"],
            ["Pickup Point ID", "'PICKUP_BLR_001'", "'PICKUP_BLR_001'", "PASS"],
            ["Shift Timing", "'09:00-18:00'", "'09:00-18:00'", "PASS"],
            ["HR Fields Preserved", "Name, email, and department untouched", "Untouched at 'Artificial Intelligence'", "PASS"]
        ], col_widths=[1.8, 2.0, 2.0, 0.7]
    )
    add_body_p(doc, "Confirms domain separation. Logistics routing depends on pickup geofences and shift schedules that are managed exclusively by transportation desks.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-006
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-006: Cross-Domain Field Ownership Invariant Protection")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-006"],
        ["Test Case Name", "Cross-Domain Invariant: HR Event Update Cannot Overwrite Transport Fields"],
        ["Priority & Category", "Critical | Functional / Data Governance / Invariant"],
        ["Verification Method", "Locally Demonstrated (Swagger UI Execution & VS Code psql Terminal)"],
        ["Target Record", "EMP1001 (Updated to Version 4 by HR Service)"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that when the HR service subsequent updates identity fields (department, email, version), the service strictly preserves transport-owned fields (pickup point, shift timing).", bold_prefix="Objective: ")
    add_body_p(doc, "HR integration submits event 'evt-demo-004' (version 4, department: 'Engineering', email: 'aarav.v4@example.com').", bold_prefix="Scenario: ")

    add_body_p(doc, "HR Update Request Payload:", bold_prefix="Request: ")
    add_code_block(doc, 
"""curl -X 'POST' 'http://127.0.0.1:8000/api/v1/roster/events' \\
  -H 'accept: */*' \\
  -H 'X-Idempotency-Key: demo-batch-004' \\
  -H 'Authorization: Bearer [REDACTED_JWT_TOKEN]' \\
  -H 'Content-Type: application/json' \\
  -d '{
  "events": [
    {
      "event_id": "evt-demo-004",
      "event_type": "UPDATE",
      "employee_id": "EMP1001",
      "source_version": 4,
      "source_timestamp": "2026-10-31T13:00:00Z",
      "first_name": "Aarav",
      "last_name": "Sharma",
      "email": "aarav.v4@example.com",
      "office_code": "BLR_01",
      "department": "Engineering",
      "manager_id": "MGR100",
      "status": "ACTIVE"
    }
  ]
}'""", title="POST /api/v1/roster/events (Version 4 Update)")

    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\core_function_test_docs\extracted_images\Ownership_Test_image2.png",
        "Swagger UI Response — HR Version 4 Event Ingestion Accepted (accepted_count: 1, source_version: 4)",
        figure_num=6, width_in=5.4
    )

    add_body_p(doc, "Database Verification in VS Code Terminal (psql shell):", bold_prefix="Database Verification: ")
    add_code_block(doc, 
"""moveinsync_roster=# SELECT employee_id, email, department, source_version,
  preferred_pickup_point_id, shift_timing, special_assistance_required 
FROM employees WHERE employee_id = 'EMP1001';
 employee_id |        email         | department  | source_version | preferred_pickup_point_id | shift_timing | special_assistance_required 
-------------+----------------------+-------------+----------------+---------------------------+--------------+-----------------------------
 EMP1001     | aarav.v4@example.com | Engineering |              4 | PICKUP_BLR_001            | 09:00-18:00  | f
(1 row)""", title="psql Shell: Transport Fields Preserved Across HR Version 4 Update")

    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\core_function_test_docs\extracted_images\Ownership_Test_image3.png",
        "VS Code Terminal Shell — Database Output Proving HR Update to Version 4 Preserved Transport Fields",
        figure_num=7, width_in=6.0
    )

    add_styled_table(doc,
        ["Field Checked", "Pre-Update Value", "Incoming HR Payload", "Final Database Value", "Status"],
        [
            ["source_version", "3", "4", "4 (Advanced)", "PASS"],
            ["department", "'Artificial Intelligence'", "'Engineering'", "'Engineering' (Updated)", "PASS"],
            ["email", "'aarav.updated@example.com'", "'aarav.v4@example.com'", "'aarav.v4@example.com' (Updated)", "PASS"],
            ["preferred_pickup_point_id", "'PICKUP_BLR_001'", "[OMITTED IN HR EVENT]", "'PICKUP_BLR_001' (PRESERVED)", "PASS"],
            ["shift_timing", "'09:00-18:00'", "[OMITTED IN HR EVENT]", "'09:00-18:00' (PRESERVED)", "PASS"],
            ["special_assistance_required", "false", "[OMITTED IN HR EVENT]", "false (PRESERVED)", "PASS"]
        ], col_widths=[1.5, 1.3, 1.4, 1.6, 0.7]
    )
    add_body_p(doc, "Eliminates cross-domain race conditions. In a naïve ORM model, an HR update with null transport fields would overwrite transport logistics with NULL. MoveInSync's selective column updating protects operational transport assignments.", bold_prefix="Technical Significance: ")

    doc.add_page_break()

    # =========================================================================
    # SECTION 3: SCALE BENCHMARK RESULTS
    # =========================================================================
    add_heading_1(doc, "3. Previously Reported Scale & Performance Benchmark Results")
    add_body_p(doc, 
        "This section documents the formal performance evaluation of the reconciliation engine under 200,000-record "
        "workloads against the official engineering targets (< 60.00s execution time, < 256.00MB peak resident set size)."
    )

    # -------------------------------------------------------------------------
    # TC-007
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-007: 200,000-Record Streaming Reconciliation Benchmark & Memory Profile")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-007"],
        ["Test Case Name", "Full 200,000-Row Nightly Snapshot Streaming Reconciliation"],
        ["Priority & Category", "Critical | Performance / Scale / Memory Boundedness"],
        ["Verification Method", "Previously Reported Benchmark & Terminal Execution Logging"],
        ["Target Scale SLAs", "Execution Time: < 60.00 seconds | Peak Memory: < 256.00 MB RSS"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that the reconciliation engine can diff a complete 200,000-row workforce snapshot against 190,000 baseline active records in strictly < 60s while keeping peak memory bounded (< 256MB).", bold_prefix="Objective: ")
    add_body_p(doc, "Run 'python scripts/benchmark_reconciliation.py 200000' against clean isolated tenant database.", bold_prefix="Scenario: ")

    add_body_p(doc, "Initial Terminal Benchmark Execution (Zero-Change Run):", bold_prefix="Terminal Evidence: ")
    add_image_with_caption(doc, 
        r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\docs\Screenshot\bechmark_reconciliation.png",
        "VS Code Terminal Execution — 200,000-Row Unchanged Reconciliation in 9.26s (Throughput: 21,597 rec/s)",
        figure_num=8, width_in=6.0
    )

    add_heading_3(doc, "Full Multi-Path 200k Benchmark Comparison (Before vs. After Optimization)")
    add_body_p(doc, 
        "Under a comprehensive production distribution (150,000 unchanged, 20,000 created, 20,000 updated, 10,000 stale, and 10,000 missing deactivations), "
        "the engine was profiled and optimized from an initial 81.01s down to 30.24s:"
    )

    add_styled_table(doc,
        ["Pipeline Stage", "Initial Baseline", "Optimized Engine", "Speedup", "Engineering Optimization Applied"],
        [
            ["Stage 1: Staging Ingestion", "44.80 s", "4.35 s", "10.3x", "Replaced 100 chunked SQLAlchemy INSERTs with asyncpg binary copy_records_to_table in 25k chunks"],
            ["Stage 2: Relational Hash Diff", "1.85 s", "0.92 s", "2.0x", "PostgreSQL relational hash join on record_hash, status, and source_version"],
            ["Stage 3: Delta Synchronization", "24.76 s", "21.60 s", "1.15x", "Outer join on (run_id, tenant_id, employee_id). session.expunge_all() evicts ORM instances"],
            ["Stage 4: Missing Deactivations", "9.60 s", "3.30 s", "2.9x", "Replaced ORDER BY id with ORDER BY employee_id matching composite index. 10 atomic batches of 1,000"],
            ["Total Reconciliation Time", "81.01 s", "30.24 s", "2.68x", "50% below official < 60.00s SLA limit"],
            ["Peak RSS Memory", "268.67 MB", "244.92 MB", "Bounded", "Strictly below 256.00 MB limit throughout full execution"]
        ], col_widths=[1.6, 0.9, 0.9, 0.7, 2.4]
    )

    add_body_p(doc, "Memory was continuously sampled every 50ms via psutil. Initial RSS: 94.66 MB; Peak RSS: 244.92 MB; Final RSS: 114.62 MB; Net Delta: +150.27 MB. Memory returns to baseline following GC.", bold_prefix="Memory Sampling Evidence: ")
    add_body_p(doc, "Proves the service can handle enterprise-scale workforces with zero risk of Out-Of-Memory (OOM) termination on resource-constrained containers (e.g. 512MB RAM pods).", bold_prefix="Technical Significance: ")

    doc.add_page_break()

    # =========================================================================
    # SECTION 4: AUTOMATED TEST-VERIFIED INVARIANTS
    # =========================================================================
    add_heading_1(doc, "4. Automated Test-Verified Edge Cases & Reliability Invariants")
    add_body_p(doc, 
        "This section documents the 8 core reliability invariants verified by the automated test suite. "
        "All 152 automated tests in the repository pass with 100% success rate (95.39s total runtime in pytest)."
    )

    # -------------------------------------------------------------------------
    # TC-008
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-008: Safety Brake Exact Boundary Enforcement (29.99%, 30.00%, 30.01%)")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-008"],
        ["Test Case Name", "Safety Brake Missing Threshold Boundary Enforcement"],
        ["Priority & Category", "Critical | Reliability / Safety Brake / Boundary Testing"],
        ["Verification Method", "Automated Pytest (tests/test_p0_safety_brake_and_duplicates.py)"],
        ["Tested Boundaries", "29.99% (2,999 missing), 30.00% (3,000 missing), 30.01% (3,001 missing) on 10k baseline"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that the safety brake triggers if and only if missing active employee percentage >= 30.00%, and that zero automatic deactivations occur on or above 30.00%.", bold_prefix="Objective: ")
    add_body_p(doc, "Seed 10,000 active employees and trigger reconciliations with exact boundary missing percentages.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Boundary Tested", "Missing Count", "Engine Status", "Records Deactivated", "Deactivation Halted?"],
        [
            ["29.99%", "2,999 / 10,000", "COMPLETED", "2,999", "NO (Auto-completed below threshold)"],
            ["30.00%", "3,000 / 10,000", "SAFETY_BRAKE_TRIGGERED", "0", "YES (Halted for operator review)"],
            ["30.01%", "3,001 / 10,000", "SAFETY_BRAKE_TRIGGERED", "0", "YES (Halted for operator review)"]
        ], col_widths=[1.2, 1.2, 1.8, 1.1, 1.2]
    )
    add_body_p(doc, "A corrupted, truncated, or empty HR export file (e.g. from an upstream ETL bug) cannot accidentally trigger mass workforce deactivations.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-009
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-009: Operator Authorization & Safety Brake Override Workflow")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-009"],
        ["Test Case Name", "Operator Role Verification and Bounded Approval Execution"],
        ["Priority & Category", "Critical | Security / Authorization / Approval Lifecycle"],
        ["Verification Method", "Automated Pytest (tests/test_p0_safety_brake_and_duplicates.py)"],
        ["Endpoints Tested", "POST /api/v1/reconciliation/{run_id}/approve"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that regular HR callers cannot approve safety-braked runs (HTTP 403), and that authorized operators trigger deactivation in bounded atomic batches with outbox events.", bold_prefix="Objective: ")
    add_body_p(doc, "1. Unauthorized caller attempts approval -> HTTP 403 Forbidden. 2. Operator approves -> status COMPLETED, 3,000 records deactivated across 3 batches of 1,000, 3,000 outbox records created.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Step", "Role", "Expected HTTP", "Observed Behavior", "Status"],
        [
            ["1. Unauthorized Approve", "hr_integration", "403 Forbidden", "Access denied; run remains SAFETY_BRAKE_TRIGGERED", "PASS"],
            ["2. Authorized Approve", "operator", "200 OK", "Status -> COMPLETED; 3,000 deactivations in 1k batches", "PASS"],
            ["3. Outbox Generation", "System", "3,000 events", "3,000 EMPLOYEE_DEACTIVATED outbox events inserted", "PASS"]
        ], col_widths=[1.5, 1.2, 1.1, 2.0, 0.7]
    )
    add_body_p(doc, "Ensures role separation. Mass personnel deactivations require explicit, audited operator intervention.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-010
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-010: Snapshot Malformed Row & Cross-Chunk Duplicate Quarantine")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-010"],
        ["Test Case Name", "Snapshot Quarantine on Duplicate Employee IDs and Syntax Corruption"],
        ["Priority & Category", "High | Reliability / Data Integrity / Quarantine Policy"],
        ["Verification Method", "Automated Pytest (tests/test_phase1_reliability.py)"],
        ["Target Vulnerability", "Duplicate employee IDs across chunk boundaries (e.g. chunk 1 and chunk 2)"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that duplicate employee IDs across chunk boundaries or malformed syntax halts deactivations into quarantine, even if total missing employees is under 30%.", bold_prefix="Objective: ")
    add_body_p(doc, "Submit snapshot where duplicate IDs appear across chunks with 10% missing employees. Engine halts with SAFETY_BRAKE_TRIGGERED, logs anomaly details, and records 0 deactivations.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Corruption Type", "Missing %", "Action Taken", "Deactivations Applied", "Status"],
        [
            ["Duplicate IDs Across Chunks", "10.00%", "Quarantined for Operator Review", "0 (Deactivations Halted)", "PASS"],
            ["Malformed Email / Syntax", "5.00%", "Quarantined for Operator Review", "0 (Deactivations Halted)", "PASS"]
        ], col_widths=[1.8, 1.0, 2.0, 1.2, 0.5]
    )
    add_body_p(doc, "Duplicate employee IDs in a single file indicate an upstream generation bug. Quarantining protects employee records from ambiguous state overwrites.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-011
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-011: Transactional Outbox Commit & Real PostgreSQL Backend Crash Rollback")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-011"],
        ["Test Case Name", "Atomic Rollback on Mid-Transaction PostgreSQL Connection Drop"],
        ["Priority & Category", "Critical | Distributed Systems / Transaction Atomicity / Failure Recovery"],
        ["Verification Method", "Automated Pytest (tests/test_p0_transaction_atomicity_and_ambiguous_commit.py)"],
        ["Failure Mechanism", "SELECT pg_terminate_backend(pg_backend_pid()) mid-transaction"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Prove that employee updates and outbox events commit on the exact same physical PostgreSQL transaction, and that killing the PostgreSQL backend rolls back both mutations.", bold_prefix="Objective: ")
    add_body_p(doc, "Begin deactivation batch, execute employee update, execute pg_terminate_backend mid-stream. Verify in fresh connection: 0 employees deactivated, 0 outbox events.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Component", "State Before Drop", "State After Drop", "Invariant Check", "Status"],
        [
            ["employees Table", "ACTIVE (v1)", "ACTIVE (v1)", "Zero partial updates committed", "PASS"],
            ["outbox_events Table", "Empty", "Empty", "Zero orphaned events inserted", "PASS"],
            ["reconciliation_runs", "APPROVING", "APPROVING", "Progress counter not incremented", "PASS"]
        ], col_widths=[1.5, 1.2, 1.2, 1.9, 0.7]
    )
    add_body_p(doc, "Guarantees transactional atomicity. System never suffers from 'phantom deactivations' where employees are deactivated in the database but no downstream cancellation event is emitted.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-012
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-012: Ambiguous Commit Outcome Recovery & Resumption")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-012"],
        ["Test Case Name", "Approval Recovery After Process Termination Between Batches"],
        ["Priority & Category", "Critical | Reliability / Crash Recovery / Idempotent Resumption"],
        ["Verification Method", "Automated Pytest (tests/test_p0_transaction_atomicity_and_ambiguous_commit.py)"],
        ["Failure Mechanism", "Process crash after Batch 1 (10 rows) commits; resume approval on Batch 2 (10 rows)"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that when a process terminates mid-approval, resuming the run skips already-committed records and processes only remaining records with zero duplicate outbox events.", bold_prefix="Objective: ")
    add_body_p(doc, "20 employees missing. Batch 1 commits (10 deactivated, 10 outbox). Process crashes. Resumed approval processes remaining 10. Exactly 20 outbox records total exist.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Phase", "Active Employees", "Deactivated Employees", "Outbox Records", "Duplicate Events"],
        [
            ["Initial Pre-Run", "20", "0", "0", "0"],
            ["Post-Batch 1 Crash", "10", "10", "10", "0"],
            ["Resumed Recovery Run", "0", "20", "20", "0 (Zero Duplicates)"]
        ], col_widths=[1.5, 1.2, 1.2, 1.2, 1.4]
    )
    add_body_p(doc, "Achieves durable recovery. Operators can safely retry interrupted approvals without creating duplicate cab cancellation requests.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-013
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-013: Downstream Cab Booking Cancellation & Seat Release on Deactivation")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-013"],
        ["Test Case Name", "Downstream Transport Side Effect Execution and Idempotent Seat Release"],
        ["Priority & Category", "High | Functional / Integration / Downstream Effects"],
        ["Verification Method", "Automated Pytest (tests/test_downstream_effects.py)"],
        ["Target Event", "EMPLOYEE_DEACTIVATED emitted to OutboxPublisher"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that deactivating an employee cancels all future cab bookings, frees occupied vehicle seats, and behaves strictly idempotently upon event redelivery.", bold_prefix="Objective: ")
    add_body_p(doc, "Employee with 2 upcoming cab bookings is deactivated. Outbox publisher dispatches event. Bookings cancelled; vehicle seat availability increments from 2 to 4. Redelivery does not increment seats again.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Stage", "Booking Status", "Occupied Seats", "Available Seats", "Status"],
        [
            ["Pre-Deactivation", "CONFIRMED", "2", "2", "Initial Booking Active"],
            ["Post-Deactivation Event", "CANCELLED", "0", "4 (+2 Seats Freed)", "PASS"],
            ["Replayed Event Delivery", "CANCELLED", "0", "4 (No change)", "PASS (Idempotent)"]
        ], col_widths=[1.5, 1.2, 1.2, 1.5, 1.1]
    )
    add_body_p(doc, "Connects roster changes directly to cost savings. Deactivated employees immediately surrender reserved vehicle seats for other commuters.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-014
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-014: Office Transfer Conflict Flagging for Operations Review")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-014"],
        ["Test Case Name", "Office Transfer Flags transport_review_required Invariant"],
        ["Priority & Category", "Medium | Functional / Domain Invariants"],
        ["Verification Method", "Automated Pytest (tests/test_field_ownership.py)"],
        ["Trigger", "HR office_code change from 'BLR_01' to 'HYD_01'"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that when HR transfers an employee to a new office city, transport_review_required is automatically set to true to force routing reassessment.", bold_prefix="Objective: ")
    add_body_p(doc, "HR event modifies office_code. Record updates office, retains transport fields, and flags transport_review_required = true.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Field", "Old Value", "New Value", "Expected Review Flag", "Actual Review Flag"],
        [
            ["office_code", "'BLR_01' (Bangalore)", "'HYD_01' (Hyderabad)", "true", "true (PASS)"],
            ["preferred_pickup_point_id", "'PICKUP_BLR_001'", "'PICKUP_BLR_001'", "Preserved", "Preserved (PASS)"]
        ], col_widths=[1.8, 1.2, 1.2, 1.2, 1.1]
    )
    add_body_p(doc, "Prevents invalid transport routing. When an employee relocates, existing pickup points in Bangalore are marked for review before cab dispatch in Hyderabad.", bold_prefix="Technical Significance: ")

    # -------------------------------------------------------------------------
    # TC-015
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-015: Strict Multi-Tenant Isolation Protection across Operations")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-015"],
        ["Test Case Name", "Multi-Tenant Data Boundary Protection Across API, DB, and Outbox"],
        ["Priority & Category", "Critical | Security / Tenant Isolation / Data Segregation"],
        ["Verification Method", "Automated Pytest (tests/test_tenant_isolation.py)"],
        ["Tested Boundaries", "Tenant 'tenant-alpha' vs. Tenant 'tenant-beta'"],
        ["Result", "PASS"]
    ])
    add_body_p(doc, "Verify that operations in tenant-alpha can never read, modify, or deactivate employees belonging to tenant-beta.", bold_prefix="Objective: ")
    add_body_p(doc, "Submit reconciliation and batch events for tenant-alpha with employee IDs identical to tenant-beta records. Verify complete segregation in database and outbox publisher.", bold_prefix="Scenario: ")
    add_styled_table(doc,
        ["Isolation Vector", "Test Scenario", "Enforcement Mechanism", "Status"],
        [
            ["Employee Table Isolation", "EMP-001 in alpha vs EMP-001 in beta", "Composite key (tenant_id, employee_id)", "PASS"],
            ["Reconciliation Runs", "Nightly diff for tenant-alpha", "Scaped queries WHERE tenant_id = :tenant_id", "PASS"],
            ["Outbox Publisher", "Event publishing for tenant-alpha", "Partitioned outbox delivery by tenant_id", "PASS"],
            ["Advisory Locks", "Operator approval locking", "hashtext('recon_approve'), hashtext(run_id)", "PASS"]
        ], col_widths=[1.5, 1.6, 2.4, 1.0]
    )
    add_body_p(doc, "Mandatory for enterprise SaaS. Guarantees complete data privacy and operational isolation between competing corporate clients.", bold_prefix="Technical Significance: ")

    doc.add_page_break()

    # =========================================================================
    # SECTION 5: UNVERIFIED FUNCTIONALITY & GAPS
    # =========================================================================
    add_heading_1(doc, "5. Functionality Requiring Further Verification & Documentation Gaps")
    add_body_p(doc, 
        "In strict adherence to Phase 4 accuracy rules, this section explicitly documents features and integration points "
        "that have been implemented in source code and unit-tested via mocks, but have NOT yet been demonstrated with live "
        "external enterprise systems."
    )

    # -------------------------------------------------------------------------
    # TC-016
    # -------------------------------------------------------------------------
    add_heading_2(doc, "TC-016: Live Third-Party HR Vendor Webhook Network Ingestion")
    add_metadata_table(doc, [
        ["Test Case ID", "TC-016"],
        ["Test Case Name", "Live Enterprise HR Webhook Delivery Over Public WAN / VPN"],
        ["Priority & Category", "High | Integration / Network / Third-Party Vendor"],
        ["Verification Method", "Functionality Not Yet Verified (Requires Staging Deployment)"],
        ["Current Evidence Level", "Implemented in code; automated tests pass against httpx ASGITransport mock"],
        ["Result", "NOT VERIFIED (Deployment Required)"]
    ])
    add_body_p(doc, "Verify end-to-end webhook receipt from live third-party enterprise HR software (e.g. Workday, SAP SuccessFactors, BambooHR) over public Internet with enterprise TLS reverse proxy, firewalls, and rate limiting.", bold_prefix="Objective: ")
    add_body_p(doc, "Simulate real-world enterprise deployment with authentic vendor payload shapes and network latency profiles.", bold_prefix="Scenario: ")
    
    add_styled_table(doc,
        ["Inspection Point", "Current Code Status", "Current Test Status", "Verification Gap"],
        [
            ["FastAPI Webhook Controller", "Fully Implemented (app/api/v1/roster.py)", "Passes in test suite (httpx client)", "Untested against live Workday IP ranges"],
            ["TLS Certificate Termination", "Implemented in Docker Compose / Nginx", "Tested via local loopback 127.0.0.1", "Untested with enterprise CA certificates"],
            ["Network Timeout & Retry", "Configured in Settings (timeout=10s)", "Tested via asyncio sleep injection", "Untested with real transcontinental packet loss"]
        ], col_widths=[1.5, 1.6, 1.6, 1.8]
    )
    add_body_p(doc, "The core application and database logic are fully verified. However, enterprise staging deployment with live HR vendor credentials is required to validate network-level firewall rules and third-party webhook signing certificates.", bold_prefix="Technical Significance: ")

    add_heading_2(doc, "Documentation Gaps & Future Recommendations")
    add_bullet_p(doc, "Kafka / Message Queue Adapter: The current system uses pure PostgreSQL transactional outbox. For tenants generating > 50,000 live roster events per second, an external Kafka bridge adapter can be benchmarked.", bold_prefix="1. High-Scale Streaming: ")
    add_bullet_p(doc, "Unlogged Staging Table Configuration: In PostgreSQL 16, setting 'reconciliation_staging' as UNLOGGED could eliminate Write-Ahead Logging (WAL) disk overhead during 1M+ record runs.", bold_prefix="2. Disk I/O Optimization: ")
    add_bullet_p(doc, "Declarative Partitioning: For multi-tenant databases exceeding 10M rows, declarative list partitioning by tenant_id on the employees table will maintain compact B-tree indexes.", bold_prefix="3. Database Partitioning: ")

    doc.add_page_break()

    # =========================================================================
    # SECTION 6: COMPREHENSIVE TRACEABILITY MATRIX
    # =========================================================================
    add_heading_1(doc, "6. Comprehensive Traceability Matrix & Final Audit Summary")
    add_body_p(doc, "The following matrix traces all 16 evaluated scenarios to their architectural components, verification methods, and outcomes:")

    matrix_rows = [
        ["TC-001", "Initial Employee Ingestion", "Roster API", "Locally Demonstrated (psql / Swagger)", "PASS"],
        ["TC-002", "Duplicate Event Replay (No-Op)", "Idempotency Svc", "Locally Demonstrated (Swagger UI)", "PASS"],
        ["TC-003", "Out-of-Order Version Ingestion (v3)", "Ordering Svc", "Locally Demonstrated (Swagger UI)", "PASS"],
        ["TC-004", "Stale Event Rejection (v2 Drop)", "Version Fence", "Locally Demonstrated (psql / Swagger)", "PASS"],
        ["TC-005", "Transport Admin Field Mutation", "Transport API", "Locally Demonstrated (Swagger UI)", "PASS"],
        ["TC-006", "Cross-Domain Field Ownership", "Domain Model", "Locally Demonstrated (psql / Swagger)", "PASS"],
        ["TC-007", "200k Reconciliation Benchmark", "Recon Engine", "Previously Reported Benchmark Log", "PASS"],
        ["TC-008", "Safety Brake Boundaries (29.99-30.01%)", "Safety Brake", "Automated Pytest (10k DB baseline)", "PASS"],
        ["TC-009", "Operator Authorization & Override", "Auth / Recon", "Automated Pytest (RBAC verification)", "PASS"],
        ["TC-010", "Snapshot Quarantine on Duplicate IDs", "Staging Engine", "Automated Pytest (Cross-chunk test)", "PASS"],
        ["TC-011", "PostgreSQL Crash mid-Transaction", "Outbox Repo", "Automated Pytest (pg_terminate_backend)", "PASS"],
        ["TC-012", "Ambiguous Commit Crash Recovery", "Recovery Worker", "Automated Pytest (Batch crash resume)", "PASS"],
        ["TC-013", "Cab Booking Cancellation on Deact", "Booking Adapter", "Automated Pytest (Mock downstream)", "PASS"],
        ["TC-014", "Office Transfer Review Flagging", "Employee Repo", "Automated Pytest (Domain listener)", "PASS"],
        ["TC-015", "Strict Multi-Tenant Isolation", "Security Scope", "Automated Pytest (Cross-tenant queries)", "PASS"],
        ["TC-016", "Live Third-Party HR Webhook Delivery", "Network Ingest", "Functionality Not Yet Verified", "NOT VERIFIED"]
    ]

    add_styled_table(doc,
        ["ID", "Scenario Name", "Component", "Verification Method", "Result"],
        matrix_rows,
        col_widths=[0.6, 2.2, 1.1, 1.9, 0.7]
    )

    add_heading_2(doc, "Final Deliverable Audit Statistics")
    add_styled_table(doc,
        ["Audit Metric", "Count", "Details"],
        [
            ["Total Test Scenarios Documented", "16", "6 Locally Demonstrated, 1 Benchmark, 8 Automated Pytest, 1 Unverified"],
            ["Original Screenshots Retained", "8", "7 from core_function_test_docs/*.docx + 1 benchmark screenshot"],
            ["Scenarios Marked PASS", "15", "All local, benchmark, and automated invariant tests verified with 100% success"],
            ["Scenarios Marked FAIL", "0", "Zero functional defects, regressions, or assertion failures"],
            ["Scenarios Marked NOT VERIFIED", "1", "Live third-party vendor WAN webhook integration (requires staging deployment)"],
            ["Pytest Automated Test Pass Rate", "100%", "152 passed in 95.39s across unit, integration, and crash recovery suites"],
            ["Scale Benchmark Performance SLA", "PASSED", "200,000 records synced in 30.24s (SLA < 60s) | Peak RSS: 244.92 MB (SLA < 256 MB)"]
        ], col_widths=[2.4, 0.8, 3.3]
    )

    output_path = r"D:\2026\MoveInSync\moveinsync-hr-roster-sync\MoveInSync_HR_Roster_Sync_Technical_Test_Report.docx"
    doc.save(output_path)
    print(f"Successfully generated final Word document at: {output_path}")


if __name__ == "__main__":
    main()
