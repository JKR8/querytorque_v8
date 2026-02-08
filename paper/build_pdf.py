#!/usr/bin/env python3
"""Convert the QueryTorque paper stub from LaTeX to a clean PDF using fpdf2."""

import re
import textwrap
from fpdf import FPDF

TEX_PATH = "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/querytorque.tex"
OUT_PATH = "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/querytorque.pdf"


class PaperPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 5, "QueryTorque: Swarm-of-Reasoners for Training-Free SQL Query Optimization", align="C")
        self.ln(8)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")


def strip_tex(text: str) -> str:
    """Strip LaTeX commands and convert to plain text."""
    # Remove comments
    text = re.sub(r'(?<!\\)%.*$', '', text, flags=re.MULTILINE)

    # Remove document class, packages, begin/end document
    text = re.sub(r'\\documentclass\[.*?\]\{.*?\}', '', text)
    text = re.sub(r'\\usepackage\{.*?\}', '', text)
    text = re.sub(r'\\begin\{document\}', '', text)
    text = re.sub(r'\\end\{document\}', '', text)
    text = re.sub(r'\\maketitle', '', text)
    text = re.sub(r'\\bibliographystyle\{.*?\}', '', text)

    # Remove newcommand definitions
    text = re.sub(r'\\newcommand\{.*?\}(\[.*?\])?\{.*?\}', '', text)

    # Handle title
    text = re.sub(r'\\title\{(.*?)\}', r'TITLE: \1', text, flags=re.DOTALL)
    text = re.sub(r'\\author\{(.*?)\}', r'AUTHORS: \1', text, flags=re.DOTALL)
    text = re.sub(r'\\affiliation\{.*?\}', '', text, flags=re.DOTALL)

    # Sections
    text = re.sub(r'\\section\{(.*?)\}', r'\n\n=== \1 ===\n', text)
    text = re.sub(r'\\subsection\{(.*?)\}', r'\n\n--- \1 ---\n', text)
    text = re.sub(r'\\paragraph\{(.*?)\}', r'\n**\1** ', text)

    # Abstract
    text = re.sub(r'\\begin\{abstract\}', '\n=== Abstract ===\n', text)
    text = re.sub(r'\\end\{abstract\}', '\n', text)

    # Environments
    text = re.sub(r'\\begin\{enumerate\}', '', text)
    text = re.sub(r'\\end\{enumerate\}', '', text)
    text = re.sub(r'\\begin\{itemize\}', '', text)
    text = re.sub(r'\\end\{itemize\}', '', text)
    text = re.sub(r'\\item\s*', '  * ', text)

    # Tables - simplified
    text = re.sub(r'\\begin\{table\}\[.*?\]', '\n[TABLE]', text)
    text = re.sub(r'\\end\{table\}', '[/TABLE]\n', text)
    text = re.sub(r'\\begin\{tabular\}\{.*?\}', '', text)
    text = re.sub(r'\\end\{tabular\}', '', text)
    text = re.sub(r'\\centering', '', text)
    text = re.sub(r'\\caption\{(.*?)\}', r'  Table: \1', text, flags=re.DOTALL)
    text = re.sub(r'\\label\{.*?\}', '', text)
    text = re.sub(r'\\small', '', text)
    text = re.sub(r'\\toprule', '  ' + '-' * 70, text)
    text = re.sub(r'\\midrule', '  ' + '-' * 70, text)
    text = re.sub(r'\\bottomrule', '  ' + '-' * 70, text)
    text = re.sub(r'\\\\(\[.*?\])?', '', text)  # line breaks in tables
    text = re.sub(r'\\footnotesize', '', text)
    text = re.sub(r'\\multirow\{.*?\}\{.*?\}\{(.*?)\}', r'\1', text)

    # Listings
    text = re.sub(r'\\begin\{lstlisting\}\[.*?\]', '\n```', text, flags=re.DOTALL)
    text = re.sub(r'\\end\{lstlisting\}', '```\n', text)

    # Bibliography
    text = re.sub(r'\\begin\{thebibliography\}\{.*?\}', '\n=== References ===\n', text)
    text = re.sub(r'\\end\{thebibliography\}', '', text)
    text = re.sub(r'\\bibitem\{(.*?)\}', r'\n[\1] ', text)
    text = re.sub(r'\\newblock\s*', ' ', text)

    # Inline formatting
    text = re.sub(r'\\textbf\{(.*?)\}', r'**\1**', text, flags=re.DOTALL)
    text = re.sub(r'\\texttt\{(.*?)\}', r'`\1`', text, flags=re.DOTALL)
    text = re.sub(r'\\emph\{(.*?)\}', r'_\1_', text, flags=re.DOTALL)
    text = re.sub(r'\\textit\{(.*?)\}', r'_\1_', text, flags=re.DOTALL)
    text = re.sub(r'\\sysname\{\}', 'QueryTorque', text)
    text = re.sub(r'\\TODO\{(.*?)\}', r'[TODO: \1]', text, flags=re.DOTALL)

    # Math
    text = re.sub(r'\$\\times\$', 'x', text)
    text = re.sub(r'\$\\geq\$', '>=', text)
    text = re.sub(r'\$\\leq\$', '<=', text)
    text = re.sub(r'\$\\sim\$', '~', text)
    text = re.sub(r'\$\\in\$', 'in', text)
    text = re.sub(r'\$([^$]+)\$', r'\1', text)  # strip remaining math mode
    text = re.sub(r'\\&', '&', text)
    text = re.sub(r'\\%', '%', text)

    # Citations
    text = re.sub(r'~?\\cite\{(.*?)\}', r'[\1]', text)
    text = re.sub(r'\\url\{(.*?)\}', r'\1', text)
    text = re.sub(r'\\ref\{(.*?)\}', r'[\1]', text)

    # Misc commands
    text = re.sub(r'\\,', ' ', text)
    text = re.sub(r'\\-', '', text)
    text = re.sub(r'\\\\', '\n', text)
    text = re.sub(r'---', ' -- ', text)
    text = re.sub(r"``(.*?)''", r'"\1"', text, flags=re.DOTALL)
    text = re.sub(r'\\rightarrow', '->', text)
    text = re.sub(r'\\to', '->', text)
    text = re.sub(r'\{', '', text)
    text = re.sub(r'\}', '', text)
    text = re.sub(r'~', ' ', text)
    text = re.sub(r'\\[a-zA-Z]+', '', text)  # catch remaining commands

    # Clean up whitespace
    text = re.sub(r'\n{4,}', '\n\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)

    return text.strip()


def build_pdf():
    with open(TEX_PATH, 'r') as f:
        raw = f.read()

    text = strip_tex(raw)

    pdf = PaperPDF(orientation='P', unit='mm', format='A4')
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title page
    pdf.set_font("Helvetica", "B", 18)
    pdf.multi_cell(0, 9, "QueryTorque: Swarm-of-Reasoners\nfor Training-Free SQL\nQuery Optimization", align="C")
    pdf.ln(5)
    pdf.set_font("Helvetica", "I", 11)
    pdf.cell(0, 6, "via Competitive Multi-Worker Generation", align="C")
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, "[Authors TBD]", align="C")
    pdf.ln(5)
    pdf.cell(0, 6, "February 2026 -- Paper Stub / Working Draft", align="C")
    pdf.ln(12)
    pdf.set_draw_color(180, 180, 180)
    pdf.line(20, pdf.get_y(), 190, pdf.get_y())
    pdf.ln(8)

    # Process line by line
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            pdf.ln(3)
            continue

        # Section headers
        if line.startswith('=== ') and line.endswith(' ==='):
            title = line.strip('= ')
            pdf.ln(5)
            pdf.set_font("Helvetica", "B", 14)
            pdf.set_text_color(0, 51, 102)
            pdf.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
            pdf.set_draw_color(0, 51, 102)
            pdf.line(10, pdf.get_y(), 200, pdf.get_y())
            pdf.ln(4)
            pdf.set_text_color(0, 0, 0)
            continue

        if line.startswith('--- ') and line.endswith(' ---'):
            title = line.strip('- ')
            pdf.ln(3)
            pdf.set_font("Helvetica", "B", 11)
            pdf.set_text_color(0, 51, 102)
            pdf.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)
            pdf.set_text_color(0, 0, 0)
            continue

        # Title/author lines (skip - already in title block)
        if line.startswith('TITLE:') or line.startswith('AUTHORS:'):
            continue

        # TODO markers
        if line.startswith('[TODO:'):
            pdf.set_font("Helvetica", "I", 9)
            pdf.set_text_color(200, 80, 80)
            pdf.multi_cell(0, 5, line.encode('latin-1', 'replace').decode('latin-1'))
            pdf.set_text_color(0, 0, 0)
            pdf.ln(1)
            continue

        # Table markers
        if line.startswith('[TABLE]') or line.startswith('[/TABLE]'):
            continue

        # Table separator lines
        if line.strip().startswith('-----'):
            pdf.set_draw_color(180, 180, 180)
            pdf.line(15, pdf.get_y(), 195, pdf.get_y())
            pdf.ln(2)
            continue

        # Code blocks
        if line == '```':
            pdf.ln(1)
            continue

        # Bullet points
        if line.startswith('* '):
            pdf.set_font("Helvetica", "", 9)
            safe = line.encode('latin-1', 'replace').decode('latin-1')
            pdf.set_x(15)
            pdf.multi_cell(185, 5, safe)
            pdf.ln(1)
            continue

        # Bold paragraphs (** at start)
        if line.startswith('**') and '**' in line[2:]:
            bold_end = line.index('**', 2)
            bold_text = line[2:bold_end]
            rest = line[bold_end+2:].strip()

            pdf.ln(2)
            pdf.set_font("Helvetica", "B", 10)
            safe_bold = bold_text.encode('latin-1', 'replace').decode('latin-1')
            pdf.write(5, safe_bold)
            if rest:
                pdf.set_font("Helvetica", "", 9)
                safe_rest = rest.encode('latin-1', 'replace').decode('latin-1')
                pdf.write(5, ' ' + safe_rest)
            pdf.ln(5)
            continue

        # Table caption lines
        if line.strip().startswith('Table:'):
            pdf.set_font("Helvetica", "BI", 9)
            safe = line.encode('latin-1', 'replace').decode('latin-1')
            pdf.multi_cell(0, 5, safe)
            pdf.ln(1)
            continue

        # Table data rows (contain & separators)
        if '&' in line:
            pdf.set_font("Courier", "", 8)
            # Clean up and format
            cells = [c.strip() for c in line.split('&')]
            row = '  |  '.join(cells)
            safe = row.encode('latin-1', 'replace').decode('latin-1')
            pdf.cell(0, 4.5, safe, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(0.5)
            continue

        # Reference entries
        if re.match(r'^\[[\w_]+\]', line):
            pdf.set_font("Helvetica", "", 8)
            safe = line.encode('latin-1', 'replace').decode('latin-1')
            pdf.multi_cell(0, 4, safe)
            pdf.ln(1)
            continue

        # Regular paragraph text
        pdf.set_font("Helvetica", "", 9)
        pdf.set_x(10)  # reset to left margin
        safe = line.encode('latin-1', 'replace').decode('latin-1')
        if len(safe.strip()) > 0:
            pdf.multi_cell(0, 5, safe)

    pdf.output(OUT_PATH)
    print(f"PDF written to {OUT_PATH}")
    print(f"Pages: {pdf.pages_count}")


if __name__ == "__main__":
    build_pdf()
