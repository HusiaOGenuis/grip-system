import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

def generate_pdf(csv_path, output_path):
    df = pd.read_csv(csv_path)

    doc = SimpleDocTemplate(output_path)
    styles = getSampleStyleSheet()

    content = []

    content.append(Paragraph("GRIP ANALYSIS REPORT", styles["Title"]))
    content.append(Paragraph(f"Rows: {len(df)}", styles["Normal"]))
    content.append(Paragraph(f"Columns: {', '.join(df.columns)}", styles["Normal"]))

    doc.build(content)