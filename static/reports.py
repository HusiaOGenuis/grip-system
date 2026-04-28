from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import uuid

def generate_pdf(dataset_id, analysis, score):
    filename = f"{dataset_id}.pdf"

    c = canvas.Canvas(filename, pagesize=letter)

    c.drawString(100, 750, f"GRIP REPORT: {dataset_id}")
    c.drawString(100, 720, f"Records: {analysis['record_count']}")
    c.drawString(100, 700, f"Missing: {analysis['missing_values']}")
    c.drawString(100, 680, f"Score: {score['completeness_score']}%")
    c.drawString(100, 660, f"Risk: {score['risk_flag']}")

    c.save()

    return filename