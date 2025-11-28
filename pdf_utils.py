from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

def generar_pdf(cliente, nota, items):
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 50

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, cliente["razon_social"])
    y -= 20
    if cliente.get("nombre_comercial"):
        c.setFont("Helvetica", 12)
        c.drawString(50, y, cliente["nombre_comercial"])
        y -= 20

    c.setFont("Helvetica", 10)
    c.drawString(50, y, "RFC: %s" % cliente["rfc"])
    y -= 15
    c.drawString(50, y, "Correo: %s  Tel: %s" % (cliente.get("email", ""), cliente.get("telefono", "")))
    y -= 25

    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "Folio: %s" % nota["folio"])
    y -= 30

    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "Cantidad")
    c.drawString(120, y, "Producto")
    c.drawString(320, y, "P. Unitario")
    c.drawString(430, y, "Importe")
    y -= 15
    c.setFont("Helvetica", 10)

    for item in items:
        if y < 80:
            c.showPage()
            y = height - 50
        c.drawString(50, y, str(item["cantidad"]))
        c.drawString(120, y, item["producto_nombre"])
        c.drawRightString(400, y, "%.2f" % item["precio_unitario"])
        c.drawRightString(500, y, "%.2f" % item["importe"])
        y -= 15

    y -= 20
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(500, y, "Total: %.2f" % nota["total"])
    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer
