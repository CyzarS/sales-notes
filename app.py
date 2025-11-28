import os
import time
from decimal import Decimal

import boto3
import requests
from flask import Flask, request, jsonify, Response

from db import get_conn
from metrics import metrics_middleware, metrics_endpoint
from pdf_utils import generar_pdf
from s3_utils import upload_pdf, head_metadata, update_metadata, get_pdf

# ----------------- CloudWatch config -----------------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
ENV = os.getenv("ENV", "local")  # local / production
SERVICE_NAME = "sales-notes"
NAMESPACE = "NotasApp"

cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)


def publish_metrics(duration_ms: float, status_code: int, route: str):
    """
    Publica 2 métricas en CloudWatch:
      - HttpRequestDuration (ms) con dimensiones Service/Env/Route
      - HttpResponses (Count) con dimensiones Service/Env/StatusGroup (2xx/4xx/5xx)
    """
    try:
        status_group = f"{status_code // 100}xx"

        metric_data = [
            {
                "MetricName": "HttpRequestDuration",
                "Dimensions": [
                    {"Name": "Service", "Value": SERVICE_NAME},
                    {"Name": "Env", "Value": ENV},
                    {"Name": "Route", "Value": route},
                ],
                "Unit": "Milliseconds",
                "Value": float(duration_ms),
            },
            {
                "MetricName": "HttpResponses",
                "Dimensions": [
                    {"Name": "Service", "Value": SERVICE_NAME},
                    {"Name": "Env", "Value": ENV},
                    {"Name": "StatusGroup", "Value": status_group},
                ],
                "Unit": "Count",
                "Value": 1.0,
            },
        ]

        cloudwatch.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=metric_data,
        )
    except Exception as e:
        # No tiramos la app por culpa de métricas
        print(f"[METRICS] Error publicando métricas en CloudWatch: {e}")


# ----------------- Flask app -----------------
app = Flask(__name__)
app.wsgi_app = metrics_middleware(app.wsgi_app)

MAIL_NOTIFIER_URL = os.getenv("MAIL_NOTIFIER_URL")


@app.get("/health")
def health():
    # Podrías también medir esto si quieres, pero normalmente el health se deja fuera
    return "ok", 200


@app.get("/metrics")
def metrics():
    data, status, headers = metrics_endpoint()
    return Response(data, status=status, headers=headers)


@app.post("/notas")
def crear_nota():
    start = time.time()
    route = "/notas"
    status_code = 500

    data = request.get_json() or {}
    required = ["cliente_id", "domicilio_facturacion_id", "domicilio_envio_id", "items"]
    if not all(k in data for k in required):
        status_code = 400
        duration_ms = (time.time() - start) * 1000.0
        publish_metrics(duration_ms, status_code, route)
        return jsonify({"error": "Faltan campos requeridos"}), status_code

    conn = get_conn()
    try:
        cur = conn.cursor()
        cliente_id = data["cliente_id"]
        df_id = data["domicilio_facturacion_id"]
        de_id = data["domicilio_envio_id"]
        items = data["items"]

        cur.execute(
            "SELECT id, razon_social, nombre_comercial, rfc, email, telefono FROM clientes WHERE id=%s",
            (cliente_id,),
        )
        row = cur.fetchone()
        if not row:
            status_code = 400
            duration_ms = (time.time() - start) * 1000.0
            publish_metrics(duration_ms, status_code, route)
            return jsonify({"error": "Cliente no encontrado"}), status_code

        cliente = {
            "id": row[0],
            "razon_social": row[1],
            "nombre_comercial": row[2],
            "rfc": row[3],
            "email": row[4],
            "telefono": row[5],
        }

        folio = "FOL-%s-%s" % (cliente_id, os.getpid())

        cur.execute(
            """
            INSERT INTO notas (folio, cliente_id, domicilio_facturacion_id, domicilio_envio_id, total)
            VALUES (%s,%s,%s,%s,%s) RETURNING id, folio
            """,
            (folio, cliente_id, df_id, de_id, Decimal("0.00")),
        )
        nota_row = cur.fetchone()
        nota_id = nota_row[0]

        total = Decimal("0.00")
        items_det = []
        for it in items:
            producto_id = it["producto_id"]
            cantidad = Decimal(str(it.get("cantidad", 1)))
            cur.execute(
                "SELECT id, nombre, precio_base FROM productos WHERE id=%s",
                (producto_id,),
            )
            prow = cur.fetchone()
            if not prow:
                conn.rollback()
                status_code = 400
                duration_ms = (time.time() - start) * 1000.0
                publish_metrics(duration_ms, status_code, route)
                return jsonify(
                    {"error": "Producto %s no encontrado" % producto_id}
                ), status_code
            prod_nombre = prow[1]
            precio_base = Decimal(str(prow[2]))
            precio_unitario = Decimal(str(it.get("precio_unitario", precio_base)))
            importe = cantidad * precio_unitario
            total += importe
            cur.execute(
                """
                INSERT INTO nota_items (nota_id, producto_id, cantidad, precio_unitario, importe)
                VALUES (%s,%s,%s,%s,%s)
                """,
                (nota_id, producto_id, cantidad, precio_unitario, importe),
            )
            items_det.append(
                {
                    "producto_id": producto_id,
                    "producto_nombre": prod_nombre,
                    "cantidad": float(cantidad),
                    "precio_unitario": float(precio_unitario),
                    "importe": float(importe),
                }
            )

        cur.execute("UPDATE notas SET total=%s WHERE id=%s", (total, nota_id))
        cur.execute("SELECT id, folio, total FROM notas WHERE id=%s", (nota_id,))
        nrow = cur.fetchone()
        nota = {"id": nrow[0], "folio": nrow[1], "total": float(nrow[2])}

        pdf_buffer = generar_pdf(cliente, nota, items_det)
        key = "%s/%s.pdf" % (cliente["rfc"], nota["folio"])
        upload_pdf(key, pdf_buffer)

        cur.execute("UPDATE notas SET pdf_s3_key=%s WHERE id=%s", (key, nota_id))
        conn.commit()

        if MAIL_NOTIFIER_URL and cliente.get("email"):
            try:
                requests.post(
                    MAIL_NOTIFIER_URL.rstrip("/") + "/notify",
                    json={
                        "email": cliente["email"],
                        "folio": nota["folio"],
                        "rfc": cliente["rfc"],
                        "s3_key": key,
                    },
                    timeout=5,
                )
            except Exception as ex:
                print("Error llamando a mail-notifier:", ex)

        status_code = 201
        duration_ms = (time.time() - start) * 1000.0
        publish_metrics(duration_ms, status_code, route)
        return (
            jsonify({"id": nota_id, "folio": nota["folio"], "total": float(total)}),
            status_code,
        )
    except Exception as e:
        print("[ERROR crear_nota]", e)
        status_code = 500
        duration_ms = (time.time() - start) * 1000.0
        publish_metrics(duration_ms, status_code, route)
        return jsonify({"error": "Internal Server Error"}), status_code
    finally:
        conn.close()


@app.get("/notas/<folio>")
def obtener_nota(folio):
    start = time.time()
    route = "/notas"  # o "/notas/{folio}" si quieres más fino
    status_code = 500

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT n.id, n.folio, n.total, n.pdf_s3_key,
                   c.id, c.razon_social, c.nombre_comercial, c.rfc, c.email, c.telefono
            FROM notas n
            JOIN clientes c ON n.cliente_id=c.id
            WHERE n.folio=%s
            """,
            (folio,),
        )
        row = cur.fetchone()
        if not row:
            status_code = 404
            duration_ms = (time.time() - start) * 1000.0
            publish_metrics(duration_ms, status_code, route)
            return jsonify({"error": "Nota no encontrada"}), status_code

        nota = {
            "id": row[0],
            "folio": row[1],
            "total": float(row[2]),
            "pdf_s3_key": row[3],
            "cliente": {
                "id": row[4],
                "razon_social": row[5],
                "nombre_comercial": row[6],
                "rfc": row[7],
                "email": row[8],
                "telefono": row[9],
            },
        }
        cur.execute(
            """
            SELECT ni.id, ni.cantidad, ni.precio_unitario, ni.importe, p.nombre
            FROM nota_items ni
            JOIN productos p ON ni.producto_id=p.id
            WHERE ni.nota_id=%s
            """,
            (nota["id"],),
        )
        items = []
        for r in cur.fetchall():
            items.append(
                {
                    "id": r[0],
                    "cantidad": float(r[1]),
                    "precio_unitario": float(r[2]),
                    "importe": float(r[3]),
                    "producto_nombre": r[4],
                }
            )
        nota["items"] = items

        status_code = 200
        duration_ms = (time.time() - start) * 1000.0
        publish_metrics(duration_ms, status_code, route)
        return jsonify(nota), status_code
    except Exception as e:
        print("[ERROR obtener_nota]", e)
        status_code = 500
        duration_ms = (time.time() - start) * 1000.0
        publish_metrics(duration_ms, status_code, route)
        return jsonify({"error": "Internal Server Error"}), status_code
    finally:
        conn.close()


@app.get("/notas/<folio>/download")
def descargar_nota(folio):
    start = time.time()
    route = "/notas/download"
    status_code = 500

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT pdf_s3_key FROM notas WHERE folio=%s", (folio,))
        row = cur.fetchone()
        if not row:
            status_code = 404
            duration_ms = (time.time() - start) * 1000.0
            publish_metrics(duration_ms, status_code, route)
            return jsonify({"error": "Nota no encontrada"}), status_code
        key = row[0]
    finally:
        conn.close()

    meta = head_metadata(key)
    meta["nota-descargada"] = "true"
    update_metadata(key, meta)

    pdf_stream = get_pdf(key)
    status_code = 200
    duration_ms = (time.time() - start) * 1000.0
    publish_metrics(duration_ms, status_code, route)
    return Response(pdf_stream.read(), mimetype="application/pdf")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "3002"))
    app.run(host="0.0.0.0", port=port)