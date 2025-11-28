import os
from datetime import datetime
import boto3

_s3 = boto3.client("s3")
BUCKET = os.getenv("S3_BUCKET")

def upload_pdf(key, pdf_buffer):
    if not BUCKET:
        raise RuntimeError("S3_BUCKET no configurado")
    _s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=pdf_buffer.read(),
        ContentType="application/pdf",
        Metadata={
            "hora-envio": datetime.utcnow().isoformat(),
            "nota-descargada": "false",
            "veces-enviado": "1",
        },
    )

def head_metadata(key):
    if not BUCKET:
        raise RuntimeError("S3_BUCKET no configurado")
    resp = _s3.head_object(Bucket=BUCKET, Key=key)
    return resp.get("Metadata", {})

def update_metadata(key, metadata):
    if not BUCKET:
        raise RuntimeError("S3_BUCKET no configurado")
    _s3.copy_object(
        Bucket=BUCKET,
        Key=key,
        CopySource={"Bucket": BUCKET, "Key": key},
        Metadata=metadata,
        MetadataDirective="REPLACE",
        ContentType="application/pdf",
    )

def get_pdf(key):
    if not BUCKET:
        raise RuntimeError("S3_BUCKET no configurado")
    resp = _s3.get_object(Bucket=BUCKET, Key=key)
    return resp["Body"]
