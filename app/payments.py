import requests
import uuid
from .db import get_conn

PAYSTACK_SECRET = "YOUR_PAYSTACK_SECRET"

def initialize_payment(api_key, email):
    ref = str(uuid.uuid4())

    res = requests.post(
        "https://api.paystack.co/transaction/initialize",
        headers={"Authorization": f"Bearer {PAYSTACK_SECRET}"},
        json={
            "email": email,
            "amount": 50000,  # R500 example
            "reference": ref
        }
    ).json()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO payments (id, api_key, reference, status) VALUES (%s,%s,%s,%s)",
                (str(uuid.uuid4()), api_key, ref, "pending")
            )

    return res["data"]["authorization_url"]


def verify_payment(reference):
    res = requests.get(
        f"https://api.paystack.co/transaction/verify/{reference}",
        headers={"Authorization": f"Bearer {PAYSTACK_SECRET}"}
    ).json()

    status = res["data"]["status"]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE payments SET status=%s WHERE reference=%s",
                (status, reference)
            )

            if status == "success":
                cur.execute("""
                    UPDATE users SET is_paid=TRUE
                    WHERE api_key = (
                        SELECT api_key FROM payments WHERE reference=%s
                    )
                """, (reference,))

    return status