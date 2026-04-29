import os
import requests

PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET_KEY")


import os
import requests

PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET_KEY")


def initialize_payment(api_key: str, email: str):
    if not PAYSTACK_SECRET:
        raise Exception("PAYSTACK_NOT_CONFIGURED")

    url = "https://api.paystack.co/transaction/initialize"

    headers = {
        "Authorization": f"Bearer {PAYSTACK_SECRET}",
        "Content-Type": "application/json"
    }

    payload = {
        "email": email,
        "amount": 500000,
        "callback_url": "https://grip-system.onrender.com/verify-payment"
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        raise Exception(f"PAYSTACK_ERROR: {response.text}")

    data = response.json()

    # ✅ RETURN BOTH URL + REFERENCE
    return {
        "payment_url": data["data"]["authorization_url"],
        "reference": data["data"]["reference"]
    }

def verify_payment(reference: str):
    if not PAYSTACK_SECRET:
        return "not_configured"

    url = f"https://api.paystack.co/transaction/verify/{reference}"

    headers = {
        "Authorization": f"Bearer {PAYSTACK_SECRET}"
    }

    try:
        response = requests.get(url, headers=headers)
        data = response.json()

        if response.status_code != 200:
            return "failed"

        return data["data"]["status"]

    except Exception:
        return "failed"