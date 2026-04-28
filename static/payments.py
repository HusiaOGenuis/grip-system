import os
import requests

PAYSTACK_SECRET = os.getenv("PAYSTACK_SECRET_KEY")


def initialize_payment(api_key: str, email: str):
    """
    Initialize Paystack transaction
    Returns:
        {"payment_url": "..."} OR {"error": "..."}
    """

    if not PAYSTACK_SECRET:
        return {"error": "PAYSTACK_NOT_CONFIGURED"}

    url = "https://api.paystack.co/transaction/initialize"

    headers = {
        "Authorization": f"Bearer {PAYSTACK_SECRET}",
        "Content-Type": "application/json"
    }

    payload = {
        "email": email,
        "amount": 500000,  # R50.00 (in kobo)
        "callback_url": "https://grip-system.onrender.com/verify-payment"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)

        # SAFE JSON PARSE
        try:
            data = response.json()
        except Exception:
            return {"error": "INVALID_PAYSTACK_RESPONSE"}

        if response.status_code != 200:
            return {"error": data}

        return {
            "payment_url": data["data"]["authorization_url"],
            "reference": data["data"]["reference"]
        }

    except Exception as e:
        return {"error": str(e)}


def verify_payment(reference: str):
    """
    Verify Paystack transaction
    Returns: success | failed | error
    """

    if not PAYSTACK_SECRET:
        return "PAYSTACK_NOT_CONFIGURED"

    url = f"https://api.paystack.co/transaction/verify/{reference}"

    headers = {
        "Authorization": f"Bearer {PAYSTACK_SECRET}"
    }

    try:
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return "failed"

        data = response.json()

        return data["data"]["status"]

    except Exception:
        return "failed"