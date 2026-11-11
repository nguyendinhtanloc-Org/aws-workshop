"""GHN Webhook Simulator & Chaos Event Generator (Member 5).

Generates realistic GHN Webhook events, computes HMAC-SHA256 signatures,
and supports chaos scenarios:
- Out-of-order delivery events (stale state arriving after terminal state)
- Delivery retry loops (delivering attempt 1 -> delivery_fail -> delivering attempt 2 -> delivered)
- Clock skew / artificial delays
- Malformed payloads and invalid HMAC signatures for security testing
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import random
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

STATUS_FLOWS = {
    "happy_path": [
        "ready_to_pick",
        "picking",
        "storing",
        "delivering",
        "delivered",
    ],
    "retry_delivery": [
        "ready_to_pick",
        "picking",
        "storing",
        "delivering",
        "delivery_fail",
        "delivering",
        "delivered",
    ],
    "return_to_sender": [
        "ready_to_pick",
        "picking",
        "storing",
        "delivering",
        "delivery_fail",
        "return",
        "returned",
    ],
    "cancelled_early": [
        "ready_to_pick",
        "cancel",
    ],
}

PROVINCES = ["Hồ Chí Minh", "Hà Nội", "Đà Nẵng", "Bình Dương", "Đồng Nai", "Cần Thơ"]


def compute_hmac(body: bytes, secret: str) -> str:
    """Compute HMAC-SHA256 hex digest."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def generate_order_history(
    order_code: str,
    flow_type: str = "happy_path",
    base_time: datetime | None = None,
    inject_clock_skew: bool = False,
) -> list[dict[str, Any]]:
    """Generate a sequence of webhook events for an order following a flow."""
    if base_time is None:
        base_time = datetime.now(UTC) - timedelta(hours=random.randint(12, 48))

    statuses = STATUS_FLOWS.get(flow_type, STATUS_FLOWS["happy_path"])
    events = []
    current_time = base_time
    history_logs = []

    province = random.choice(PROVINCES)
    phone_suffix = f"{random.randint(10000000, 99999999):08d}"
    phone = f"09{phone_suffix}"
    total_fee = random.choice([25000, 32000, 45000, 55000])
    cod_amount = random.choice([0, 150000, 320000, 500000, 1200000])

    for idx, status in enumerate(statuses):
        # Step forward in time (15 mins to 2 hours)
        step_minutes = random.randint(15, 120)
        current_time += timedelta(minutes=step_minutes)

        event_time = current_time
        if inject_clock_skew and idx == len(statuses) - 1:
            # Simulate sender clock running 30 minutes slow
            event_time = current_time - timedelta(minutes=30)

        history_logs.append(
            {
                "status": status,
                "updated_date": event_time.isoformat(),
            }
        )

        event = {
            "OrderCode": order_code,
            "Status": status,
            "CreatedDate": base_time.isoformat(),
            "Time": event_time.isoformat(),
            "Type": "switch_status" if idx > 0 else "create",
            "TotalFee": total_fee,
            "CODAmount": cod_amount,
            "ToName": f"Khách hàng {order_code[-4:]}",
            "ToPhone": phone,
            "ToAddress": f"Số {random.randint(1, 200)} Đường Giải Phóng",
            "ToProvince": province,
            "log": list(history_logs),  # Include full log up to this point
        }
        events.append(event)

    return events


def generate_10k_sample_dataset(output_path: str) -> dict[str, int]:
    """Generate a 10,000 order sample dataset with realistic distribution and >=500 out-of-order anomalies."""
    random.seed(42)
    stats = {
        "happy_path": 0,
        "retry_delivery": 0,
        "return_to_sender": 0,
        "cancelled_early": 0,
        "clock_skew_orders": 0,
        "out_of_order_orders": 0,
    }
    total_events = 0

    weights = [0.70, 0.15, 0.10, 0.05]
    flows = ["happy_path", "retry_delivery", "return_to_sender", "cancelled_early"]

    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(1, 10001):
            order_code = f"GHN{i:06d}"
            flow = random.choices(flows, weights=weights)[0]
            stats[flow] += 1

            # 5% (500 orders) have clock skew; ~6.6% (666 orders, >= 500) arrive out-of-order
            inject_skew = i % 20 == 0
            inject_out_of_order = i % 15 == 0
            if inject_skew:
                stats["clock_skew_orders"] += 1

            events = generate_order_history(
                order_code=order_code,
                flow_type=flow,
                inject_clock_skew=inject_skew,
            )

            if inject_out_of_order and len(events) >= 2:
                # Swap terminal event with prior non-terminal event to simulate out-of-order arrival
                events[-1], events[-2] = events[-2], events[-1]
                stats["out_of_order_orders"] += 1

            for ev in events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                total_events += 1

    return {"orders": 10000, "events": total_events, **stats}


def send_webhook(
    url: str,
    payload: dict[str, Any],
    secret: str = "ghn-webhook-test-secret",
    corrupt_signature: bool = False,
    timeout: float = 5.0,
) -> tuple[int, str]:
    """Send a single webhook HTTP request with HMAC header."""
    body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    sig = compute_hmac(body_bytes, secret)
    if corrupt_signature:
        sig = "invalid_signature_hex_0000000000000000000000000000000000000000"

    headers = {
        "Content-Type": "application/json",
        "X-Ghn-Signature": sig,
        "User-Agent": "GHN-Webhook-Simulator/1.0",
    }

    req = urllib.request.Request(url, data=body_bytes, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception as e:
        return 500, str(e)


def main() -> None:
    parser = argparse.ArgumentParser(description="GHN Webhook Simulator & Chaos Generator")
    subparsers = parser.add_subparsers(dest="command")

    gen_parser = subparsers.add_parser("generate", help="Generate sample 10k orders dataset")
    gen_parser.add_argument("--output", default="data/sample/10k_orders.jsonl")

    send_parser = subparsers.add_parser("send", help="Send simulated webhook requests")
    send_parser.add_argument("--url", required=True, help="Target webhook endpoint URL")
    send_parser.add_argument("--secret", default="ghn-webhook-test-secret")
    send_parser.add_argument("--code", default="GHN999001", help="Order code")
    send_parser.add_argument("--flow", default="retry_delivery", choices=list(STATUS_FLOWS.keys()))
    send_parser.add_argument("--corrupt-sig", action="store_true", help="Send bad signature")
    send_parser.add_argument("--chaos-out-of-order", action="store_true", help="Send late stale event")

    args = parser.parse_args()

    if args.command == "generate":
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        print(f"Generating 10,000 orders to {args.output}...")
        res = generate_10k_sample_dataset(args.output)
        print(f"Done! Stats: {res}")
    elif args.command == "send":
        events = generate_order_history(args.code, flow_type=args.flow)
        if args.chaos_out_of_order and len(events) >= 2:
            # Swap last two events to simulate out-of-order delivery
            print("Injecting Chaos: Swapping terminal event with prior state...")
            events[-1], events[-2] = events[-2], events[-1]

        for idx, ev in enumerate(events, 1):
            print(f"[{idx}/{len(events)}] Sending {ev['OrderCode']} -> {ev['Status']} ({ev['Type']})...")
            code, resp = send_webhook(
                args.url,
                ev,
                secret=args.secret,
                corrupt_signature=args.corrupt_sig,
            )
            print(f"  Response ({code}): {resp}")
            time.sleep(0.1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

