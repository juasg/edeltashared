#!/usr/bin/env python3
"""
Generate test DML events on the simulated HANA source table.

Inserts, updates, and deletes rows in SALES_ORDERS which triggers
CDC shadow table writes via Postgres triggers. This simulates
real HANA DML activity for end-to-end pipeline testing.

Usage:
    python scripts/generate-test-data.py [--count 100] [--interval 0.5]
"""

import argparse
import random
import time
import uuid

import psycopg2


def main():
    parser = argparse.ArgumentParser(description="Generate test CDC events")
    parser.add_argument("--count", type=int, default=100, help="Number of events")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between events")
    parser.add_argument("--host", default="localhost", help="Postgres host")
    parser.add_argument("--port", type=int, default=5432, help="Postgres port")
    parser.add_argument("--db", default="edeltashared", help="Database name")
    parser.add_argument("--user", default="edeltashared", help="DB user")
    parser.add_argument("--password", default="changeme_postgres", help="DB password")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.db,
        user=args.user,
        password=args.password,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    statuses = ["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    existing_ids = []

    print(f"Generating {args.count} test events (interval: {args.interval}s)...")

    for i in range(args.count):
        op = random.choices(["INSERT", "UPDATE", "DELETE"], weights=[50, 40, 10])[0]

        if op == "INSERT" or not existing_ids:
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
            customer_id = f"C-{random.randint(1000, 9999)}"
            amount = round(random.uniform(10, 5000), 2)
            status = random.choice(statuses[:3])

            cursor.execute(
                'INSERT INTO "SAPSR3"."SALES_ORDERS" '
                '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
                "VALUES (%s, %s, %s, %s)",
                (order_id, customer_id, amount, status),
            )
            existing_ids.append(order_id)
            print(f"  [{i+1}/{args.count}] INSERT {order_id} amount={amount} status={status}")

        elif op == "UPDATE" and existing_ids:
            order_id = random.choice(existing_ids)
            new_status = random.choice(statuses)
            new_amount = round(random.uniform(10, 5000), 2)

            cursor.execute(
                'UPDATE "SAPSR3"."SALES_ORDERS" '
                'SET "STATUS" = %s, "TOTAL_AMOUNT" = %s, "LAST_MODIFIED" = NOW() '
                'WHERE "ORDER_ID" = %s',
                (new_status, new_amount, order_id),
            )
            print(f"  [{i+1}/{args.count}] UPDATE {order_id} status={new_status}")

        elif op == "DELETE" and existing_ids:
            order_id = existing_ids.pop(random.randint(0, len(existing_ids) - 1))
            cursor.execute(
                'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
                (order_id,),
            )
            print(f"  [{i+1}/{args.count}] DELETE {order_id}")

        if args.interval > 0:
            time.sleep(args.interval)

    # Show shadow table stats
    cursor.execute(
        'SELECT COUNT(*) as total, '
        'COUNT(*) FILTER (WHERE is_consumed = FALSE) as pending '
        'FROM "_EDELTA_CDC_SALES_ORDERS"'
    )
    row = cursor.fetchone()
    print(f"\nShadow table: {row[0]} total, {row[1]} pending")

    cursor.close()
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
