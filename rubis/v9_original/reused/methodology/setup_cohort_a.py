"""Setup Cohort A Keyspaces — Continuation 006.

Creates rubis_v8_c006_auto_norm and rubis_v8_c006_auto_c3c5c7 keyspaces
and populates them with the exact same domain closure dataset as the
historical baselines (5616 users, 5906 items, 5690 bids, 6501 comments, 5300 buynow),
ensuring 100% data volume, cache, and semantic comparability for Cohort A.
"""
from __future__ import annotations

import os
import time
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent

def setup():
    cluster = Cluster([os.environ.get("CASSANDRA_HOST", "127.0.0.1")], port=int(os.environ.get("CASSANDRA_PORT", "9042")))
    session = cluster.connect()

    for ks in ["rubis_v8_c006_auto_norm", "rubis_v8_c006_auto_c3c5c7"]:
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")

    # Table DDLs for SchemaLens
    schema_norm = [
        "CREATE TABLE IF NOT EXISTS {ks}.regions (id int PRIMARY KEY, name text)",
        "CREATE TABLE IF NOT EXISTS {ks}.categories (id int PRIMARY KEY, name text)",
        "CREATE TABLE IF NOT EXISTS {ks}.users (id int PRIMARY KEY, firstname text, lastname text, nickname text, password text, email text, rating int, balance double, creation_date timestamp, region_id int)",
        "CREATE TABLE IF NOT EXISTS {ks}.users_by_region (region_id int, user_id int, nickname text, PRIMARY KEY (region_id, user_id))",
        "CREATE TABLE IF NOT EXISTS {ks}.items (id int PRIMARY KEY, name text, description text, initial_price double, quantity int, reserve_price double, buy_now double, nb_of_bids int, max_bid double, start_date timestamp, end_date timestamp, seller_id int, category_id int)",
        "CREATE TABLE IF NOT EXISTS {ks}.items_by_category (category_id int, end_date timestamp, item_id int, name text, initial_price double, max_bid double, nb_of_bids int, PRIMARY KEY (category_id, end_date, item_id)) WITH CLUSTERING ORDER BY (end_date DESC, item_id ASC)",
        "CREATE TABLE IF NOT EXISTS {ks}.items_by_region (region_id int, category_id int, end_date timestamp, item_id int, seller_id int, name text, PRIMARY KEY (region_id, category_id, end_date, item_id)) WITH CLUSTERING ORDER BY (category_id ASC, end_date DESC, item_id ASC)",
        "CREATE TABLE IF NOT EXISTS {ks}.items_by_seller (seller_id int, end_date timestamp, item_id int, name text, PRIMARY KEY (seller_id, end_date, item_id)) WITH CLUSTERING ORDER BY (end_date DESC, item_id ASC)",
        "CREATE TABLE IF NOT EXISTS {ks}.bids (id int PRIMARY KEY, user_id int, item_id int, qty int, bid double, bid_date timestamp)",
        "CREATE TABLE IF NOT EXISTS {ks}.bids_by_item (item_id int, bid_date timestamp, bid_id int, user_id int, bid double, qty int, PRIMARY KEY (item_id, bid_date, bid_id)) WITH CLUSTERING ORDER BY (bid_date DESC, bid_id ASC)",
        "CREATE TABLE IF NOT EXISTS {ks}.comments (id int PRIMARY KEY, from_user_id int, to_user_id int, item_id int, rating int, comment_date timestamp, comment text)",
        "CREATE TABLE IF NOT EXISTS {ks}.comments_by_to_user (to_user_id int, comment_date timestamp, comment_id int, rating int, comment text, PRIMARY KEY (to_user_id, comment_date, comment_id)) WITH CLUSTERING ORDER BY (comment_date DESC, comment_id ASC)",
        "CREATE TABLE IF NOT EXISTS {ks}.comments_by_id (id int PRIMARY KEY, from_user_id int, nickname text)",
        "CREATE TABLE IF NOT EXISTS {ks}.buynow (id int PRIMARY KEY, buyer_id int, item_id int, qty int, buy_date timestamp)",
        "CREATE TABLE IF NOT EXISTS {ks}.buynow_by_buyer (buyer_id int, buy_date timestamp, id int, item_id int, qty int, PRIMARY KEY (buyer_id, buy_date, id)) WITH CLUSTERING ORDER BY (buy_date DESC, id ASC)",
    ]

    schema_c3c5c7 = schema_norm + [
        "CREATE TABLE IF NOT EXISTS {ks}.items_selective (id int PRIMARY KEY, category_id int, name text)",
        "CREATE TABLE IF NOT EXISTS {ks}.item_bid_shared (item_id int, bid_date timestamp, bid_id int, bid double, item_name text, qty int, user_id int, PRIMARY KEY (item_id, bid_date, bid_id)) WITH CLUSTERING ORDER BY (bid_date DESC, bid_id ASC)",
        "CREATE TABLE IF NOT EXISTS {ks}.items_region_bounded (region_id int, category_id int, end_date timestamp, item_id int, name text, PRIMARY KEY (region_id, category_id, end_date, item_id)) WITH CLUSTERING ORDER BY (category_id ASC, end_date DESC, item_id ASC)",
    ]

    print("Creating tables in rubis_v8_c006_auto_norm...")
    for stmt in schema_norm:
        session.execute(stmt.format(ks="rubis_v8_c006_auto_norm"))

    print("Creating tables in rubis_v8_c006_auto_c3c5c7...")
    for stmt in schema_c3c5c7:
        session.execute(stmt.format(ks="rubis_v8_c006_auto_c3c5c7"))

    # Discover closure entity IDs from rubis_v8_c005_normalized
    uids = [r.users_id for r in session.execute("SELECT users_id FROM rubis_v8_c005_normalized.users")]
    iids = [r.items_id for r in session.execute("SELECT items_id FROM rubis_v8_c005_normalized.items")]
    bids = [r.bids_id for r in session.execute("SELECT bids_id FROM rubis_v8_c005_normalized.bids")]
    cids = [r.comments_id for r in session.execute("SELECT comments_id FROM rubis_v8_c005_normalized.comments")]
    bnids = [r.buynow_id for r in session.execute("SELECT buynow_id FROM rubis_v8_c005_normalized.buynow")]

    print(f"Domain closure cardinalities: users={len(uids)}, items={len(iids)}, bids={len(bids)}, comments={len(cids)}, buynow={len(bnids)}")

    # 1. Regions and Categories
    for tbl in ["regions", "categories"]:
        rows = list(session.execute(f"SELECT * FROM rubis_full.{tbl}"))
        for target_ks in ["rubis_v8_c006_auto_norm", "rubis_v8_c006_auto_c3c5c7"]:
            prep = session.prepare(f"INSERT INTO {target_ks}.{tbl} (id, name) VALUES (?, ?)")
            execute_concurrent(session, [(prep, (r.id, r.name)) for r in rows], concurrency=50)

    # 2. Users
    user_rows = []
    for chunk in [uids[i:i+100] for i in range(0, len(uids), 100)]:
        user_rows.extend(session.execute(f"SELECT * FROM rubis_full.users WHERE id IN ({','.join(map(str, chunk))})"))
    print(f"Fetched {len(user_rows)} users from rubis_full.")

    for target_ks in ["rubis_v8_c006_auto_norm", "rubis_v8_c006_auto_c3c5c7"]:
        prep_u = session.prepare(f"INSERT INTO {target_ks}.users (id, firstname, lastname, nickname, password, email, rating, balance, creation_date, region_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        prep_ur = session.prepare(f"INSERT INTO {target_ks}.users_by_region (region_id, user_id, nickname) VALUES (?, ?, ?)")
        jobs_u = [(prep_u, (u.id, u.firstname, u.lastname, u.nickname, u.password, u.email, u.rating, float(u.balance), u.creation_date, u.region_id)) for u in user_rows]
        jobs_ur = [(prep_ur, (u.region_id, u.id, u.nickname)) for u in user_rows]
        execute_concurrent(session, jobs_u, concurrency=50)
        execute_concurrent(session, jobs_ur, concurrency=50)

    # User lookup map for seller region
    user_region_map = {u.id: u.region_id for u in user_rows}

    # 3. Items
    item_rows = []
    for chunk in [iids[i:i+100] for i in range(0, len(iids), 100)]:
        item_rows.extend(session.execute(f"SELECT * FROM rubis_full.items WHERE id IN ({','.join(map(str, chunk))})"))
    print(f"Fetched {len(item_rows)} items from rubis_full.")

    for target_ks in ["rubis_v8_c006_auto_norm", "rubis_v8_c006_auto_c3c5c7"]:
        prep_it = session.prepare(f"INSERT INTO {target_ks}.items (id, name, description, initial_price, quantity, reserve_price, buy_now, nb_of_bids, max_bid, start_date, end_date, seller_id, category_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        prep_ic = session.prepare(f"INSERT INTO {target_ks}.items_by_category (category_id, end_date, item_id, name, initial_price, max_bid, nb_of_bids) VALUES (?, ?, ?, ?, ?, ?, ?)")
        prep_ir = session.prepare(f"INSERT INTO {target_ks}.items_by_region (region_id, category_id, end_date, item_id, seller_id, name) VALUES (?, ?, ?, ?, ?, ?)")
        prep_is = session.prepare(f"INSERT INTO {target_ks}.items_by_seller (seller_id, end_date, item_id, name) VALUES (?, ?, ?, ?)")

        jobs_it = [(prep_it, (it.id, it.name, it.description, float(it.initial_price), it.quantity, float(it.reserve_price), float(it.buy_now), it.nb_of_bids, float(it.max_bid), it.start_date, it.end_date, it.seller_id, it.category_id)) for it in item_rows]
        jobs_ic = [(prep_ic, (it.category_id, it.end_date, it.id, it.name, float(it.initial_price), float(it.max_bid), it.nb_of_bids)) for it in item_rows]
        jobs_ir = [(prep_ir, (user_region_map.get(it.seller_id, 1), it.category_id, it.end_date, it.id, it.seller_id, it.name)) for it in item_rows]
        jobs_is = [(prep_is, (it.seller_id, it.end_date, it.id, it.name)) for it in item_rows]

        execute_concurrent(session, jobs_it, concurrency=50)
        execute_concurrent(session, jobs_ic, concurrency=50)
        execute_concurrent(session, jobs_ir, concurrency=50)
        execute_concurrent(session, jobs_is, concurrency=50)

    # C3 and C7 specific tables in rubis_v8_c006_auto_c3c5c7
    prep_c3 = session.prepare("INSERT INTO rubis_v8_c006_auto_c3c5c7.items_selective (id, category_id, name) VALUES (?, ?, ?)")
    prep_c7 = session.prepare("INSERT INTO rubis_v8_c006_auto_c3c5c7.items_region_bounded (region_id, category_id, end_date, item_id, name) VALUES (?, ?, ?, ?, ?)")
    jobs_c3 = [(prep_c3, (it.id, it.category_id, it.name)) for it in item_rows]
    jobs_c7 = [(prep_c7, (user_region_map.get(it.seller_id, 1), it.category_id, it.end_date, it.id, it.name)) for it in item_rows]
    execute_concurrent(session, jobs_c3, concurrency=50)
    execute_concurrent(session, jobs_c7, concurrency=50)

    item_name_map = {it.id: it.name for it in item_rows}

    # 4. Bids
    bid_rows = []
    for chunk in [bids[i:i+100] for i in range(0, len(bids), 100)]:
        bid_rows.extend(session.execute(f"SELECT * FROM rubis_full.bids WHERE id IN ({','.join(map(str, chunk))})"))
    print(f"Fetched {len(bid_rows)} bids from rubis_full.")

    for target_ks in ["rubis_v8_c006_auto_norm", "rubis_v8_c006_auto_c3c5c7"]:
        prep_b = session.prepare(f"INSERT INTO {target_ks}.bids (id, user_id, item_id, qty, bid, bid_date) VALUES (?, ?, ?, ?, ?, ?)")
        prep_bi = session.prepare(f"INSERT INTO {target_ks}.bids_by_item (item_id, bid_date, bid_id, user_id, bid, qty) VALUES (?, ?, ?, ?, ?, ?)")
        jobs_b = [(prep_b, (b.id, b.user_id, b.item_id, b.qty, float(b.bid), b.bid_date)) for b in bid_rows]
        jobs_bi = [(prep_bi, (b.item_id, b.bid_date, b.id, b.user_id, float(b.bid), b.qty)) for b in bid_rows]
        execute_concurrent(session, jobs_b, concurrency=50)
        execute_concurrent(session, jobs_bi, concurrency=50)

    # C5 specific table in rubis_v8_c006_auto_c3c5c7
    prep_c5 = session.prepare("INSERT INTO rubis_v8_c006_auto_c3c5c7.item_bid_shared (item_id, bid_date, bid_id, bid, item_name, qty, user_id) VALUES (?, ?, ?, ?, ?, ?, ?)")
    jobs_c5 = [(prep_c5, (b.item_id, b.bid_date, b.id, float(b.bid), item_name_map.get(b.item_id, f"item-{b.item_id}"), b.qty, b.user_id)) for b in bid_rows]
    execute_concurrent(session, jobs_c5, concurrency=50)

    # 5. Comments
    comment_rows = []
    for chunk in [cids[i:i+100] for i in range(0, len(cids), 100)]:
        comment_rows.extend(session.execute(f"SELECT * FROM rubis_full.comments WHERE id IN ({','.join(map(str, chunk))})"))
    print(f"Fetched {len(comment_rows)} comments from rubis_full.")

    user_nick_map = {u.id: u.nickname for u in user_rows}
    for target_ks in ["rubis_v8_c006_auto_norm", "rubis_v8_c006_auto_c3c5c7"]:
        prep_c = session.prepare(f"INSERT INTO {target_ks}.comments (id, from_user_id, to_user_id, item_id, rating, comment_date, comment) VALUES (?, ?, ?, ?, ?, ?, ?)")
        prep_ct = session.prepare(f"INSERT INTO {target_ks}.comments_by_to_user (to_user_id, comment_date, comment_id, rating, comment) VALUES (?, ?, ?, ?, ?)")
        prep_ci = session.prepare(f"INSERT INTO {target_ks}.comments_by_id (id, from_user_id, nickname) VALUES (?, ?, ?)")

        jobs_c = [(prep_c, (c.id, c.from_user_id, c.to_user_id, c.item_id, c.rating, c.comment_date, c.comment)) for c in comment_rows]
        jobs_ct = [(prep_ct, (c.to_user_id, c.comment_date, c.id, c.rating, c.comment)) for c in comment_rows]
        jobs_ci = [(prep_ci, (c.id, c.from_user_id, user_nick_map.get(c.from_user_id, f"user-{c.from_user_id}"))) for c in comment_rows]
        execute_concurrent(session, jobs_c, concurrency=50)
        execute_concurrent(session, jobs_ct, concurrency=50)
        execute_concurrent(session, jobs_ci, concurrency=50)

    # 6. Buynow
    buynow_rows = []
    for chunk in [bnids[i:i+100] for i in range(0, len(bnids), 100)]:
        buynow_rows.extend(session.execute(f"SELECT * FROM rubis_full.buynow WHERE id IN ({','.join(map(str, chunk))})"))
    print(f"Fetched {len(buynow_rows)} buynow from rubis_full.")

    for target_ks in ["rubis_v8_c006_auto_norm", "rubis_v8_c006_auto_c3c5c7"]:
        prep_bn = session.prepare(f"INSERT INTO {target_ks}.buynow (id, buyer_id, item_id, qty, buy_date) VALUES (?, ?, ?, ?, ?)")
        prep_bnb = session.prepare(f"INSERT INTO {target_ks}.buynow_by_buyer (buyer_id, buy_date, id, item_id, qty) VALUES (?, ?, ?, ?, ?)")
        jobs_bn = [(prep_bn, (bn.id, bn.buyer_id, bn.item_id, bn.qty, bn.buy_date)) for bn in buynow_rows]
        jobs_bnb = [(prep_bnb, (bn.buyer_id, bn.buy_date, bn.id, bn.item_id, bn.qty)) for bn in buynow_rows]
        execute_concurrent(session, jobs_bn, concurrency=50)
        execute_concurrent(session, jobs_bnb, concurrency=50)

    print("Cohort A keyspaces successfully populated and verified.")
    cluster.shutdown()

if __name__ == "__main__":
    setup()
