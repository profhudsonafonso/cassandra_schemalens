"""E6 — semantic contract of the 36 RUBiS statements, as executed (V8 reference semantics).

Declarative form, interpreted by the V9 oracle (oracle_v9.py) and, in Phase 2, by the route synthesiser.
Attribute references use model attribute names; a dotted prefix navigates a relationship of the EER
(e.g. ``seller.region_id`` from items = users.region_id of the item's seller).

Read contract keys
  entity      entity whose instances are enumerated (one candidate row per instance)
  where       [(attr_ref, op, source)]  source = "param:<name>" | "ctx:<key>|param:<name>" | "const:<v>"
  require     [rel]  inner joins that must resolve (row dropped if the related instance is absent)
  order       [(attr_ref, "asc"|"desc")]  total order, ties broken by the listed keys
  limit       int | None  applied AFTER selection and ordering
  distinct    attr_ref | None  keep the first row per value (after ordering)
  project     [(output_key, attr_ref)]  output_key is the canonical result key
  fallback    {attr_ref: "user-{fk}"}  value used when a navigated instance is missing (V8 behaviour)
  ctx_out     {ctx_key: (row_index, output_key)}
Write contract keys
  op INSERT: entity, values {attr: source}, event {key: source}
  op UPDATE: entity, key param, values {attr: expr}, event {...}; expr evaluated over ctx/params
Deviations from the NoSE text are listed per statement in ``deviation`` (D-V9-005, D-V9-006).
"""
from __future__ import annotations

ITEM_ALL = [("items.id", "id"), ("items.name", "name"), ("items.description", "description"), ("items.initial_price", "initial_price"),
            ("items.quantity", "quantity"), ("items.reserve_price", "reserve_price"), ("items.buy_now", "buy_now"),
            ("items.nb_of_bids", "nb_of_bids"), ("items.max_bid", "max_bid"), ("items.start_date", "start_date"), ("items.end_date", "end_date")]
USER_ALL = [("users.id", "id"), ("users.firstname", "firstname"), ("users.lastname", "lastname"), ("users.nickname", "nickname"),
            ("users.password", "password"), ("users.email", "email"), ("users.rating", "rating"), ("users.balance", "balance"),
            ("users.creation_date", "creation_date")]
USER_BY_ID = [("id", "=", "param:user_id")]
ITEM_BY_ID = [("id", "=", "param:item_id")]


def _r(entity, where, project, **kw):
    return {"kind": "read", "entity": entity, "where": where, "project": project, "order": kw.get("order", []),
            "limit": kw.get("limit"), "distinct": kw.get("distinct"), "require": kw.get("require", []),
            "fallback": kw.get("fallback", {}), "ctx_out": kw.get("ctx_out", {}), "deviation": kw.get("deviation")}


CONTRACT = {
    1: _r("users", USER_BY_ID, [("users.nickname", "nickname"), ("users.password", "password")]),
    2: _r("categories", [("dummy", "=", "const:1")], [("categories.id", "id"), ("categories.name", "name")], order=[("id", "asc")],
          deviation="resultado ordenado por id (NoSE não fixa ordem)"),
    3: _r("items", ITEM_BY_ID, [("items.name", "name")]),
    4: _r("bids", [("item_id", "=", "param:item_id")],
          [("users.id", "user_id"), ("users.nickname", "user.nickname"), ("bids.id", "id"), ("item.id", "item_id"),
           ("bids.qty", "qty"), ("bids.bid", "bid"), ("bids.date", "date")],
          order=[("date", "asc"), ("id", "asc")], fallback={"user.nickname": "user-{user_id}"},
          deviation="ORDER BY bids.date com desempate por bids.id"),
    5: _r("items", ITEM_BY_ID, ITEM_ALL),
    6: _r("bids", [("item_id", "=", "param:item_id")], [("bids.id", "id"), ("bids.qty", "qty"), ("bids.bid", "bid"), ("bids.date", "date")],
          order=[("id", "asc")], deviation="projeção bids.* sem as chaves estrangeiras; ordem por bids.id (NoSE não fixa ordem)"),
    7: _r("items", [("category_id", "=", "param:category_id"), ("end_date", ">=", "param:since")],
          [("items.id", "id"), ("items.name", "name"), ("items.initial_price", "initial_price"), ("items.max_bid", "max_bid"),
           ("items.nb_of_bids", "nb_of_bids"), ("items.end_date", "end_date")],
          order=[("end_date", "asc"), ("id", "asc")], limit=25, deviation="ordem (end_date, id) antes do LIMIT 25"),
    8: _r("users", USER_BY_ID, USER_ALL),
    9: _r("comments", [("to_user_id", "=", "param:user_id")],
          [("comments.id", "id"), ("comments.rating", "rating"), ("comments.date", "date"), ("comments.comment", "comment")],
          order=[("id", "asc")], limit=25, deviation="LIMIT 25 por id (ausente no NoSE)"),
    10: {"kind": "write", "op": "INSERT", "entity": "items",
         "values": {"id": "param:new_id", "name": "param:name", "description": "param:description", "initial_price": "param:initial_price",
                    "quantity": "param:quantity", "reserve_price": "param:reserve_price", "buy_now": "param:buy_now", "nb_of_bids": "const:0",
                    "max_bid": "const:0.0", "start_date": "param:date", "end_date": "param:end_date", "seller_id": "param:user_id",
                    "category_id": "param:category_id"},
         "event": {"write_event": "const:RegisterItem", "item_id": "param:new_id"}},
    11: {"kind": "write", "op": "INSERT", "entity": "users",
         "values": {"id": "param:new_id", "firstname": "param:firstname", "lastname": "param:lastname", "nickname": "param:nickname",
                    "password": "param:password", "email": "param:email", "rating": "const:0", "balance": "const:0.0",
                    "creation_date": "param:date", "region_id": "param:region_id"},
         "event": {"write_event": "const:RegisterUser", "user_id": "param:new_id"}},
    12: _r("users", USER_BY_ID, [("users.nickname", "nickname")]),
    13: _r("items", ITEM_BY_ID, ITEM_ALL),
    14: _r("items", ITEM_BY_ID, [("items.quantity", "quantity"), ("items.nb_of_bids", "nb_of_bids"), ("items.end_date", "end_date")],
           ctx_out={"read_item_14": (0, None)}),
    15: {"kind": "write", "op": "UPDATE", "entity": "items", "key": "param:item_id",
         "values": {"quantity": "max0(ctx:read_item_14.items.quantity|param:quantity - param:qty)"},
         "event": {"write_event": "const:ReduceQuantity", "item_id": "param:item_id", "quantity": "value:quantity"},
         "deviation": "D-V9-005: só quantity muda; nb_of_bids e end_date ficam como estão (NoSE: SET quantity, nb_of_bids, end_date)"},
    16: {"kind": "write", "op": "INSERT", "entity": "buynow",
         "values": {"id": "param:new_id", "qty": "param:qty", "date": "param:date", "buyer_id": "param:user_id", "item_id": "param:item_id"},
         "event": {"write_event": "const:StoreBuyNow", "buynow_id": "param:new_id"}},
    17: _r("users", USER_BY_ID, [("users.nickname", "nickname"), ("users.password", "password")]),
    18: _r("items", ITEM_BY_ID, ITEM_ALL),
    19: _r("bids", [("item_id", "=", "param:item_id")], [("bids.qty", "qty"), ("bids.date", "date")],
           order=[("bid", "asc"), ("id", "asc")], limit=2, deviation="ORDER BY bids.bid com desempate por bids.id"),
    20: {"kind": "write", "op": "INSERT", "entity": "bids",
         "values": {"id": "param:new_id", "qty": "param:qty", "bid": "param:bid", "date": "param:date", "user_id": "param:user_id",
                    "item_id": "param:item_id"},
         "event": {"write_event": "const:StoreBid", "bid_id": "param:new_id"}},
    21: _r("items", ITEM_BY_ID, [("items.nb_of_bids", "nb_of_bids"), ("items.max_bid", "max_bid")], ctx_out={"read_item_21": (0, None)}),
    22: {"kind": "write", "op": "UPDATE", "entity": "items", "key": "param:item_id",
         "values": {"nb_of_bids": "ctx:read_item_21.items.nb_of_bids|const:0 + 1",
                    "max_bid": "max(ctx:read_item_21.items.max_bid|const:0.0, param:bid)"},
         "event": {"write_event": "const:UpdateMaxBid", "item_id": "param:item_id", "nb_of_bids": "value:nb_of_bids"}},
    23: _r("users", USER_BY_ID, [("users.nickname", "nickname"), ("users.password", "password")]),
    24: _r("items", ITEM_BY_ID, ITEM_ALL),
    25: _r("users", USER_BY_ID, USER_ALL),
    26: _r("users", USER_BY_ID, [("users.rating", "rating")], ctx_out={"read_user_rating": (0, "users.rating")}),
    27: {"kind": "write", "op": "UPDATE", "entity": "users", "key": "param:user_id",
         "values": {"rating": "ctx:read_user_rating|const:0 + param:rating"},
         "event": {"write_event": "const:UpdateUserRating", "user_id": "param:user_id", "rating": "value:rating"}},
    28: {"kind": "write", "op": "INSERT", "entity": "comments",
         "values": {"id": "param:new_id", "rating": "param:rating", "date": "param:date", "comment": "param:comment",
                    "from_user_id": "param:from_user_id", "to_user_id": "param:user_id", "item_id": "param:item_id"},
         "event": {"write_event": "const:StoreComment", "comment_id": "param:new_id"}},
    29: _r("users", USER_BY_ID, USER_ALL),
    30: _r("comments", [("to_user_id", "=", "param:user_id")],
           [("comments_received.id", "id"), ("comments_received.rating", "rating"), ("comments_received.date", "date"),
            ("comments_received.comment", "comment")],
           order=[("id", "asc")], limit=25, ctx_out={"active_comment_id": (0, "comments_received.id")},
           deviation="LIMIT 25 por id (ausente no NoSE); o primeiro comentário alimenta a instrução 31"),
    31: _r("comments", [("id", "=", "ctx:active_comment_id|param:comment_id")], [("from_user.nickname", "from_user.nickname")],
           fallback={"from_user.nickname": "user-{from_user_id}"}),
    32: _r("buynow", [("buyer_id", "=", "param:user_id"), ("date", ">=", "param:since")],
           [("bought_now.id", "id"), ("bought_now.qty", "qty"), ("bought_now.date", "date"), ("items.id", "item.id"), ("items.name", "item.name")],
           order=[("date", "asc"), ("id", "asc")], require=["item"], limit=25,
           deviation="projeção reduzida a (compra, items.id, items.name); ordem (date, id) crescente; LIMIT 25; compras de itens ausentes da coorte são descartadas"),
    33: _r("items", [("seller_id", "=", "param:user_id"), ("end_date", ">=", "param:since")], [("items.id", "id"), ("items.name", "name")],
           order=[("id", "asc")], limit=25, deviation="projeção items.* reduzida a (id, name); LIMIT 25 por id"),
    34: _r("bids", [("user_id", "=", "param:user_id"), ("item.end_date", ">=", "param:since")], [("items.id", "item.id"), ("items.name", "item.name")],
           order=[("item.id", "asc")], distinct="item.id", limit=25, require=["item"],
           deviation="projeção items.* reduzida a (id, name); itens distintos ordenados por id, LIMIT 25"),
    35: _r("items", [("seller.region_id", "=", "param:region_id"), ("category_id", "=", "param:category_id"), ("end_date", ">=", "param:since")],
           [("items.id", "id"), ("items.name", "name"), ("items.end_date", "end_date")],
           order=[("end_date", "asc"), ("id", "asc")], limit=25, deviation="projeção reduzida a (id, name, end_date); ordem (end_date, id)"),
    36: _r("regions", [("dummy", "=", "const:1")], [("regions.id", "id"), ("regions.name", "name")], order=[("id", "asc")],
           deviation="resultado ordenado por id (NoSE não fixa ordem)"),
}
WRITE_IDS = sorted(s for s, c in CONTRACT.items() if c["kind"] == "write")
READ_IDS = sorted(s for s, c in CONTRACT.items() if c["kind"] == "read")
