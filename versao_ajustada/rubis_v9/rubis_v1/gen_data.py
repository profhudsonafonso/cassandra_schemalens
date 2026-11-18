"""Passo 1 (servidor) - regenera a fonte rubis_full da coorte de forma deterministica.

O rubis_full da V8/V9 (Mac) foi gerado sem semente e o gerador nao foi preservado. Este modulo o RECONSTROI:

* Estrutura (chaves estrangeiras): modular, reconstruida das estatisticas observadas no E04 da V9
  (fan-outs exatos: 10 itens/vendedor, 4.000 itens/categoria, 4.000 usuarios/regiao, 500 pares
  (regiao do vendedor, categoria), 10 lances/item, 100 lances/usuario, 50 comentarios enviados e
  recebidos/usuario, 5 comentarios/item, 10 buynow/comprador, 1 buynow/item) e das convencoes que o
  codigo do fecho da V8 usa (itens do vendedor u = u + k*200000; lances do usuario u = u + k*200000;
  from_user do comentario c = (c-1) mod 200000 + 1).
* Valores: contrato do fake.js do NoSE (reused/inputs/historical_sources/experiments/rubis/fake.js),
  com um gerador pseudoaleatorio por linha (semente fixa + entidade + id): qualquer linha e reproduzivel
  isoladamente, sem gerar as 70,4 M linhas.
* Coorte: fecho do dominio do benchmark calculado OFFLINE com a mesma regra de
  reused/methodology/benchmark_005.enrich_baselines_with_domain_closure (consultas por particao das
  tabelas secundarias, com as ordens de clustering do esquema rubis_full).
Saida: frozen/cohort_source.pkl (mesmo formato do transfer.export) + frozen/GENERATION_MANIFEST.json.
"""
from __future__ import annotations

import hashlib
import pickle
import random
import string
import uuid
from datetime import datetime, timedelta

from common_paths import FROZEN, dump, sha

SEED = 20261117
N = dict(categories=500, regions=50, users=200_000, items=2_000_000, bids=20_000_000, comments=10_000_000, buynow=2_000_000)
CLOCK = datetime(2016, 1, 1)                     # fake.js date_past: ultimo ano antes do relogio de geracao
LOREM = ('lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna '
         'aliqua enim ad minim veniam quis nostrud exercitation ullamco laboris nisi aliquip ex ea commodo consequat duis aute '
         'irure in reprehenderit voluptate velit esse cillum fugiat nulla pariatur excepteur sint occaecat cupidatat non '
         'proident sunt culpa qui officia deserunt mollit anim id est laborum').split()
FIRST = ('Ana Bruno Carla Diego Elisa Fabio Gina Hugo Iris Joao Karen Luis Maria Nuno Olga Paulo Rita Sergio Tania Victor '
         'Alice Bob Carol David Emma Frank Grace Henry Irene Jack Kate Liam Mia Noah Olivia Peter Quinn Rose Sam Tom').split()
LAST = ('Silva Santos Souza Lima Costa Gomes Rocha Alves Pinto Ramos Smith Jones Brown Taylor Wilson Davies Evans Thomas '
        'Johnson Roberts Walker Wright Robinson Thompson White Hughes Edwards Green Hall Wood').split()
DOMAINS = ('gmail.com', 'yahoo.com', 'hotmail.com', 'example.org', 'mail.net')
TABLE_COLUMNS = {
    'categories': [('id', 'int'), ('name', 'text'), ('dummy', 'int')],
    'regions': [('id', 'int'), ('name', 'text'), ('dummy', 'int')],
    'users': [('id', 'int'), ('firstname', 'text'), ('lastname', 'text'), ('nickname', 'text'), ('password', 'text'),
              ('email', 'text'), ('rating', 'int'), ('balance', 'double'), ('creation_date', 'timestamp'), ('region_id', 'int')],
    'items': [('id', 'int'), ('name', 'text'), ('description', 'text'), ('initial_price', 'double'), ('quantity', 'int'),
              ('reserve_price', 'double'), ('buy_now', 'double'), ('nb_of_bids', 'int'), ('max_bid', 'double'),
              ('start_date', 'timestamp'), ('end_date', 'timestamp'), ('seller_id', 'int'), ('category_id', 'int')],
    'bids': [('id', 'int'), ('user_id', 'int'), ('item_id', 'int'), ('qty', 'int'), ('bid', 'double'), ('max_bid', 'double'), ('bid_date', 'timestamp')],
    'comments': [('id', 'int'), ('from_user_id', 'int'), ('to_user_id', 'int'), ('item_id', 'int'), ('rating', 'int'),
                 ('comment_date', 'timestamp'), ('comment', 'text')],
    'buynow': [('id', 'int'), ('buyer_id', 'int'), ('item_id', 'int'), ('qty', 'int'), ('buy_date', 'timestamp')],
}


def rng(entity, i):
    return random.Random(f'{SEED}:{entity}:{i}')


def words(r, n):
    return ' '.join(r.choice(LOREM) for _ in range(n))


def sentence(r):
    s = words(r, r.randint(4, 10))
    return s[0].upper() + s[1:] + '.'


def amount(r):                                   # faker finance.amount(): 0..1000, 2 casas
    return round(r.uniform(0, 1000), 2)


def mono(entity, i, offset_days=0):
    """Dates increase with the id inside the last year before CLOCK. Inferred from the V9 evidence: the NoSE
    baseline, whose tables cluster by id, matched the E6 contract (ordered by date) on 100% of the V9 requests,
    so in the original rubis_full the dates of an entity grow with its id (not fake.js date.past draws)."""
    step = (365 * 86_400_000) // N[entity]
    return CLOCK - timedelta(days=365) + timedelta(milliseconds=(i - 1) * step, days=offset_days)


# ---- structural rules (E04 fan-outs + V8 closure conventions)
def region_of_user(u): return (u - 1) % N['regions'] + 1
def seller_of_item(i): return (i - 1) % N['users'] + 1
def category_of_item(i): return (i - 1) % N['categories'] + 1
def bid_user(b): return (b - 1) % N['users'] + 1
def bid_item(b): return (b - 1) % N['items'] + 1
def comment_from(c): return (c - 1) % N['users'] + 1
def comment_to(c): return ((c - 1) // 50) % N['users'] + 1
def comment_item(c): return (c - 1) % N['items'] + 1
def buynow_buyer(b): return (b - 1) % N['users'] + 1
def buynow_item(b): return (b - 1) % N['items'] + 1


def row(entity, i):
    r = rng(entity, i)
    if entity in ('categories', 'regions'):
        return {'id': i, 'name': words(r, 2), 'dummy': 1}
    if entity == 'users':
        fn, ln = r.choice(FIRST), r.choice(LAST)
        return {'id': i, 'firstname': fn, 'lastname': ln, 'nickname': str(uuid.UUID(int=r.getrandbits(128), version=4)),
                'password': ''.join(r.choice(string.ascii_letters + string.digits) for _ in range(15)),
                'email': f'{fn}.{ln}{r.randint(1, 99)}@{r.choice(DOMAINS)}'.lower(), 'rating': r.randint(-50, 200),
                'balance': amount(r), 'creation_date': mono('users', i), 'region_id': region_of_user(i)}
    if entity == 'items':
        return {'id': i, 'name': words(r, 3), 'description': ' '.join(sentence(r) for _ in range(3)),
                'initial_price': amount(r), 'quantity': r.randint(0, 10), 'reserve_price': amount(r), 'buy_now': amount(r),
                'nb_of_bids': r.randint(0, 100), 'max_bid': amount(r), 'start_date': mono('items', i), 'end_date': mono('items', i, 7),
                'seller_id': seller_of_item(i), 'category_id': category_of_item(i)}
    if entity == 'bids':
        return {'id': i, 'user_id': bid_user(i), 'item_id': bid_item(i), 'qty': r.randint(1, 5), 'bid': amount(r),
                'max_bid': amount(r), 'bid_date': mono('bids', i)}
    if entity == 'comments':
        return {'id': i, 'from_user_id': comment_from(i), 'to_user_id': comment_to(i), 'item_id': comment_item(i),
                'rating': r.randint(-5, 5), 'comment_date': mono('comments', i), 'comment': ' '.join(sentence(r) for _ in range(2))}
    if entity == 'buynow':
        return {'id': i, 'buyer_id': buynow_buyer(i), 'item_id': buynow_item(i), 'qty': r.randint(1, 3), 'buy_date': mono('buynow', i)}
    raise ValueError(entity)


# ---- secondary-table partitions (rubis_full schema, same clustering as the V8 normalized schema)
def items_by_category(cid, limit):
    ids = range(cid, N['items'] + 1, N['categories'])                      # (i-1) % 500 + 1 == cid
    rows = sorted(((row('items', i)['end_date'], i) for i in ids), key=lambda x: (-x[0].timestamp(), x[1]))
    return [i for _, i in rows[:limit]]


def items_by_region(rg, cid, limit):
    ids = [i for i in range(cid, N['items'] + 1, N['categories']) if region_of_user(seller_of_item(i)) == rg]
    rows = sorted(((row('items', i)['end_date'], i) for i in ids), key=lambda x: (-x[0].timestamp(), x[1]))
    return [i for _, i in rows[:limit]]


def bids_by_item(iid): return [iid + k * N['items'] for k in range(N['bids'] // N['items'])]
def buynow_by_buyer(uid): return [uid + k * N['users'] for k in range(N['buynow'] // N['users'])]
def comments_by_to_user(uid): return list(range(50 * (uid - 1) + 1, 50 * uid + 1))


def closure(domain):
    """Offline replica of benchmark_005.enrich_baselines_with_domain_closure."""
    c = {e: {} for e in ('categories', 'regions', 'users', 'items', 'bids', 'comments', 'buynow')}
    for e in ('regions', 'categories'):
        for i in range(1, N[e] + 1):
            c[e][i] = row(e, i)
    user_ids, item_ids = set(domain['users']), set(domain['items'])
    comment_ids = set(domain['comments'])
    for uid in list(user_ids):
        for k in range(10):
            item_ids.add(uid + k * N['users'])
    for cid in domain['categories']:
        item_ids.update(items_by_category(cid, 60))
        for rg in domain['regions']:
            item_ids.update(items_by_region(rg, cid, 60))
    bid_ids = set()
    for iid in domain['items']:
        bid_ids.update(bids_by_item(iid))
    for uid in domain['users']:
        for k in range(15):
            bid_ids.add(uid + k * N['users'])
    buynow_ids = set()
    for uid in domain['users']:
        for b in buynow_by_buyer(uid):
            buynow_ids.add(b); item_ids.add(buynow_item(b))
    for uid in domain['users']:
        comment_ids.update(comments_by_to_user(uid))
    for b in sorted(bid_ids):
        if 1 <= b <= N['bids']:
            r = row('bids', b); c['bids'][b] = r; item_ids.add(r['item_id']); user_ids.add(r['user_id'])
    for x in sorted(comment_ids):
        if 1 <= x <= N['comments']:
            r = row('comments', x); c['comments'][x] = r; item_ids.add(r['item_id']); user_ids.update((r['from_user_id'], r['to_user_id']))
    for x in sorted(buynow_ids):
        if 1 <= x <= N['buynow']:
            r = row('buynow', x); c['buynow'][x] = r; item_ids.add(r['item_id']); user_ids.add(r['buyer_id'])
    for i in sorted(item_ids):
        if 1 <= i <= N['items']:
            r = row('items', i); c['items'][i] = r; user_ids.add(r['seller_id'])
    for u in sorted(user_ids):
        if 1 <= u <= N['users']:
            c['users'][u] = row('users', u)
    return c


def ddl(ks, table):
    cols = ', '.join(f'{n} {t}' for n, t in TABLE_COLUMNS[table])
    return f'CREATE TABLE {ks}.{table} ({cols}, PRIMARY KEY (id))'


def main():
    from reused.methodology.benchmark_005 import BENCHMARK_DOMAIN
    from reused.methodology import oracle_011 as O
    out = FROZEN / 'cohort_source.pkl'
    if out.exists():
        raise FileExistsError(f'{out} ja existe (nao sobrescreve)')
    cl = closure(BENCHMARK_DOMAIN)
    # V8 cohort (rubis_v8_c005_normalized) = first 5 pages x 1000 rows of each entity's token-ordered scan
    # (baselines_005.materialize, max_pages) UNION the domain closure. Token = Murmur3 of the int key.
    import heapq
    import struct
    from cassandra.murmur3 import murmur3
    pk = struct.Struct('>i').pack
    ids = {}
    for e in ('items', 'users', 'bids', 'comments', 'buynow'):
        first = {i for _, i in heapq.nsmallest(5000, ((murmur3(pk(i)), i) for i in range(1, N[e] + 1)))}
        ids[e] = sorted(first | set(cl[e]))
        for i in ids[e]:
            if i not in cl[e]:
                cl[e][i] = row(e, i)
    for e, fks in (('items', ['seller_id']), ('bids', ['user_id']), ('comments', ['from_user_id', 'to_user_id']), ('buynow', ['buyer_id'])):
        for r in list(cl[e].values()):
            for k in fks:
                if r[k] not in cl['users']:
                    cl['users'][r[k]] = row('users', r[k])
    blob = {'ddl': {'rubis_full': {t: ddl('rubis_full', t) for t in TABLE_COLUMNS},
                    O.COHORT_KEYSPACE: {t: f'CREATE TABLE {O.COHORT_KEYSPACE}.{t} (id int PRIMARY KEY)' for t in ids}},
            'rubis_full_rows': cl,
            'cohort_rows': {t: [{'id': i} for i in v] for t, v in ids.items()}}
    FROZEN.mkdir(parents=True, exist_ok=True)
    out.write_bytes(pickle.dumps(blob, protocol=4))
    man = {'origin': 'REGENERATED_ON_SERVER', 'seed': SEED, 'clock': CLOCK.isoformat(), 'full_scale_counts': N,
           'structure': 'modular; reconstruida das estatisticas E04 da V9 + convencoes do fecho da V8',
           'values': 'contrato fake.js do NoSE com PRNG por linha; datas monotonas no id (inferido: NoSE ordena por id e concordava 100% com o contrato E6 na V9)', 'closure_rule': 'coorte = 5 primeiras paginas x 1000 linhas em ordem de token Murmur3 (baselines_005.materialize, max_pages=5) UNIAO fecho do dominio (replica offline de benchmark_005.enrich_baselines_with_domain_closure)',
           'cohort_identity_keyspace': O.COHORT_KEYSPACE, 'cohort_counts': {e: len(v) for e, v in ids.items()},
           'rows_written': {e: len(v) for e, v in cl.items()},
           'v9_cohort_counts_for_reference': {'items': 5906, 'users': 5616, 'bids': 5690, 'comments': 6501, 'buynow': 5300},
           'pickle_sha256': sha(out), 'ddl': blob['ddl'], 'fingerprints_source': None}
    dump(FROZEN / 'TRANSFER_MANIFEST.json', man)
    print('GERADO', man['rows_written'], 'coorte', man['cohort_counts'])


if __name__ == '__main__':
    main()
