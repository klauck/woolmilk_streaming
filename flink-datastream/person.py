import random

COLUMNS = ["id", "name", "email_address", "credit_card", "city", "state", "date_time", "extra"]

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi",
    "Ivan", "Judy", "Karl", "Laura", "Mallory", "Niaj", "Olivia", "Peggy",
    "Quinn", "Rupert", "Sybil", "Trent", "Uma", "Victor", "Walter", "Xena",
    "Yves", "Zara",
]
LAST_NAMES = ["Smith", "Jones", "Taylor", "Brown", "Wilson", "Evans", "Wright", "Hall"]
CITIES = ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Leipzig"]
STATES = ["BE", "HH", "BY", "NW", "HE", "SN"]
BASE_TIME_MS = 1_700_000_000_000


def make_person(i):
    rng = random.Random(i)
    name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    row = [
        i,
        name,
        f"{name.split()[0].lower()}{i}@example.com",
        f"{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}",
        rng.choice(CITIES),
        rng.choice(STATES),
        BASE_TIME_MS + i * 10,
        f"extra_{rng.randint(0, 1_000_000)}",
    ]
    return ",".join(str(v) for v in row)


def name_gt_h(line):
    return line.split(",")[1] > "H"
