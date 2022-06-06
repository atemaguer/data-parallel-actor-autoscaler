import string

# consistent hashing
MMH3_HASH_SEED = 1
TOKENS_INITIAL = 1 << 6  # 64

NUM_REPEATS = 50

# data = [
#    "a",
#    "b",
#    "c",
#    "d",
#    "e",
#    "f",
#    "g",
#    "h",
#    "i",
#    "j",
#    "k",
#    "l",
#    "m",
#    "n",
#    "o",
#    "p",
#    "q",
#    "r",
#    "s",
#    "t",
#    "u",
#    "v",
#    "w",
#    "x",
#    "y",
#    "z",
# ] * NUM_REPEATS

# data = ['a'] * 10 + ['b','c','d','e','f','g','h','i','j']
# data = data * NUM_REPEATS

# ls = string.ascii_lowercase
# data = list(ls[0:5]) * 5 + list(ls[5:10]) * 3 + list(ls[10:])
# data = data * (NUM_REPEATS // 2)

# def generate_for_a(a):
#    rep = 100 if a in string.ascii_lowercase[:5] else 1
#    return [f'{a}{b}' for b in string.ascii_lowercase] * rep

# data = [
#    item
#    for a in string.ascii_lowercase
#    for item in generate_for_a(a)
# ]

# import mmh3

# def gen(l):
#    rep = 20 if mmh3.hash(l, seed=0) % 4 == 1 else 1
#    return [l] * rep

# data = [
#    item
#    for l in string.ascii_lowercase
#    for item in gen(l)
# ] * 10

data = [
    "a",
    "b",
    "c",
    "d",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "n",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
] * 10


def wl4_gen():
    r1 = ["o", "u"]
    r2 = ["f", "h", "j", "w"]
    out = r1 * 20
    out += r2 * 10
    out += [l for l in string.ascii_lowercase if l not in out]
    assert len(out) == 100
    return out


wl1 = ["a", "b", "c", "f"] * 25
wl2 = ["a", "g", "i", "q"] * 25
wl3 = ["a"] * 100
wl4 = wl4_gen()
wl5 = list(string.ascii_lowercase[:25] * 4)


WORKLOADS = dict(
    wl1=wl1,
    wl2=wl2,
    wl3=wl3,
    wl4=wl4,
    wl5=wl5,
)
