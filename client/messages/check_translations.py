import json

with open('en.json') as f:
    en = json.load(f)

files = ['zh.json', 'ja.json', 'de.json', 'pl.json', 'pt.json', 'it.json', 'fr.json', 'ko.json', 'es.json']

for fname in files:
    with open(fname) as f:
        trans = json.load(f)

    empty = [k for k, v in trans.items() if v == '']
    stale = [k for k in trans if k not in en]
    missing = [k for k in en if k not in trans]
    english = [k for k, v in trans.items() if k in en and v == en[k] and v != '']

    print(f'{fname}:')
    print(f'  Empty strings: {empty}')
    print(f'  Stale keys: {stale}')
    print(f'  Missing keys: {missing}')
    print(f'  Still English (same as en.json): {english}')
    print()
