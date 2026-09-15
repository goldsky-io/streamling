"""Independent exact-rational oracle for full-binary arithmetic roundtrips."""
import json
import os
import random
from fractions import Fraction
from pathlib import Path

rng = random.Random(370003)

def text(value, scale):
    coefficient = round(value * 10**scale)
    sign = '-' if coefficient < 0 else ''
    digits = str(abs(coefficient)).zfill(scale + 1)
    if scale:
        digits = (digits[:-scale] + '.' + digits[-scale:]).rstrip('0').rstrip('.')
    return (sign + digits) if coefficient else '0'

groups = []
pool = [0, 1, -1, 2, -2, 3, 7, 8, 16, 31, 32, 127, 128, 255, 256,
        257, 65535, 65536, 2**64 - 1, 2**64 + 1, 2**128 - 1,
        2**255 - 1, 10**99 - 1, -(10**99 - 1)]
for sa, sb in [(0, 0), (2, 18), (18, 2), (18, 18), (0, 78), (78, 0), (100, 100)]:
    rows = []
    for i in range(96):
        ac = pool[i % len(pool)] if i < 48 else rng.randrange(-(10**99), 10**99)
        bc = pool[(i * 7 + 3) % len(pool)] if i < 48 else rng.randrange(-(10**99), 10**99)
        if bc == 0:
            bc = 2
        a, b = Fraction(ac, 10**sa), Fraction(bc, 10**sb)
        av = None if i % 17 == 16 else str(ac)
        bv = None if i % 19 == 18 else str(bc)
        if av is None or bv is None:
            expected = dict.fromkeys(['added','subtracted','multiplied','divided','remainder','recovered'])
        else:
            quotient = abs(a / b).numerator // abs(a / b).denominator
            if (a < 0) != (b < 0):
                quotient = -quotient
            expected = {
                'added': text(a+b,max(sa,sb)),
                'subtracted': text(a-b,max(sa,sb)),
                'multiplied': text(a*b,sa+sb),
                'divided': text(a/b,max(sa,18)),
                'remainder': text(a-b*quotient,max(sa,sb)),
                'recovered': text(a,sa),
            }
        rows.append({'id':i+1,'a':av,'b':bv,'expected':expected})
    groups.append({'sa':sa,'sb':sb,'precision':100,'rows':rows})

destination = Path(os.environ.get('STREAMLING_REVIEW_PIPELINE_ORACLE', Path(__file__).resolve().parents[3] / 'crates/streamling-e2e/tests/deep_v3_pipeline_oracle.json'))
destination.write_text(json.dumps(groups,indent=2)+'\n')
print(f'{len(groups)} scale pairs; {sum(len(g["rows"]) for g in groups)} rows; six output columns per row')
