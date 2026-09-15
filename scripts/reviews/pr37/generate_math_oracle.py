"""Independent expected results via Python Fraction.__round__ (exact, half even)."""
from fractions import Fraction
import json, os, random
from pathlib import Path
random.seed(20260915)
cases=[]
def add(n,sa,d,sb,s):
    a=Fraction(n,10**sa) if sa>=0 else Fraction(n*10**(-sa),1)
    b=Fraction(d,10**sb) if sb>=0 else Fraction(d*10**(-sb),1)
    cases.append(dict(n=str(n),sa=sa,d=str(d),sb=sb,scale=s,expected=str(round(a/b*10**s))))
for s in [0,1,2,18,76,120,256,512]:
    for k in [-10**78,-3,-2,-1,0,1,2,3,10**78-1]:
        for delta in [-1,0,1]:
            guard=10**160
            add((2*k+1)*guard+delta,s,2*guard,0,s)
for digits_a in [1,2,18,38,78,120,200]:
    for digits_b in [1,3,19,77,179]:
        for i in range(12):
            n=random.randrange(10**(digits_a-1),10**digits_a)*random.choice([-1,1])
            d=random.randrange(10**(digits_b-1),10**digits_b)*random.choice([-1,1])
            sa=random.choice([-32,-1,0,1,8,18,76,120,256])
            sb=random.choice([-32,-1,0,1,8,18,76,120,256])
            for s in [0,18,120,512]:add(n,sa,d,sb,s)
for s in [0,18,512]:
    for d in [-17,-1,1,17]:add(0,256,d,-32,s)
avgs=[]
sets=[[],[None,None],[0],[1,2],[1,2,None,3],[1,1,2],[-1,-1,-2],[10**180-1,10**180-1,10**180-2],[-10**180+1,10**180-1,1], [None, 10**78-1, -10**78+2, None, 0, 0,0], [0,0,0,1],[-1,0,0,0], [10**200-1,-10**200+1]*7+[1,2,3]]
for scale in [0,1,18,76,120,256]:
    for vals in sets:
        xs=[x for x in vals if x is not None]
        expected=None if not xs else str(round(Fraction(sum(xs)*10,len(xs))))
        avgs.append(dict(scale=scale,values=[None if x is None else str(x) for x in vals],expected=expected))
casts=[]
for bits,precisions in [(128,[1,2,18,38]),(256,[1,2,18,38,76])]:
    for p in precisions:
        for ts in sorted(set([-128,-2,0,1,min(p,18),p])):
            for k in [-10**p,-10**p+1,-3,-2,-1,0,1,2,3,10**p-2,10**p-1,10**p]:
                for delta in [-1,0,1]:
                    exponent=ts+61
                    n=(2*k+1)*5*10**60+delta
                    value=Fraction(n,10**exponent) if exponent>=0 else Fraction(n*10**(-exponent),1)
                    scaled=value*10**ts if ts>=0 else value/Fraction(10**(-ts),1)
                    expected=round(scaled)
                    coeff=n if exponent>=0 else n*10**(-exponent)
                    casts.append(dict(bits=bits,precision=p,scale=ts,n=str(coeff),source_scale=max(0,exponent),expected=str(expected),fits=abs(expected)<10**p))
root=Path(os.environ.get('STREAMLING_REVIEW_TEST_DIR', Path(__file__).resolve().parents[3] / 'crates/streamling-common/tests'))
root.mkdir(parents=True,exist_ok=True)
(root/'deep_v2_math_oracle.json').write_text(json.dumps(dict(division=cases,avg=avgs,casts=casts),separators=(',',':')))
print(f'{len(cases)} exact division cases; {len(avgs)} average data sets; {len(casts)} narrowing boundary cases')
