"""Deterministic self-contained Parquet fixtures for the v3 file-source audit."""
from pathlib import Path
from decimal import Decimal
import json, os
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(os.environ.get('STREAMLING_REVIEW_PARQUET_FIXTURE_DIR', Path(__file__).resolve().parents[3] / 'crates/streamling-e2e/tests/fixtures/deep_v3'))
ROOT.mkdir(parents=True, exist_ok=True)

def canonical_integer(n):
    return bytes([255 if n < 0 else 0]) + abs(n).to_bytes(max(1, (abs(n).bit_length()+7)//8), 'big')

manifest = []
for kind in ['arb', 'native', 'binary']:
    for scale, ident in [(0,1),(0,2),(2,2),(2,1)]:
        name=f'{kind}_s{scale}_id{ident}.parquet'
        if kind=='arb':
            field=pa.field('amount',pa.large_binary(),True,metadata={b'ARROW:extension:name':b'streamling.decimal_arb',b'ARROW:extension:metadata':json.dumps({'precision':100,'scale':scale},separators=(',',':')).encode()})
            amount=pa.array([canonical_integer(10**scale)],type=pa.large_binary())
        elif kind=='binary':
            field=pa.field('amount',pa.large_binary(),True)
            amount=pa.array([canonical_integer(10**scale)],type=pa.large_binary())
        else:
            field=pa.field('amount',pa.decimal128(30,scale),True)
            amount=pa.array([Decimal(1)],type=field.type)
        schema=pa.schema([pa.field('id',pa.int64(),False),field])
        table=pa.Table.from_arrays([pa.array([ident],pa.int64()),amount],schema=schema)
        pq.write_table(table,ROOT/name,compression='NONE',use_dictionary=True,row_group_size=1,store_schema=True)
        reloaded=pq.read_table(ROOT/name)
        assert reloaded.schema==schema
        assert reloaded.column('amount').to_pylist()==amount.to_pylist()
        manifest.append({'file':name,'kind':kind,'scale':scale,'id':ident,**({'numeric_value':'1'} if kind != 'binary' else {'hex_bytes':canonical_integer(10**scale).hex()}),'metadata':{k.decode():v.decode() for k,v in (field.metadata or {}).items()}})
(ROOT/'manifest.json').write_text(json.dumps({'generator':'generate_parquet_fixtures.py','pyarrow':pa.__version__,'files':manifest},indent=2)+'\n')
print(f'wrote and independently reread {len(manifest)} fixtures at {ROOT}')
