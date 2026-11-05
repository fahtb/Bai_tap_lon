"""Independent simulated POS service. CSV is storage behind the HTTP API."""
import csv
from fastapi import FastAPI, HTTPException
import uvicorn
from generator.make_reference import DATA

app = FastAPI(title='Cinema counter POS API', version='1.0.0')

@app.get('/health')
def health():
    return {'status': 'ok', 'source': 'counter-pos'}

@app.get('/sales')
def sales(run_id: str = 'demo', offset: int = 0, limit: int = 500):
    if not run_id.replace('-', '').replace('_', '').isalnum():
        raise HTTPException(400, 'Invalid run_id')
    if offset < 0 or not 1 <= limit <= 5000:
        raise HTTPException(400, 'Invalid page')
    path = DATA / 'runs' / run_id / 'counter.csv'
    if not path.exists():
        raise HTTPException(404, 'Unknown run')
    with path.open(encoding='utf-8', newline='') as f:
        rows = list(csv.DictReader(f))
    return {'sales': rows[offset:offset + limit], 'total': len(rows)}

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
