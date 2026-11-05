import json
from functools import lru_cache
from fastapi import FastAPI, HTTPException
import uvicorn
from generator.make_reference import DATA

app = FastAPI(title='Cinema online fixture API')

@app.get('/health')
def health(): return {'status':'ok'}

@lru_cache(maxsize=2)
def load(run_id):
    if not run_id.replace('-','').replace('_','').isalnum(): raise HTTPException(400,'Invalid run_id')
    path=DATA/'runs'/run_id/'online.json'
    if not path.exists(): raise HTTPException(404,'Unknown run')
    return json.loads(path.read_text())

@app.get('/events')
def events(run_id: str='demo', offset: int=0, limit: int=500):
    if offset<0 or not 1<=limit<=5000: raise HTTPException(400,'Invalid page')
    return {'events':load(run_id)[offset:offset+limit], 'total':len(load(run_id))}

if __name__=='__main__': uvicorn.run(app,host='0.0.0.0',port=8000)
