"""Projeto Gabriel: API e WebSocket de uma simulação clínica educativa.
Inicie com `python -m uvicorn main:app --host 0.0.0.0 --port 3000`.
Em produção, use HTTPS/WSS, um único worker, e armazenamento compartilhado para escala.
"""
import asyncio
import io
import json
import secrets
import time
from pathlib import Path
from typing import Dict
from urllib.parse import quote
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import qrcode
import qrcode.image.svg

BASE=Path(__file__).resolve().parent
QUESTIONS=json.loads((BASE/'questions.json').read_text(encoding='utf-8'))
ROOMS: Dict[str,dict]={}
app=FastAPI(title='Projeto Gabriel — Semiologia')
app.mount('/assets',StaticFiles(directory=BASE/'public'/'assets'),name='assets')

@app.get('/')
async def index():return FileResponse(BASE/'public'/'index.html')
@app.get('/style.css')
async def css():return FileResponse(BASE/'public'/'style.css',media_type='text/css')
@app.get('/app.js')
async def js():return FileResponse(BASE/'public'/'app.js',media_type='text/javascript')
@app.get('/api/questions')
async def questions():return [{**q,'id':i} for i,q in enumerate(QUESTIONS)]
@app.post('/api/room')
async def create_room():
    code=str(secrets.randbelow(900000)+100000)
    while code in ROOMS: code=str(secrets.randbelow(900000)+100000)
    secret=secrets.token_hex(20)
    ROOMS[code]={'code':code,'secret':secret,'phase':'lobby','questionIndex':-1,'round':QUESTIONS[:], 'players':{},'clients':set(),'openAt':0}
    return JSONResponse({'code':code,'hostSecret':secret},status_code=201)
@app.get('/api/qr/{code}')
async def qr(code:str, request:Request):
    if code not in ROOMS:raise HTTPException(status_code=404,detail='Sala não encontrada')
    # A URL apontada depende do domínio em que a aplicação estiver publicada.
    base=str(request.base_url).rstrip('/')
    target=f'{base}/?room={quote(code)}'
    img=qrcode.make(target,image_factory=qrcode.image.svg.SvgImage,box_size=12,border=2)
    f=io.BytesIO();img.save(f)
    return Response(f.getvalue(),media_type='image/svg+xml',headers={'Cache-Control':'no-store'})

def results(room):
    q=room['round'][room['questionIndex']]
    return {'correct':q['correct'],'why':q['why'],
      'counts':[sum(1 for p in room['players'].values() if p['answers'].get(room['questionIndex'],{}).get('answer')==i) for i in range(4)],
      'ranking':sorted([{'name':p['name'],'score':p['score']} for p in room['players'].values()],key=lambda x:-x['score'])}

def public(room):
    phase=room['phase'];i=room['questionIndex']
    return {'code':room['code'],'phase':phase,'questionIndex':i,'round':[{'id':k} for k in range(len(room['round']))],
      'players':[{'id':p['id'],'name':p['name'],'score':p['score']} for p in room['players'].values()],
      'question':{'q':room['round'][i]['q'],'a':room['round'][i]['a']} if phase=='question' else None,
      'results':results(room) if phase=='results' else None}

async def broadcast(room):
    payload={'type':'state','state':public(room)}
    expired=[]
    for client in list(room['clients']):
        try:await client.send_json(payload)
        except Exception:expired.append(client)
    for client in expired:room['clients'].discard(client)

@app.websocket('/ws')
async def ws_route(ws:WebSocket):
    await ws.accept();room=None;role='guest';player=None
    try:
        while True:
            d=await ws.receive_json();action=d.get('type')
            if action=='join':
                room=ROOMS.get(str(d.get('room','')))
                if not room:await ws.send_json({'type':'error','message':'Sala não encontrada'});continue
                room['clients'].add(ws)
                if d.get('secret') and secrets.compare_digest(str(d.get('secret')),room['secret']):
                    role='host';await ws.send_json({'type':'authorized','role':'host'})
                elif str(d.get('playerId','')) in room['players']:
                    role='player';player=room['players'][str(d.get('playerId'))]
                    await ws.send_json({'type':'authorized','role':'player','playerId':player['id']})
                elif isinstance(d.get('name'),str) and d['name'].strip():
                    role='player';pid=secrets.token_hex(8).upper()
                    player={'id':pid,'name':d['name'].strip()[:40],'score':0,'answers':{}}
                    room['players'][pid]=player
                    await ws.send_json({'type':'authorized','role':'player','playerId':pid})
                else:await ws.send_json({'type':'error','message':'Informe um apelido'});continue
                await broadcast(room);continue
            if not room:continue
            if role=='host':
                if action=='start':
                    room['round']=QUESTIONS[:] if d.get('mode')=='full' else [QUESTIONS[j] for j in (0,2,3,11)]
                    room['questionIndex']=0;room['phase']='question';room['openAt']=time.monotonic()
                    for p in room['players'].values():p['score']=0;p['answers']={}
                    await broadcast(room)
                elif action=='reveal' and room['phase']=='question':
                    room['phase']='results';await broadcast(room)
                elif action=='next' and room['phase']=='results':
                    room['questionIndex']+=1
                    room['phase']='finished' if room['questionIndex']>=len(room['round']) else 'question'
                    room['openAt']=time.monotonic();await broadcast(room)
                elif action=='reset':
                    room['phase']='lobby';room['questionIndex']=-1;await broadcast(room)
            elif role=='player' and action=='answer' and room['phase']=='question' and player:
                i=room['questionIndex'];ans=d.get('answer')
                if not isinstance(ans,int) or isinstance(ans,bool) or ans<0 or ans>3 or i in player['answers']:continue
                correct=ans==room['round'][i]['correct']
                elapsed=max(0,int((time.monotonic()-room['openAt'])*1000))
                points=max(500,1000-elapsed//60) if correct else 0
                player['answers'][i]={'answer':ans,'points':points};player['score']+=points
                await ws.send_json({'type':'answered','answer':ans,'points':points})
                await broadcast(room)
    except (WebSocketDisconnect,RuntimeError,ValueError,KeyError):pass
    finally:
        if room:room['clients'].discard(ws);await broadcast(room)
