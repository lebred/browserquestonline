# BrowserQuest Online

Browser RPG prototype (Orson Quest) extracted from the live game at lab.trendearly.xyz/game.

## Included Files
- game.html: main browser game frontend
- game-wiki.html: game wiki page
- main.py: backend source that currently serves game API endpoints (/api/game/*)
- requirements.txt: Python dependencies from production app

## Quick Run (Dev)
1. python3 -m venv .venv
2. source .venv/bin/activate
3. pip install -r requirements.txt
4. uvicorn main:app --host 0.0.0.0 --port 8000

Then open:
- http://localhost:8000/game
- http://localhost:8000/wiki

## Notes
- This repository mirrors live game code from VPS at commit time.
- main.py includes non-game routes as well, because it is copied from the running production app.
