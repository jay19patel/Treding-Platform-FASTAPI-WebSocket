import asyncio
import json
import datetime
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from nselive import NSEDataFetcher
app = FastAPI()
websocket_clients = []

# Initialize the NSEDataFetcher
nse_data_fetcher = NSEDataFetcher()

# Define market open and close times
market_open_time = datetime.time(9, 15)
market_close_time = datetime.time(15, 15)

# Function to get NSE data if the market is open
def get_nse_data():
    current_time = datetime.datetime.now().time()
    current_datetime = datetime.datetime.now()
    formatted_time = current_datetime.strftime("%I:%M:%S %p %m-%d-%Y")
    response = nse_data_fetcher.fetch_all_data()
    return response

# WebSocket task to send NSE data to clients
async def send_nse_data():
    while True:
        if websocket_clients:
            message = json.dumps({'Data': get_nse_data()})
            await asyncio.gather(
                *[client.send_text(message) for client in websocket_clients]
            )
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(send_nse_data())

@app.websocket("/nselive_ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_clients.append(websocket)

    try:
        while True:
            data = await websocket.receive_text()
    except Exception as e:
        print(f"WebSocket Error: {e}")
    finally:
        websocket_clients.remove(websocket)
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app)
