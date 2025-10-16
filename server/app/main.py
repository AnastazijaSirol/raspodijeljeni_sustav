from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Raspodijeljeni sustavi")

@app.get("/health")
def health_check():
    return JSONResponse(content={"status": "ok", "message": "Server is running"})
