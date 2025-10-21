from fastapi import FastAPI

app = FastAPI(title="Distributed Traffic System")

@app.get("/")
def root():
    return {"message": "Server is running"}
