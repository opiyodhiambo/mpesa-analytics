from fastapi import FastAPI
from api.routes import router as mpesa_router
from dashboard.dashboard_app import app as dash_app
from starlette.middleware.wsgi import WSGIMiddleware

app = FastAPI()

# Dash (Flask) app at /dashboard
app.mount("/dashboard", WSGIMiddleware(dash_app.server), name="dashboard")

# FastAPI API endpoint
app.include_router(mpesa_router)
