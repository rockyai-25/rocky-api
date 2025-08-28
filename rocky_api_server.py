"""
🔥 ROCKY API SERVER - FastAPI Backend
Servidor para ejecutar ROCKY desde el Control Center
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, Dict, List
import subprocess
import asyncio
import json
import os
import uuid
from datetime import datetime
import logging
from pathlib import Path
import signal
import psutil

 
# Configurar logging
import os
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Crear FastAPI app
app = FastAPI(title="ROCKY API Server", version="1.0.0")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://app.rockyai.sbs",
        "https://*.vercel.app",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()

# API Secret desde environment
API_SECRET = os.getenv("ROCKY_API_SECRET", "your-default-secret-key")

# Base paths
BASE_DIR = Path(__file__).parent
VIDEOS_DIR = BASE_DIR / "data" / "videos"
LOGS_DIR = BASE_DIR / "logs"

# Crear directorios si no existen
VIDEOS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Store active jobs in memory (en producción usar Redis)
active_jobs: Dict[str, dict] = {}
active_processes: Dict[str, subprocess.Popen] = {}

# WebSocket connections
websocket_connections: List[WebSocket] = []

# Pydantic models
class ExecuteRequest(BaseModel):
    mode: str  # test, single, batch
    theme: Optional[str] = None
    count: Optional[int] = 3
    user_id: Optional[str] = None

class JobStatus(BaseModel):
    job_id: str
    status: str  # pending, processing, completed, failed, cancelled
    progress: int
    mode: str
    theme: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    output_path: Optional[str]
    logs: List[str]
    videos_generated: int
    error: Optional[str]

# Verificar autenticación
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if token != API_SECRET:
        raise HTTPException(status_code=403, detail="Invalid authentication")
    return token

# Health check
@app.get("/")
async def root():
    return {
        "status": "active",
        "service": "ROCKY API Server",
        "version": "1.0.0",
        "active_jobs": len(active_jobs)
    }

# Health endpoint para Railway (mantener activo)
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Ejecutar ROCKY
@app.post("/execute")
async def execute_rocky(
    request: ExecuteRequest,
    background_tasks: BackgroundTasks,
    token: str = Depends(verify_token)
):
    """Ejecutar rocky_complete_video_pipeline.py con los parámetros especificados"""
    
    job_id = str(uuid.uuid4())
    
    # Preparar comando
    cmd = ["python", "rocky_complete_video_pipeline.py"]
    
    if request.mode == "test":
        cmd.append("--test")
    elif request.mode == "single":
        cmd.extend(["--single", request.theme or "peliculas"])
    elif request.mode == "batch":
        cmd.append("--batch")
    else:
        raise HTTPException(status_code=400, detail="Invalid mode")
    
    # Crear job
    job = {
        "job_id": job_id,
        "status": "pending",
        "progress": 0,
        "mode": request.mode,
        "theme": request.theme,
        "started_at": datetime.now(),
        "completed_at": None,
        "output_path": str(VIDEOS_DIR / job_id),
        "logs": [],
        "videos_generated": 0,
        "error": None,
        "user_id": request.user_id
    }
    
    active_jobs[job_id] = job
    
    # Ejecutar en background
    background_tasks.add_task(run_rocky_process, job_id, cmd)
    
    logger.info(f"Started job {job_id} with mode {request.mode}")
    
    # Notificar via WebSocket
    await notify_websockets({
        "type": "job_started",
        "job_id": job_id,
        "mode": request.mode,
        "theme": request.theme
    })
    
    return {
        "success": True,
        "job_id": job_id,
        "message": f"Job started in {request.mode} mode",
        "status_url": f"/status/{job_id}"
    }

# Ejecutar proceso ROCKY
async def run_rocky_process(job_id: str, cmd: List[str]):
    """Ejecutar el proceso y actualizar el estado"""
    
    job = active_jobs[job_id]
    job["status"] = "processing"
    job["progress"] = 10
    
    try:
        # Crear directorio para output
        output_dir = Path(job["output_path"])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Log file para este job
        log_file = LOGS_DIR / f"job_{job_id}.log"
        
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        # Ejecutar proceso
        with open(log_file, "w") as log:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            active_processes[job_id] = process
            
            # Leer output línea por línea
            for line in process.stdout:
                log.write(line)
                job["logs"].append(line.strip())
                
                # Actualizar progreso basado en output
                if "Processing" in line:
                    job["progress"] = 30
                elif "Downloading" in line:
                    job["progress"] = 50
                elif "Segmenting" in line:
                    job["progress"] = 70
                elif "Completed" in line or "Success" in line:
                    job["progress"] = 90
                
                # Notificar progreso via WebSocket
                await notify_websockets({
                    "type": "job_progress",
                    "job_id": job_id,
                    "progress": job["progress"],
                    "log": line.strip()
                })
            
            process.wait()
            
            if process.returncode == 0:
                job["status"] = "completed"
                job["progress"] = 100
                
                # Contar videos generados
                video_files = list(Path(VIDEOS_DIR).glob(f"**/*.mp4"))
                job["videos_generated"] = len(video_files)
                
                logger.info(f"Job {job_id} completed successfully")
            else:
                job["status"] = "failed"
                job["error"] = f"Process exited with code {process.returncode}"
                logger.error(f"Job {job_id} failed with code {process.returncode}")
    
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        logger.error(f"Job {job_id} error: {e}")
    
    finally:
        job["completed_at"] = datetime.now()
        if job_id in active_processes:
            del active_processes[job_id]
        
        # Notificar finalización
        await notify_websockets({
            "type": "job_completed",
            "job_id": job_id,
            "status": job["status"],
            "videos_generated": job.get("videos_generated", 0)
        })

# Obtener estado del job
@app.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Obtener el estado actual de un job"""
    
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = active_jobs[job_id]
    
    return JobStatus(
        job_id=job["job_id"],
        status=job["status"],
        progress=job["progress"],
        mode=job["mode"],
        theme=job["theme"],
        started_at=job["started_at"],
        completed_at=job["completed_at"],
        output_path=job["output_path"],
        logs=job["logs"][-20:],  # Últimas 20 líneas
        videos_generated=job["videos_generated"],
        error=job["error"]
    )

# Listar todos los jobs
@app.get("/jobs")
async def list_jobs(
    token: str = Depends(verify_token),
    limit: int = 20
):
    """Listar todos los jobs activos y recientes"""
    
    jobs_list = []
    for job_id, job in list(active_jobs.items())[-limit:]:
        jobs_list.append({
            "job_id": job_id,
            "status": job["status"],
            "mode": job["mode"],
            "theme": job["theme"],
            "progress": job["progress"],
            "started_at": job["started_at"],
            "videos_generated": job.get("videos_generated", 0)
        })
    
    return {
        "total": len(active_jobs),
        "jobs": jobs_list
    }

# Cancelar job
@app.post("/cancel/{job_id}")
async def cancel_job(
    job_id: str,
    token: str = Depends(verify_token)
):
    """Cancelar un job en ejecución"""
    
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job_id in active_processes:
        try:
            process = active_processes[job_id]
            
            # Terminar proceso y sus hijos
            parent = psutil.Process(process.pid)
            for child in parent.children(recursive=True):
                child.kill()
            parent.kill()
            
            active_jobs[job_id]["status"] = "cancelled"
            active_jobs[job_id]["completed_at"] = datetime.now()
            
            del active_processes[job_id]
            
            logger.info(f"Job {job_id} cancelled")
            
            return {"success": True, "message": "Job cancelled"}
        
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    return {"success": False, "message": "Job not running"}

# WebSocket para actualizaciones en tiempo real
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket para actualizaciones en tiempo real"""
    
    await websocket.accept()
    websocket_connections.append(websocket)
    
    try:
        # Enviar estado inicial
        await websocket.send_json({
            "type": "connected",
            "active_jobs": len(active_jobs)
        })
        
        # Mantener conexión abierta
        while True:
            # Recibir mensajes del cliente (para mantener vivo)
            data = await websocket.receive_text()
            
            if data == "ping":
                await websocket.send_text("pong")
    
    except WebSocketDisconnect:
        websocket_connections.remove(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        if websocket in websocket_connections:
            websocket_connections.remove(websocket)

# Notificar a todos los WebSockets
async def notify_websockets(message: dict):
    """Enviar mensaje a todos los clientes conectados"""
    
    disconnected = []
    for connection in websocket_connections:
        try:
            await connection.send_json(message)
        except:
            disconnected.append(connection)
    
    # Limpiar conexiones muertas
    for conn in disconnected:
        if conn in websocket_connections:
            websocket_connections.remove(conn)

# Limpiar recursos al cerrar
@app.on_event("shutdown")
async def shutdown_event():
    """Limpiar procesos al cerrar el servidor"""
    
    for job_id, process in active_processes.items():
        try:
            process.terminate()
            logger.info(f"Terminated process for job {job_id}")
        except:
            pass

# Keep-alive task para Railway
async def keep_alive():
    """Tarea para mantener el servidor activo en Railway"""
    while True:
        await asyncio.sleep(240)  # Cada 4 minutos
        logger.info("Keep-alive ping")
        # Auto-ping para evitar sleep
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                await client.get("http://localhost:8000/health")
        except:
            pass

# Iniciar keep-alive al arrancar
@app.on_event("startup")
async def startup_event():
    """Iniciar tareas de background"""
    asyncio.create_task(keep_alive())
    logger.info("ROCKY API Server started")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)