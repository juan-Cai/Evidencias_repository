from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from downloader import EvidenciasDownloader, check_dependencies
from pathlib import Path
import shutil
import os
import tempfile
import zipfile
import uuid

app = FastAPI(title="Evidencias Downloader API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # O puedes restringirlo a tu dominio Apps Script
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process")
async def process_files(files: list[UploadFile] = File(...), background_tasks: BackgroundTasks = None):
    """
    Recibe uno o varios archivos (CSV o Excel),
    procesa las evidencias y devuelve un ZIP descargable.
    """
    output_folder = "resultados"
    session_id = str(uuid.uuid4())
    input_dir = Path(tempfile.mkdtemp(prefix=f"input_{session_id}_"))
    output_dir = Path(tempfile.mkdtemp(prefix=f"output_{session_id}_"))

    try:
        # Guardar archivos subidos
        for file in files:
            file_path = input_dir / file.filename
            with open(file_path, "wb") as f:
                shutil.copyfileobj(file.file, f)

        # Verificar dependencias
        deps_ok = check_dependencies()
        downloader = EvidenciasDownloader(
            max_workers=6,
            convert_files=deps_ok
        )

        # Procesar los archivos
        downloader.process_folder(str(input_dir), str(output_dir))

        # Crear ZIP con los resultados
        zip_path = Path(tempfile.gettempdir()) / f"resultados_{session_id}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(output_dir):
                for file in files:
                    file_path = Path(root) / file
                    arcname = file_path.relative_to(output_dir)
                    zipf.write(file_path, arcname)

        # Limpieza automática después de enviar respuesta
        if background_tasks:
            background_tasks.add_task(shutil.rmtree, input_dir, ignore_errors=True)
            background_tasks.add_task(shutil.rmtree, output_dir, ignore_errors=True)
            background_tasks.add_task(os.remove, zip_path)

        zip_path = "resultados.zip"
        shutil.make_archive("resultados", 'zip', output_folder)
        return FileResponse(zip_path, media_type="application/zip", filename="resultados.zip")

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/")
def root():
    return {"message": "API para descarga y conversión de evidencias lista ✅"}



