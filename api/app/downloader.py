import pandas as pd
import requests
import os
from urllib.parse import urlparse
import time
from datetime import datetime
from pathlib import Path
import concurrent.futures
import threading
from typing import List, Dict, Tuple, Optional
import logging
from datetime import datetime
import openpyxl
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from PIL import Image
import pillow_heif
from pdf2image import convert_from_path
import tempfile

# Configuración de logging
def setup_logging(log_folder: str = "logs") -> logging.Logger:
    """Configura el sistema de logging"""
    os.makedirs(log_folder, exist_ok=True)
    
    logger = logging.getLogger('evidencias_downloader')
    logger.setLevel(logging.INFO)
    
    # Handler para archivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_handler = logging.FileHandler(f"{log_folder}/download_{timestamp}.log", encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formato
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

class EvidenciasDownloader:
    def __init__(self, max_workers: int = 5, max_retries: int = 3, timeout: int = 30, convert_files: bool = True):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.timeout = timeout
        self.convert_files = convert_files
        self.logger = setup_logging()
        self.download_stats = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'converted': 0,
            'conversion_failed': 0
        }
        self.lock = threading.Lock()
        
        # Registrar plugin HEIF para PIL
        if self.convert_files:
            try:
                pillow_heif.register_heif_opener()
                self.logger.info("✅ Plugin HEIF registrado para PIL")
            except Exception as e:
                self.logger.warning(f"⚠️ No se pudo registrar plugin HEIF: {e}")
        
        # Configurar sesión de requests con reintentos
        self.session = requests.Session()
        try:
            # Intentar con el parámetro nuevo
            retry_strategy = Retry(
                total=max_retries,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"],
                backoff_factor=1
            )
        except TypeError:
            # Fallback para versiones anteriores
            try:
                retry_strategy = Retry(
                    total=max_retries,
                    status_forcelist=[429, 500, 502, 503, 504],
                    method_whitelist=["HEAD", "GET", "OPTIONS"],
                    backoff_factor=1
                )
            except TypeError:
                # Versión más simple si fallan ambas
                retry_strategy = Retry(
                    total=max_retries,
                    status_forcelist=[429, 500, 502, 503, 504],
                    backoff_factor=1
                )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def clean_filename(self, filename: str) -> str:
        """Limpia caracteres especiales del nombre de archivo"""
        if not filename or pd.isna(filename):
            return "archivo_sin_nombre"
        
        filename = str(filename)
        
        # Reemplaza caracteres especiales por versiones ASCII
        replacements = {
            'ó': 'o', 'ú': 'u', 'í': 'i', 'á': 'a', 'é': 'e',
            'ñ': 'n', 'ü': 'u', 'Ó': 'O', 'Ú': 'U', 'Í': 'I',
            'Á': 'A', 'É': 'E', 'Ñ': 'N', 'Ü': 'U'
        }
        
        for old, new in replacements.items():
            filename = filename.replace(old, new)
        
        # Remueve caracteres no válidos para nombres de archivo
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Limitar longitud del nombre
        if len(filename) > 200:
            filename = filename[:200]
        
        return filename

    def get_file_extension(self, url: str) -> str:
        """Obtiene la extensión del archivo desde la URL"""
        if not url or pd.isna(url):
            return ""
        
        try:
            parsed_url = urlparse(str(url))
            path = parsed_url.path
            
            # Buscar extensión en el path
            if '.' in path:
                ext = '.' + path.split('.')[-1]
                # Validar que la extensión no sea muy larga
                if len(ext) <= 10:
                    return ext
        except Exception:
            pass
        
        return ""

    def convert_heic_to_jpg(self, input_path: str, output_path: str) -> bool:
        """Convierte archivo HEIC a JPG"""
        try:
            # Abrir imagen HEIC
            with Image.open(input_path) as image:
                # Convertir a RGB si es necesario
                if image.mode != 'RGB':
                    image = image.convert('RGB')
                
                # Guardar como JPG con buena calidad
                image.save(output_path, 'JPEG', quality=90, optimize=True)
            
            self.logger.info(f"🔄 HEIC convertido a JPG: {os.path.basename(output_path)}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error convirtiendo HEIC {input_path}: {e}")
            return False

    def convert_pdf_to_jpg(self, input_path: str, output_folder: str, base_filename: str) -> bool:
        """Convierte archivo PDF a JPG (primera página o todas las páginas)"""
        try:
            # Convertir PDF a imágenes
            images = convert_from_path(
                input_path,
                dpi=200,  # Buena calidad
                first_page=1,
                last_page=1
            )
            
            if images:
                # Guardar primera página como JPG
                output_path = os.path.join(output_folder, f"{base_filename}.jpg")
                images[0].save(output_path, 'JPEG', quality=90, optimize=True)
                
                self.logger.info(f"🔄 PDF convertido a JPG: {os.path.basename(output_path)}")
                return True
            else:
                self.logger.error(f"❌ No se pudieron extraer imágenes del PDF: {input_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error convirtiendo PDF {input_path}: {e}")
            return False

    def post_process_file(self, file_path: str) -> Optional[str]:
        """Post-procesa el archivo descargado (conversiones)"""
        if not self.convert_files:
            return file_path
        
        file_ext = os.path.splitext(file_path)[1].lower()
        base_path = os.path.splitext(file_path)[0]
        folder_path = os.path.dirname(file_path)
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        
        try:
            if file_ext == '.heic':
                # Convertir HEIC a JPG
                jpg_path = f"{base_path}.jpg"
                if self.convert_heic_to_jpg(file_path, jpg_path):
                    # Eliminar archivo original HEIC
                    os.remove(file_path)
                    with self.lock:
                        self.download_stats['converted'] += 1
                    return jpg_path
                else:
                    with self.lock:
                        self.download_stats['conversion_failed'] += 1
                    return file_path
            
            elif file_ext == '.pdf':
                # Convertir PDF a JPG
                jpg_path = f"{base_path}.jpg"
                if self.convert_pdf_to_jpg(file_path, folder_path, base_filename):
                    # Eliminar archivo original PDF
                    os.remove(file_path)
                    with self.lock:
                        self.download_stats['converted'] += 1
                    return jpg_path
                else:
                    with self.lock:
                        self.download_stats['conversion_failed'] += 1
                    return file_path
            
            return file_path
            
        except Exception as e:
            self.logger.error(f"❌ Error en post-procesamiento de {file_path}: {e}")
            with self.lock:
                self.download_stats['conversion_failed'] += 1
            return file_path

    def download_single_file(self, url: str, filename: str, folder_path: str) -> bool:
        """Descarga un archivo individual y lo post-procesa si es necesario"""
        if not url or pd.isna(url):
            return False
        
        try:
            # Verificar si el archivo final (posiblemente convertido) ya existe
            base_filename = os.path.splitext(filename)[0]
            original_ext = os.path.splitext(filename)[1].lower()
            
            # Si el archivo original es HEIC o PDF, verificar si ya existe la versión JPG
            if self.convert_files and original_ext in ['.heic', '.pdf']:
                jpg_filename = f"{base_filename}.jpg"
                jpg_path = os.path.join(folder_path, jpg_filename)
                if os.path.exists(jpg_path) and os.path.getsize(jpg_path) > 0:
                    self.logger.info(f"⏭️ Archivo convertido ya existe: {jpg_filename}")
                    with self.lock:
                        self.download_stats['skipped'] += 1
                    return True
            
            # Verificar archivo original
            file_path = os.path.join(folder_path, filename)
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                # Si no necesita conversión, está listo
                if not self.convert_files or original_ext not in ['.heic', '.pdf']:
                    self.logger.info(f"⏭️ Archivo ya existe: {filename}")
                    with self.lock:
                        self.download_stats['skipped'] += 1
                    return True
            
            # Crear carpeta si no existe
            os.makedirs(folder_path, exist_ok=True)
            
            # Realizar descarga
            response = self.session.get(url, stream=True, timeout=self.timeout)
            response.raise_for_status()
            
            # Escribir archivo temporal
            temp_path = f"{file_path}.tmp"
            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Verificar descarga exitosa
            if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                self.logger.error(f"❌ Descarga falló: {filename}")
                with self.lock:
                    self.download_stats['failed'] += 1
                return False
            
            # Mover archivo temporal a ubicación final
            os.rename(temp_path, file_path)
            
            self.logger.info(f"✅ Descargado: {filename} ({os.path.getsize(file_path)} bytes)")
            
            # Post-procesar archivo (conversiones)
            final_path = self.post_process_file(file_path)
            
            with self.lock:
                self.download_stats['successful'] += 1
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Error de conexión descargando {filename}: {e}")
            with self.lock:
                self.download_stats['failed'] += 1
            return False
        except Exception as e:
            self.logger.error(f"❌ Error inesperado con {filename}: {e}")
            with self.lock:
                self.download_stats['failed'] += 1
            return False

    def prepare_download_tasks(self, df: pd.DataFrame, output_folder: str) -> List[Dict]:
        """Prepara las tareas de descarga desde el DataFrame"""
        tasks = []
        
        for index, row in df.iterrows():
            # Extraer información básica
            grupo = str(row.get('Código del grupo', 'Sin_grupo')) if pd.notna(row.get('Código del grupo')) else "Sin_grupo"
            sesion = str(row.get('Sesión', 'Sin_sesion')) if pd.notna(row.get('Sesión')) else f"Sesion_{index+1}"
            
            # Limpiar nombres
            grupo = self.clean_filename(grupo)
            sesion = self.clean_filename(sesion)
            
            # Crear carpeta para este grupo
            session_folder = os.path.join(output_folder, grupo)
            
            # Definir tipos de archivos a procesar
            file_types = [
                ('Archivo asistencia', 'asistencia'),
                ('Archivo foto inicial', 'foto_inicial'),
                ('Archivo foto final', 'foto_final')
            ]
            
            for column_name, file_type in file_types:
                if column_name in row and pd.notna(row[column_name]):
                    url = str(row[column_name]).strip()
                    if url:
                        ext = self.get_file_extension(url)
                        filename = f"{sesion}_{file_type}{ext}"
                        
                        task = {
                            'url': url,
                            'filename': filename,
                            'folder_path': session_folder,
                            'grupo': grupo,
                            'sesion': sesion,
                            'tipo': file_type
                        }
                        tasks.append(task)
                        
                        with self.lock:
                            self.download_stats['total'] += 1
        
        return tasks

    def download_with_threads(self, tasks: List[Dict]) -> None:
        """Ejecuta las descargas usando ThreadPoolExecutor"""
        if not tasks:
            self.logger.warning("No hay tareas de descarga para ejecutar")
            return
        
        self.logger.info(f"🚀 Iniciando descarga de {len(tasks)} archivos con {self.max_workers} hilos")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Crear futures para todas las tareas
            future_to_task = {
                executor.submit(
                    self.download_single_file,
                    task['url'],
                    task['filename'],
                    task['folder_path']
                ): task for task in tasks
            }
            
            # Procesar completados
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    if not result:
                        self.logger.error(f"❌ Falló descarga: {task['filename']}")
                except Exception as e:
                    self.logger.error(f"❌ Excepción en descarga: {task['filename']} - {e}")

    def read_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Lee un archivo CSV o Excel y retorna un DataFrame"""
        try:
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.csv':
                # Intentar diferentes delimitadores y encodings
                for delimiter in [';', ',', '\t']:
                    for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                        try:
                            df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
                            if len(df.columns) > 1:  # Verificar que se leyó correctamente
                                self.logger.info(f"📄 CSV leído exitosamente: {os.path.basename(file_path)} ({len(df)} filas)")
                                return df
                        except Exception:
                            continue
                
            elif file_ext in ['.xlsx', '.xls']:
                try:
                    # Intentar leer Excel
                    df = pd.read_excel(file_path, engine='openpyxl' if file_ext == '.xlsx' else 'xlrd')
                    self.logger.info(f"📊 Excel leído exitosamente: {os.path.basename(file_path)} ({len(df)} filas)")
                    return df
                except Exception as e:
                    self.logger.error(f"❌ Error leyendo Excel {file_path}: {e}")
            
            else:
                self.logger.error(f"❌ Formato de archivo no soportado: {file_ext}")
            
        except Exception as e:
            self.logger.error(f"❌ Error leyendo archivo {file_path}: {e}")
        
        return None

    def process_folder(self, input_folder: str, output_folder: str = None) -> None:
        """
        Procesa todos los archivos CSV y Excel dentro de una carpeta de entrada.
        Crea subcarpetas dentro del directorio de salida para cada archivo.
        """
    
        # 🧩 Si no se pasa output_folder, crear uno temporal dentro de /tmp
        if output_folder is None:
            base_tmp = os.getenv("TMPDIR", "/tmp")
            output_folder = os.path.join(base_tmp, "Evidencias_Descargadas")
    
        # 📁 Asegurarse de que las carpetas existen
        os.makedirs(output_folder, exist_ok=True)
    
        # 🚨 Validar existencia de la carpeta de entrada
        if not os.path.exists(input_folder):
            self.logger.error(f"❌ La carpeta {input_folder} no existe")
            return
    
        # 🔍 Buscar archivos CSV y Excel
        supported_extensions = ['.csv', '.xlsx', '.xls']
        files_to_process = [
            os.path.join(input_folder, f)
            for f in os.listdir(input_folder)
            if os.path.isfile(os.path.join(input_folder, f))
            and os.path.splitext(f)[1].lower() in supported_extensions
        ]
    
        if not files_to_process:
            self.logger.warning(f"⚠️ No se encontraron archivos CSV o Excel en {input_folder}")
            return
    
        self.logger.info(f"🔍 Encontrados {len(files_to_process)} archivos para procesar en {input_folder}")
    
        all_tasks = []
    
        # 🔄 Procesar cada archivo
        for file_path in files_to_process:
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            self.logger.info(f"📂 Procesando: {file_name}")
    
            df = self.read_file(file_path)
            if df is None:
                self.logger.warning(f"⚠️ No se pudo leer el archivo {file_path}")
                continue
    
            # Crear subcarpeta específica para el archivo
            file_output_folder = os.path.join(output_folder, file_name)
            os.makedirs(file_output_folder, exist_ok=True)
    
            # Preparar tareas
            tasks = self.prepare_download_tasks(df, file_output_folder)
            all_tasks.extend(tasks)
            self.logger.info(f"📋 Preparadas {len(tasks)} tareas de descarga para {file_name}")
    
        # 🧵 Ejecutar descargas y mostrar estadísticas
        if all_tasks:
            start_time = datetime.now()
            self.download_with_threads(all_tasks)
            end_time = datetime.now()
            self.print_final_stats(start_time, end_time, output_folder)
        else:
            self.logger.warning("⚠️ No se generaron tareas de descarga")


    def print_final_stats(self, start_time: datetime, end_time: datetime, output_folder: str):
        """Imprime estadísticas finales del proceso"""
        duration = end_time - start_time
        
        print("\n" + "="*60)
        print("📊 RESUMEN DE DESCARGA Y CONVERSIÓN")
        print("="*60)
        print(f"⏰ Tiempo total: {duration}")
        print(f"📁 Carpeta de salida: {output_folder}")
        print(f"📊 Total de archivos procesados: {self.download_stats['total']}")
        print(f"✅ Archivos descargados exitosamente: {self.download_stats['successful']}")
        print(f"⏭️ Archivos omitidos (ya existían): {self.download_stats['skipped']}")
        print(f"❌ Archivos con error: {self.download_stats['failed']}")
        
        if self.convert_files:
            print(f"🔄 Archivos convertidos a JPG: {self.download_stats['converted']}")
            print(f"⚠️ Conversiones fallidas: {self.download_stats['conversion_failed']}")
        
        if self.download_stats['total'] > 0:
            success_rate = (self.download_stats['successful'] / self.download_stats['total']) * 100
            print(f"📈 Tasa de éxito: {success_rate:.1f}%")
        
        print("="*60)

def check_dependencies():
    """Verifica que las dependencias necesarias estén instaladas"""
    missing_deps = []
    
    try:
        import pillow_heif
    except ImportError:
        missing_deps.append("pillow-heif")
    
    try:
        from pdf2image import convert_from_path
    except ImportError:
        missing_deps.append("pdf2image")
    
    if missing_deps:
        print("❌ Faltan dependencias para conversión de archivos:")
        print("📦 Instala las dependencias con:")
        print("   pip install pillow-heif pdf2image")
        print("\n💡 Para instalar poppler (necesario para pdf2image):")
        print("   🪟 Windows:")
        print("     1. Descargar desde: https://github.com/oschwartz10612/poppler-windows/releases")
        print("     2. Extraer y agregar bin/ al PATH del sistema")
        print("     3. O usar: winget install poppler")
        print("   🍎 macOS:")
        print("     brew install poppler")
        print("   🐧 Linux:")
        print("     sudo apt-get install poppler-utils")
        print("     # o sudo yum install poppler-utils (Red Hat/CentOS)")
        print("\n✅ Comando completo de instalación:")
        print("   pip install pillow-heif pdf2image")
        return False
    
    return True

def main():
    """Función principal"""
    print("🚀 Descargador de Evidencias con Conversión HEIC/PDF → JPG")
    print("="*60)
    
    # Verificar dependencias
    deps_ok = check_dependencies()
    
    # Configuración
    MAX_WORKERS = 8  # Número de hilos concurrentes
    OUTPUT_FOLDER = "Evidencias_Descargadas"  # Carpeta de salida
    CONVERT_FILES = deps_ok  # Solo convertir si las dependencias están disponibles
    
    if not deps_ok:
        print("⚠️ Continuando sin conversión automática de archivos...")
        time.sleep(2)
    
    # Crear instancia del descargador
    downloader = EvidenciasDownloader(
        max_workers=MAX_WORKERS,
        convert_files=CONVERT_FILES
    )
    
