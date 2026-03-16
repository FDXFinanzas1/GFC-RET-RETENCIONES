"""
cargar_catastro_sri.py
======================
Descarga los CSV de Datos Abiertos del SRI (catastro RUC por provincia)
y los carga en la BD Azure para que el calculador de retenciones pueda
buscar CUALQUIER RUC de Ecuador sin necesidad de scraping.

Tablas destino:
  - public."SRI_Catastro_RUC"       → tabla de referencia con todos los RUCs
  - public."GFC-Prov-Proveedores"   → actualiza proveedores FOODIX existentes

Uso:
  python cargar_catastro_sri.py
  python cargar_catastro_sri.py --solo-actualizar   (no descarga, usa CSVs ya descargados)

Fuente: https://descargas.sri.gob.ec/download/datosAbiertos/SRI_RUC_{Provincia}.zip
"""

import os
import sys
import csv
import zipfile
import tempfile
import time
import urllib.request
import psycopg2
from datetime import datetime

# ── Configuracion BD ──────────────────────────────────────────────
DB_CONFIG = {
    'host': 'chiosburguer.postgres.database.azure.com',
    'port': 5432,
    'dbname': 'Chios',
    'user': 'adminChios',
    'password': 'Burger2023',
    'sslmode': 'require'
}

# ── Provincias del Ecuador ────────────────────────────────────────
PROVINCIAS = [
    'Azuay', 'Bolivar', 'Canar', 'Carchi', 'Chimborazo',
    'Cotopaxi', 'El_Oro', 'Esmeraldas', 'Galapagos', 'Guayas',
    'Imbabura', 'Loja', 'Los_Rios', 'Manabi', 'Morona_Santiago',
    'Napo', 'Orellana', 'Pastaza', 'Pichincha', 'Santa_Elena',
    'Santo_Domingo_de_los_Tsachilas', 'Sucumbios', 'Tungurahua',
    'Zamora_Chinchipe'
]

BASE_URL = 'https://descargas.sri.gob.ec/download/datosAbiertos'
DIR_DESCARGAS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'csv_sri')

# ── Grandes Contribuyentes (se descarga aparte) ──────────────────
URL_GRANDES = (
    'https://www.sri.gob.ec/o/sri-portlet-biblioteca-alfresco-internet/'
    'descargar/42c75ec2-95cb-4fa3-9d7b-dc5f59e9e4e2/'
    'Catastro%20Grandes%20Contribuyentes.xlsx'
)


def get_conn():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception:
        cfg = DB_CONFIG.copy()
        cfg['host'] = '4.246.223.171'
        return psycopg2.connect(**cfg)


def sn_a_sino(valor):
    """Convierte S/N del CSV a SI/NO de la BD."""
    if valor and valor.strip().upper() == 'S':
        return 'SI'
    return 'NO'


def crear_tabla_catastro(conn):
    """Crea la tabla SRI_Catastro_RUC si no existe."""
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS public."SRI_Catastro_RUC" (
            ruc                     VARCHAR(13) PRIMARY KEY,
            razon_social            TEXT,
            estado                  VARCHAR(30),
            tipo_persona            VARCHAR(50),
            regimen                 VARCHAR(30),
            obligado_contabilidad   VARCHAR(3),
            agente_retencion        VARCHAR(3),
            contribuyente_especial  VARCHAR(3),
            gran_contribuyente      VARCHAR(3) DEFAULT 'NO',
            actividad_economica     TEXT,
            fecha_inicio_actividades DATE,
            fecha_carga             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Indice para busquedas rapidas
    cur.execute('''
        CREATE INDEX IF NOT EXISTS idx_catastro_ruc
        ON public."SRI_Catastro_RUC" (ruc)
    ''')
    conn.commit()
    cur.close()
    print('[OK] Tabla SRI_Catastro_RUC verificada/creada')


def descargar_provincia(provincia):
    """Descarga y extrae el ZIP de una provincia. Retorna la ruta del CSV."""
    os.makedirs(DIR_DESCARGAS, exist_ok=True)

    csv_path = os.path.join(DIR_DESCARGAS, f'SRI_RUC_{provincia}.csv')
    if os.path.exists(csv_path):
        print(f'  [CACHE] {provincia} ya descargado')
        return csv_path

    # Intentar ZIP primero, luego CSV directo
    zip_url = f'{BASE_URL}/SRI_RUC_{provincia}.zip'
    zip_path = os.path.join(DIR_DESCARGAS, f'SRI_RUC_{provincia}.zip')

    print(f'  [DESCARGA] {provincia}...', end=' ', flush=True)
    try:
        req = urllib.request.Request(zip_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=120) as resp:
            with open(zip_path, 'wb') as f:
                f.write(resp.read())

        # Extraer ZIP
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(DIR_DESCARGAS)
        os.remove(zip_path)

        # Buscar el CSV extraido
        if os.path.exists(csv_path):
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f'{size_mb:.1f} MB')
            return csv_path

        # A veces el nombre dentro del ZIP difiere
        for f in os.listdir(DIR_DESCARGAS):
            if f.lower().startswith('sri_ruc') and f.lower().endswith('.csv') and provincia.lower() in f.lower():
                real_path = os.path.join(DIR_DESCARGAS, f)
                os.rename(real_path, csv_path)
                size_mb = os.path.getsize(csv_path) / (1024 * 1024)
                print(f'{size_mb:.1f} MB')
                return csv_path

    except Exception as e:
        print(f'ZIP falló ({e}), intentando CSV directo...')
        # Intentar CSV directo
        try:
            csv_url = f'{BASE_URL}/SRI_RUC_{provincia}.csv'
            req = urllib.request.Request(csv_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=120) as resp:
                with open(csv_path, 'wb') as f:
                    f.write(resp.read())
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            print(f'{size_mb:.1f} MB (CSV directo)')
            return csv_path
        except Exception as e2:
            print(f'ERROR: {e2}')
            return None

    print('ERROR: CSV no encontrado después de extraer')
    return None


def procesar_csv(csv_path, conn, batch_size=5000):
    """Lee un CSV del SRI y hace UPSERT en SRI_Catastro_RUC."""
    cur = conn.cursor()
    count = 0
    errores = 0

    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='|')

        batch = {}  # dict por RUC para deduplicar (un RUC tiene multiples establecimientos)
        for row in reader:
            ruc = (row.get('NUMERO_RUC') or '').strip()
            if not ruc or len(ruc) != 13:
                errores += 1
                continue

            # Solo tomar el primer establecimiento de cada RUC
            if ruc in batch:
                continue

            # Parsear fecha
            fecha_inicio = None
            fi_raw = (row.get('FECHA_INICIO_ACTIVIDADES') or '').strip()
            if fi_raw:
                try:
                    fecha_inicio = fi_raw[:10]  # YYYY-MM-DD
                except Exception:
                    pass

            registro = (
                ruc,
                (row.get('RAZON_SOCIAL') or '').strip(),
                (row.get('ESTADO_CONTRIBUYENTE') or '').strip(),
                (row.get('TIPO_CONTRIBUYENTE') or '').strip(),
                (row.get('CLASE_CONTRIBUYENTE') or 'GEN').strip(),
                sn_a_sino(row.get('OBLIGADO')),
                sn_a_sino(row.get('AGENTE_RETENCION')),
                sn_a_sino(row.get('ESPECIAL')),
                (row.get('ACTIVIDAD_ECONOMICA') or '').strip(),
                fecha_inicio,
            )
            batch[ruc] = registro

            if len(batch) >= batch_size:
                _insertar_batch(cur, list(batch.values()))
                count += len(batch)
                batch = {}

        if batch:
            _insertar_batch(cur, list(batch.values()))
            count += len(batch)

    conn.commit()
    cur.close()
    return count, errores


def _insertar_batch(cur, batch):
    """Inserta un lote de registros con UPSERT."""
    args_str = ','.join(
        cur.mogrify(
            '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            r
        ).decode('utf-8') for r in batch
    )
    cur.execute(f'''
        INSERT INTO public."SRI_Catastro_RUC"
            (ruc, razon_social, estado, tipo_persona, regimen,
             obligado_contabilidad, agente_retencion, contribuyente_especial,
             actividad_economica, fecha_inicio_actividades)
        VALUES {args_str}
        ON CONFLICT (ruc) DO UPDATE SET
            razon_social            = EXCLUDED.razon_social,
            estado                  = EXCLUDED.estado,
            tipo_persona            = EXCLUDED.tipo_persona,
            regimen                 = EXCLUDED.regimen,
            obligado_contabilidad   = EXCLUDED.obligado_contabilidad,
            agente_retencion        = EXCLUDED.agente_retencion,
            contribuyente_especial  = EXCLUDED.contribuyente_especial,
            actividad_economica     = EXCLUDED.actividad_economica,
            fecha_inicio_actividades= EXCLUDED.fecha_inicio_actividades,
            fecha_carga             = CURRENT_TIMESTAMP
    ''')


def cargar_grandes_contribuyentes(conn):
    """Descarga el catastro de Grandes Contribuyentes y marca en la tabla."""
    print('\n[GRANDES CONTRIBUYENTES]')
    try:
        import openpyxl
    except ImportError:
        print('  AVISO: instala openpyxl para cargar grandes contribuyentes (pip install openpyxl)')
        return 0

    xlsx_path = os.path.join(DIR_DESCARGAS, 'Grandes_Contribuyentes.xlsx')
    if not os.path.exists(xlsx_path):
        print('  Descargando catastro...', end=' ', flush=True)
        try:
            req = urllib.request.Request(URL_GRANDES, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=60) as resp:
                with open(xlsx_path, 'wb') as f:
                    f.write(resp.read())
            print('OK')
        except Exception as e:
            print(f'ERROR: {e}')
            return 0

    # Leer el Excel
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb.active

    rucs_grandes = set()
    for row in ws.iter_rows(min_row=2, values_only=True):
        # Buscar columna con RUC (13 digitos)
        for cell in row:
            val = str(cell).strip() if cell else ''
            if len(val) == 13 and val.isdigit():
                rucs_grandes.add(val)
                break

    wb.close()

    if not rucs_grandes:
        print('  No se encontraron RUCs en el archivo')
        return 0

    # Marcar en la tabla
    cur = conn.cursor()
    # Primero resetear todos
    cur.execute('UPDATE public."SRI_Catastro_RUC" SET gran_contribuyente = %s', ('NO',))

    # Marcar los grandes
    for ruc in rucs_grandes:
        cur.execute(
            'UPDATE public."SRI_Catastro_RUC" SET gran_contribuyente = %s WHERE ruc = %s',
            ('SI', ruc)
        )

    conn.commit()
    cur.close()
    print(f'  [OK] {len(rucs_grandes)} grandes contribuyentes marcados')
    return len(rucs_grandes)


def actualizar_proveedores_foodix(conn):
    """Actualiza GFC-Prov-Proveedores con datos del catastro SRI."""
    print('\n[ACTUALIZAR PROVEEDORES FOODIX]')
    cur = conn.cursor()

    cur.execute('''
        UPDATE public."GFC-Prov-Proveedores" p
        SET
            razon_social            = COALESCE(c.razon_social, p.razon_social),
            tipo_persona            = COALESCE(c.tipo_persona, p.tipo_persona),
            regimen                 = COALESCE(c.regimen, p.regimen),
            obligado_contabilidad   = COALESCE(c.obligado_contabilidad, p.obligado_contabilidad),
            agente_retencion        = COALESCE(c.agente_retencion, p.agente_retencion),
            contribuyente_especial  = COALESCE(c.contribuyente_especial, p.contribuyente_especial),
            gran_contribuyente      = COALESCE(c.gran_contribuyente, p.gran_contribuyente),
            estado                  = COALESCE(c.estado, p.estado),
            actividad_economica     = COALESCE(c.actividad_economica, p.actividad_economica),
            fecha_actualizacion     = CURRENT_TIMESTAMP
        FROM public."SRI_Catastro_RUC" c
        WHERE p.ruc = c.ruc
    ''')

    updated = cur.rowcount
    conn.commit()
    cur.close()
    print(f'  [OK] {updated} proveedores FOODIX actualizados con datos del catastro SRI')
    return updated


def main():
    solo_actualizar = '--solo-actualizar' in sys.argv

    print('=' * 60)
    print('  CARGA CATASTRO SRI - DATOS ABIERTOS')
    print(f'  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)

    conn = get_conn()
    print('[OK] Conexion a BD establecida')

    crear_tabla_catastro(conn)

    total_registros = 0
    total_errores = 0
    provincias_ok = 0

    if not solo_actualizar:
        print(f'\n[DESCARGA] {len(PROVINCIAS)} provincias desde descargas.sri.gob.ec')
        print('-' * 60)

        for i, prov in enumerate(PROVINCIAS, 1):
            print(f'\n[{i}/{len(PROVINCIAS)}] {prov}')

            csv_path = descargar_provincia(prov)
            if not csv_path:
                print(f'  SALTANDO {prov}')
                continue

            count, errores = procesar_csv(csv_path, conn)
            total_registros += count
            total_errores += errores
            provincias_ok += 1
            print(f'  [BD] {count:,} registros cargados ({errores} errores)')

        # Grandes Contribuyentes
        cargar_grandes_contribuyentes(conn)

    # Actualizar proveedores FOODIX
    actualizar_proveedores_foodix(conn)

    # Estadisticas finales
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM public."SRI_Catastro_RUC"')
    total_catastro = cur.fetchone()[0]
    cur.close()

    conn.close()

    print('\n' + '=' * 60)
    print('  RESUMEN')
    print('=' * 60)
    if not solo_actualizar:
        print(f'  Provincias procesadas:  {provincias_ok}/{len(PROVINCIAS)}')
        print(f'  Registros cargados:     {total_registros:,}')
        print(f'  Errores/descartados:    {total_errores:,}')
    print(f'  Total en catastro BD:   {total_catastro:,}')
    print(f'  Hora fin:               {datetime.now().strftime("%H:%M:%S")}')
    print('=' * 60)


if __name__ == '__main__':
    main()
