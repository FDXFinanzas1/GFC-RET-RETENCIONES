"""
cargar_exportadores.py
======================
Carga el catastro de exportadores habituales de bienes del SRI
en la tabla SRI_Catastro_RUC y GFC-Prov-Proveedores.

Los exportadores habituales de bienes NO están sujetos a retención de IVA
(Resolución NAC-DGERCGC20-00000061). El IR SÍ se retiene normalmente.

Uso:
  python cargar_exportadores.py
"""

import os
import sys
import psycopg2

DB_CONFIG = {
    'host': 'chiosburguer.postgres.database.azure.com',
    'port': 5432,
    'dbname': 'Chios',
    'user': 'adminChios',
    'password': 'Burger2023',
    'sslmode': 'require'
}

XLS_PATH = r'C:\Users\ASUS\Downloads\Catastro_de_exportadores_bienes.xls'


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def leer_rucs_exportadores(path):
    """Lee el XLS del SRI y retorna set de RUCs válidos."""
    try:
        import xlrd
    except ImportError:
        print('ERROR: instala xlrd (pip install xlrd)')
        sys.exit(1)

    wb = xlrd.open_workbook(path)
    ws = wb.sheet_by_index(0)

    rucs = set()
    # Headers en fila 3 (0-indexed), datos desde fila 4
    for r in range(4, ws.nrows):
        ruc = str(ws.cell_value(r, 0)).strip()
        if len(ruc) == 13 and ruc.isdigit():
            rucs.add(ruc)

    return rucs


def agregar_columna_si_no_existe(conn, tabla, columna):
    """Agrega columna VARCHAR(3) DEFAULT 'NO' si no existe."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = '{tabla}'
          AND column_name = '{columna}'
    """)
    if not cur.fetchone():
        cur.execute(f'ALTER TABLE public."{tabla}" ADD COLUMN {columna} VARCHAR(3) DEFAULT %s', ('NO',))
        conn.commit()
        print(f'  [OK] Columna {columna} agregada a {tabla}')
    else:
        print(f'  [OK] Columna {columna} ya existe en {tabla}')
    cur.close()


def marcar_exportadores(conn, rucs):
    """Marca exportador_bienes = SI en SRI_Catastro_RUC y GFC-Prov-Proveedores.
    Usa UPDATE con ANY(array) para hacer batch en una sola query."""
    cur = conn.cursor()
    rucs_list = list(rucs)

    # Marcar exportadores en GFC-Prov-Proveedores (tabla pequeña ~2K, batch directo)
    print('  Actualizando GFC-Prov-Proveedores...')
    cur.execute(
        'UPDATE public."GFC-Prov-Proveedores" SET exportador_bienes = %s WHERE ruc = ANY(%s) AND COALESCE(exportador_bienes, %s) != %s',
        ('SI', rucs_list, 'NO', 'SI')
    )
    marcados_prov = cur.rowcount
    conn.commit()
    print(f'  [OK] Proveedores: {marcados_prov} marcados')

    # Marcar exportadores en SRI_Catastro_RUC (tabla grande 6.6M, batch por chunks)
    print('  Actualizando SRI_Catastro_RUC (puede tardar)...')
    marcados_catastro = 0
    chunk_size = 200
    for i in range(0, len(rucs_list), chunk_size):
        chunk = rucs_list[i:i+chunk_size]
        cur.execute(
            'UPDATE public."SRI_Catastro_RUC" SET exportador_bienes = %s WHERE ruc = ANY(%s) AND COALESCE(exportador_bienes, %s) != %s',
            ('SI', chunk, 'NO', 'SI')
        )
        marcados_catastro += cur.rowcount
        conn.commit()
        done = min(i + chunk_size, len(rucs_list))
        print(f'    [{done}/{len(rucs_list)}] chunks procesados...', flush=True)

    cur.close()
    return marcados_catastro, marcados_prov


def main():
    if not os.path.exists(XLS_PATH):
        print(f'ERROR: No se encontró el archivo: {XLS_PATH}')
        sys.exit(1)

    print('=' * 60)
    print('  CARGA CATASTRO EXPORTADORES HABITUALES DE BIENES')
    print('  Resolución NAC-DGERCGC20-00000061')
    print('=' * 60)

    # Leer RUCs del XLS
    print('\n[1] Leyendo archivo XLS...')
    rucs = leer_rucs_exportadores(XLS_PATH)
    print(f'  [OK] {len(rucs)} RUCs de exportadores encontrados')

    # Conectar BD
    print('\n[2] Conectando a BD...')
    conn = get_conn()
    print('  [OK] Conexión establecida')

    # Agregar columna si no existe
    print('\n[3] Verificando columnas...')
    agregar_columna_si_no_existe(conn, 'SRI_Catastro_RUC', 'exportador_bienes')
    agregar_columna_si_no_existe(conn, 'GFC-Prov-Proveedores', 'exportador_bienes')

    # Marcar exportadores
    print('\n[4] Marcando exportadores...')
    catastro, proveedores = marcar_exportadores(conn, rucs)
    print(f'  [OK] SRI_Catastro_RUC: {catastro} exportadores marcados')
    print(f'  [OK] GFC-Prov-Proveedores: {proveedores} exportadores marcados')

    conn.close()

    print('\n' + '=' * 60)
    print('  RESUMEN')
    print('=' * 60)
    print(f'  RUCs en archivo XLS:     {len(rucs)}')
    print(f'  Marcados en catastro:    {catastro}')
    print(f'  Marcados en proveedores: {proveedores}')
    print(f'  Regla: NO retener IVA / IR SI se retiene')
    print('=' * 60)


if __name__ == '__main__':
    main()
