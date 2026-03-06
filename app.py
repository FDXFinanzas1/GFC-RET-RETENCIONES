from flask import Flask, render_template, request, jsonify
import psycopg2
import os

app = Flask(__name__)

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'chiosburguer.postgres.database.azure.com'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'dbname': os.environ.get('DB_NAME', 'Chios'),
    'user': os.environ.get('DB_USER', 'adminChios'),
    'password': os.environ.get('DB_PASSWORD', 'Burger2023'),
    'sslmode': 'require'
}

# Tabla de conceptos IR 2026 - Resolucion NAC-DGERCGC26-00000009
# (grupo, cod, descripcion, pct_ir, tipo_defecto)
CONCEPTOS = [
    ('RIMPE',                   '332',        'RIMPE Negocios Populares (comprobante preimpreso)',                                     0.0,  'ambos'),
    ('RIMPE',                   '343',        'RIMPE Emprendedores',                                                                   1.0,  'ambos'),
    ('Bienes y Transporte',     '312A',       'Bienes agricolas — PRODUCTOR DIRECTO (carne, pollo, hortalizas del campo)',             1.0,  'bien'),
    ('Bienes y Transporte',     '310',        'Transporte de carga / pasajeros (flete, delivery)',                                     1.0,  'servicio'),
    ('Bienes y Transporte',     '312C',       'Bienes agricolas — COMERCIALIZADOR (distribuidora, no productor directo)',              1.75, 'bien'),
    ('Bienes y Transporte',     '312',        'Bienes muebles — insumos, bebidas, materiales',           2.0,  'bien'),
    ('Servicios Generales',     '343A',       'Energia electrica (CNEL, EEQ)',                                                         2.0,  'servicio'),
    ('Servicios Generales',     '322',        'Seguros y reaseguros',                                                                  2.0,  'servicio'),
    ('Servicios Generales',     '307',        'Servicios PN mano de obra (tecnico, mantenimiento, limpieza, operario)',                3.0,  'servicio'),
    ('Servicios Generales',     '309',        'Publicidad y medios de comunicacion (anuncios, agencias)',                              3.0,  'servicio'),
    ('Servicios Generales',     '346',        'Otros pagos sin porcentaje especifico (servicios varios no clasificados)',              3.0,  'ambos'),
    ('Especiales',              '311',        'Liquidacion de compra — productor rural sin RUC (IR 3% + IVA 100%)',                   3.0,  'bien'),
    ('Especiales',              'COMBUSTIBLE','Combustible — NO retiene IR ni IVA',                                                   0.0,  'bien'),
    ('Honorarios y Profesionales','3030',     'Servicios profesionales por SOCIEDADES (estudio contable, asesoria legal S.A.)',        5.0,  'servicio'),
    ('Honorarios y Profesionales','3482',     'Comisiones pagadas a SOCIEDADES',                                                       5.0,  'servicio'),
    ('Honorarios y Profesionales','303',      'Honorarios PN con titulo profesional (contador, abogado, consultor)',                   10.0, 'servicio'),
    ('Honorarios y Profesionales','304',      'Servicios PN intelecto sin titulo (asesor, analista, consultor)',                       10.0, 'servicio'),
    ('Honorarios y Profesionales','304E',     'Honorarios docencia PN (instructor, capacitador)',                                      10.0, 'servicio'),
    ('Arrendamiento',           '320',        'Arrendamiento bienes inmuebles (local comercial, cualquier modalidad)',                 10.0, 'servicio'),
]

CONCEPTOS_DICT = {
    c[1]: {'grupo': c[0], 'cod': c[1], 'desc': c[2], 'pct_ir': c[3], 'tipo': c[4]}
    for c in CONCEPTOS
}

def calcular_pct_iva(tipo_persona, contrib_especial, obligado, regimen, concepto_cod, tipo_compra, gran_contribuyente='NO'):
    """Retorna el % de retención de IVA según proveedor y concepto.

    FOODIX es SOCIEDAD — puede retener IVA a cualquier proveedor excepto:
      - Grandes Contribuyentes → 0% IVA (y 0% IR)
      - Contribuyentes Especiales → 0% IVA
      - Combustible, RIMPE Negocios Populares (332) → 0% IVA
    """
    # Prioridad 1: Gran Contribuyente — no retiene IVA ni IR
    if gran_contribuyente == 'SI':               return 0

    # Prioridad 2: casos especiales por concepto (independiente del proveedor)
    if concepto_cod == '311':                    return 100  # Liquidacion de compra: 100% siempre
    if concepto_cod in ('303', '304', '304E'):   return 100  # Honorarios PN: siempre 100%
    if concepto_cod == '320':                    return 100  # Arrendamiento: siempre 100%
    if concepto_cod in ('COMBUSTIBLE', '332'):   return 0    # Combustible / RIMPE NP: no retiene

    # Prioridad 3: Contribuyente Especial — no retiene IVA
    if contrib_especial == 'SI':                 return 0

    # Prioridad 4: FOODIX retiene IVA a todos los demas (Sociedades, PN Obligada, PN No Obligada, RIMPE)
    # Bienes: 30%, Servicios: 70%
    return 30 if tipo_compra == 'bien' else 70


def calcular_pct_ir(pct_ir_base, gran_contribuyente='NO'):
    """Gran Contribuyente: no se retiene IR."""
    if gran_contribuyente == 'SI':
        return 0.0
    return pct_ir_base

def get_conn():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception:
        cfg = DB_CONFIG.copy(); cfg['host'] = '4.246.223.171'
        return psycopg2.connect(**cfg)

@app.route('/')
def index():
    grupos = {}
    for c in CONCEPTOS:
        g = c[0]
        if g not in grupos:
            grupos[g] = []
        grupos[g].append({'cod': c[1], 'desc': c[2], 'pct_ir': c[3], 'tipo': c[4]})
    return render_template('index.html', grupos=grupos, conceptos_dict=CONCEPTOS_DICT)

@app.route('/api/proveedor/<ruc>')
def buscar_proveedor(ruc):
    ruc = ruc.strip()
    if len(ruc) != 13 or not ruc.isdigit():
        return jsonify({'error': 'RUC invalido — debe tener exactamente 13 digitos numericos'}), 400
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute('''
            SELECT ruc, razon_social, tipo_persona, regimen,
                   contribuyente_especial, obligado_contabilidad,
                   agente_retencion, estado, actividad_economica, gran_contribuyente
            FROM public.proveedores_sri WHERE ruc = %s
        ''', (ruc,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return jsonify({'error': 'RUC no encontrado en el registro de proveedores FOODIX'}), 404
        return jsonify({
            'ruc': row[0], 'razon_social': row[1], 'tipo_persona': row[2],
            'regimen': row[3], 'contribuyente_especial': row[4],
            'obligado_contabilidad': row[5], 'agente_retencion': row[6],
            'estado': row[7], 'actividad_economica': row[8],
            'gran_contribuyente': row[9] or 'NO',
        })
    except Exception as e:
        return jsonify({'error': f'Error de conexion: {str(e)[:120]}'}), 500

@app.route('/api/registrar_ruc', methods=['POST'])
def registrar_ruc():
    d = request.get_json()
    ruc = (d.get('ruc') or '').strip()
    if len(ruc) != 13 or not ruc.isdigit():
        return jsonify({'error': 'RUC invalido'}), 400
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute('''
            INSERT INTO public.proveedores_sri (ruc)
            VALUES (%s)
            ON CONFLICT (ruc) DO NOTHING
        ''', (ruc,))
        inserted = cur.rowcount
        conn.commit(); cur.close(); conn.close()
        if inserted:
            return jsonify({'ok': True, 'msg': 'RUC registrado. El sistema completará los datos automáticamente.'})
        else:
            return jsonify({'ok': False, 'msg': 'El RUC ya existe en la base de datos.'})
    except Exception as e:
        return jsonify({'error': f'Error al registrar: {str(e)[:120]}'}), 500

@app.route('/api/calcular', methods=['POST'])
def calcular():
    d = request.get_json()
    subtotal          = float(d.get('subtotal', 0) or 0)
    iva_valor         = float(d.get('iva_valor', 0) or 0)
    concepto_cod      = d.get('concepto_cod', '')
    tipo_compra       = d.get('tipo_compra', 'bien')
    tipo_persona      = (d.get('tipo_persona') or '').upper()
    contrib_especial  = (d.get('contribuyente_especial') or '').upper()
    obligado          = (d.get('obligado_contabilidad') or '').upper()
    regimen           = (d.get('regimen') or '').upper()
    gran_contribuyente= (d.get('gran_contribuyente') or 'NO').upper()

    if subtotal <= 0:
        return jsonify({'error': 'El subtotal debe ser mayor a 0'}), 400
    concepto = CONCEPTOS_DICT.get(concepto_cod)
    if not concepto:
        return jsonify({'error': 'Seleccione un concepto de pago valido'}), 400

    pct_ir  = calcular_pct_ir(concepto['pct_ir'], gran_contribuyente)
    pct_iva = calcular_pct_iva(tipo_persona, contrib_especial, obligado, regimen, concepto_cod, tipo_compra, gran_contribuyente)

    ret_ir        = round(subtotal * pct_ir / 100, 2)
    ret_iva       = round(iva_valor * pct_iva / 100, 2)
    total_factura = round(subtotal + iva_valor, 2)
    total_pagar   = round(total_factura - ret_ir - ret_iva, 2)

    return jsonify({
        'subtotal': subtotal, 'iva_valor': iva_valor,
        'total_factura': total_factura,
        'concepto_desc': concepto['desc'], 'concepto_cod': concepto_cod,
        'pct_ir': pct_ir, 'ret_ir': ret_ir,
        'pct_iva': pct_iva, 'ret_iva': ret_iva,
        'total_pagar': total_pagar, 'tipo_compra': tipo_compra,
        'gran_contribuyente': gran_contribuyente,
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
