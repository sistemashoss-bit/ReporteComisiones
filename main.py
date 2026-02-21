from flask import Flask, request, jsonify
import gspread
import pandas as pd
import numpy as np
import re
import unicodedata
import duckdb
import traceback
import sys
from google.auth import default

app = Flask(__name__)

creds, _ = default()
gc = gspread.authorize(creds)

# ── Cache DuckDB ──────────────────────────────────────────────────────
_con = None
_last_spreadsheet_id = None


def get_con(spreadsheet_id, sheet_name, fecha_ini=None, fecha_fin=None, force_reload=False):
    global _con, _last_spreadsheet_id

    if _con is None or force_reload or _last_spreadsheet_id != spreadsheet_id:
        df = procesar_ventas(spreadsheet_id, sheet_name, fecha_ini, fecha_fin)
        _con = duckdb.connect()
        _con.register('ventas', df)
        _last_spreadsheet_id = spreadsheet_id
        print(f"DuckDB cargado con {len(df)} filas", file=sys.stderr)

    return _con


# ── Helpers ───────────────────────────────────────────────────────────
def escribir_en_sheets(spreadsheet_id, sheet_name, df):
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=5000, cols=50)
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
    print(f"Escrito en '{sheet_name}': {len(df)} filas", file=sys.stderr)


# ── Procesamiento ─────────────────────────────────────────────────────
def procesar_ventas(spreadsheet_id, sheet_name, fecha_ini=None, fecha_fin=None):
     # Obtener gid de la hoja por nombre
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(sheet_name)
    gid = ws.id
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={gid}"
    df = pd.read_csv(url, dtype=str)
    df = df.drop_duplicates()

    df['Cuenta de Depósito'] = df['Cuenta de Depósito'].replace('', 'No puso Cuenta').fillna('No puso Cuenta')

    df.rename(columns={
        "Descripción de Producto o Servicio Vendido": "NotaVenta",
        "Cantidad por Concepto de Venta": "Total",
        "Persona, Método y Fecha de Confirmación de Pago": "Confirmacion Pago",
        "Importe Total de la Nota de Venta": "Importe Total"
    }, inplace=True)

    df['Folio'] = df['Folio'].fillna('0').apply(
        lambda x: str(int(float(x))) if str(x).replace('.', '').isdigit() else ''
    )

    df["Unidades Vendidas"] = (
        df["Unidades Vendidas"]
            .astype(str)
            .str.replace(r'^\s*\.\s*', '', regex=True)
            .str.strip()
            .str.split()
            .apply(lambda x: ','.join([v for v in x if v.isdigit()]))
    )

    df["Total"] = df["Total"].astype(str).str.replace(r"\s+", ",", regex=True)

    def limpiar_descripcion(texto):
        lineas = str(texto).split("\n")[1:]
        articulos = []
        for linea in lineas:
            linea = linea.strip()
            if not linea:
                continue
            if re.match(r'^(Descuento:|Seguro|instalacion|Abono|Anticipo)', linea, re.IGNORECASE):
                continue
            articulos.append(linea)
        return ",".join(articulos)

    df["NotaVenta"] = df["NotaVenta"].apply(limpiar_descripcion)

    df["Sucursal"] = (
        df["Sucursal y Método de Venta"]
            .str.extract(r"Sucursal:\s*(.*?)\n", expand=False)
            .str.strip()
    )
    df["Metodo de Venta"] = (
        df["Sucursal y Método de Venta"]
            .str.extract(r"Método de Venta:\s*(.*)", expand=False)
            .str.strip()
    )
    df.drop(columns=["Sucursal y Método de Venta"], inplace=True)

    def validar_coherencia(row):
        unidades = [u for u in row["Unidades Vendidas"].split(",") if u.strip()]
        articulos = [a for a in row["NotaVenta"].split(",") if a.strip()]
        n = min(len(unidades), len(articulos))
        return pd.Series([",".join(unidades[:n]), ",".join(articulos[:n])])

    df[["Unidades Vendidas", "NotaVenta"]] = df.apply(validar_coherencia, axis=1)

    df['Total'] = df['Total'].str.lstrip(',').apply(
        lambda x: x.replace('.00', '').replace(',', '').lstrip(',').replace('$', ',').lstrip(',')
    )
    df['Total'] = df.apply(
        lambda row: ','.join(row['Total'].split(',')[:len(row['NotaVenta'].split(',')) * 2]),
        axis=1
    )

    cols = df.columns.tolist()
    cols.remove('Sucursal')
    cols.remove('Metodo de Venta')
    idx = cols.index('Cliente') + 1
    cols = cols[:idx] + ['Sucursal', 'Metodo de Venta'] + cols[idx:]
    df = df[cols]

    def calcular_total(row):
        unidades = row['Unidades Vendidas'].split(',')
        totales  = row['Total'].split(',')
        def to_int(val):
            return int(''.join(filter(str.isdigit, val)) or 0)
        resultados = []
        for i, _ in enumerate(unidades):
            idx       = i * 2
            precio    = to_int(totales[idx])     if idx     < len(totales) else 0
            descuento = to_int(totales[idx + 1]) if idx + 1 < len(totales) else 0
            resultados.append(precio - descuento)
        return ','.join(str(r) for r in resultados)

    df['Total'] = df.apply(calcular_total, axis=1)

    def aplanar_df(df):
        filas = []

        for _, row in df.iterrows():
            unidades  = [u.strip() for u in str(row['Unidades Vendidas']).split(',') if u.strip()]
            articulos = [a.strip() for a in str(row['NotaVenta']).split(',') if a.strip()]
            totales   = [t.strip() for t in str(row['Total']).split(',') if t.strip()]

            n = min(len(unidades), len(articulos), len(totales))

            for i in range(n):
                cantidad = int(unidades[i]) if unidades[i].isdigit() else 1
                total    = int(''.join(filter(str.isdigit, totales[i])) or 0)

                # Expandir por cantidad
                for _ in range(cantidad):
                    fila = row.to_dict()
                    fila['Unidades Vendidas'] = 1
                    fila['NotaVenta']         = articulos[i]
                    fila['Total']             = total
                    filas.append(fila)

        return pd.DataFrame(filas)

    df = aplanar_df(df)

    df['Total']         = pd.to_numeric(df['Total'], errors='coerce')
    df['Pago Recibido'] = pd.to_numeric(df['Pago Recibido'], errors='coerce')
    df = df[(df['Total'] != 0) & (df['Pago Recibido'] != 0)]

    df['Metodo de Venta'] = df['Metodo de Venta'].str.rstrip('.').apply(
        lambda x: 'Hoss Center' if str(x).strip().lower().startswith('h') else 'Sucursal'
    )

    def normalizar_sucursal(valor):
        if pd.isna(valor):
            return 'Sin Especificar'
        valor = str(valor).strip()
        valor = ''.join(c for c in unicodedata.normalize('NFD', valor) if unicodedata.category(c) != 'Mn')
        valor = valor.replace(',', '').replace('.', '')
        match = re.match(r'^(\d+)\s+(.*)', valor)
        if match:
            return f"{match.group(1).zfill(2)} {match.group(2).strip().title()}"
        return valor

    df['Sucursal'] = df['Sucursal'].apply(normalizar_sucursal)
    df['Articulo'] = df['NotaVenta'].apply(
        lambda x: 'Instalacion' if str(x).strip().lower().startswith('instalac') else 'Puerta'
    )

    df['Articulo'] = df.apply(
        lambda row: 'Chapa Digital'
        if str(row['NotaVenta']).lower().startswith('chapa')
        else row['Articulo'],
        axis=1
        )

    # Crear máscara para Tipo de Pago == "Complemento"
    mask = df["Tipo de Pago"] == "Complemento"

    df["Fecha de captura"] = pd.to_datetime(df["Fecha de captura"])
    df["Fecha de venta"] = pd.to_datetime(df["Fecha de venta"])

    # Obtener la fecha mayor por fila
    fecha_mayor = df.loc[mask, ["Fecha de captura", "Fecha de venta"]].max(axis=1)

    # Asignar la fecha mayor a ambas columnas
    df.loc[mask, "Fecha de captura"] = fecha_mayor
    df.loc[mask, "Fecha de venta"] = fecha_mayor
    df.drop(columns=['Saldo Restante','Fecha de captura','Importe Total'], inplace=True)

    

    if fecha_ini and fecha_fin:
        df['Fecha de venta'] = pd.to_datetime(df['Fecha de venta'], errors='coerce')
        df = df[
            (df['Fecha de venta'] >= pd.to_datetime(fecha_ini)) &
            (df['Fecha de venta'] <= pd.to_datetime(fecha_fin))
        ]

    print(f"Ventas procesadas: {len(df)} filas", file=sys.stderr)
    return df
     




# ── Endpoints ─────────────────────────────────────────────────────────

@app.route('/run-multi', methods=['POST'])
def run_multi():
    try:
        data = request.get_json()
        print(f"Payload /run-multi: {data}", file=sys.stderr)

        spreadsheet_id = data['spreadsheet_base_id']   # mismo sheet para base y reporte
        sheet_base     = data.get('sheet_base', 'Global')
        sheet_hoss     = data.get('sheet_reporte2', 'Hoss Center')
        sheet_suc      = data.get('sheet_reporte1', 'Sucursales')
        fecha_ini      = data.get('fecha_ini')
        fecha_fin      = data.get('fecha_fin')
        tipo           = data.get('tipo', 'ambos')
        force_reload   = data.get('force_reload', True)

        con = get_con(spreadsheet_id, sheet_base, fecha_ini, fecha_fin, force_reload)
        df  = con.execute("SELECT * FROM ventas").df()

        if tipo in ('Hoss Center', 'ambos'):
            escribir_en_sheets(spreadsheet_id, sheet_hoss, df[df['Metodo de Venta'] == 'Hoss Center'])

        if tipo in ('Sucursal', 'ambos'):
            escribir_en_sheets(spreadsheet_id, sheet_suc, df[df['Metodo de Venta'] == 'Sucursal'])

        return jsonify({"status": "ok", "filas": len(df)}), 200

    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify({"status": "error", "mensaje": str(e)}), 500


@app.route('/query', methods=['POST'])
def query():
    try:
        data = request.get_json()
        print(f"Payload /query: {data}", file=sys.stderr)

        spreadsheet_id = data['spreadsheet_base_id']
        sheet_base     = data.get('sheet_base', 'Global')
        sheet_destino  = data.get('sheet_destino')       # opcional
        sql            = data['query']
        fecha_ini      = data.get('fecha_ini')
        fecha_fin      = data.get('fecha_fin')
        force_reload   = data.get('force_reload', False)

        con       = get_con(spreadsheet_id, sheet_base, fecha_ini, fecha_fin, force_reload)
        resultado = con.execute(sql).df()

        if sheet_destino:
            escribir_en_sheets(spreadsheet_id, sheet_destino, resultado)

        return jsonify({
            "status":   "ok",
            "filas":    len(resultado),
            "columnas": resultado.columns.tolist(),
            "data":     resultado.astype(str).values.tolist()
        }), 200

    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify({"status": "error", "mensaje": str(e)}), 500


if __name__ == '__main__':
    import os
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))