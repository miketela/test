#!/usr/bin/env python3
"""Generate synthetic AT12 source files for local development."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


BASE_DATE = "20250131"
RUN_ID = "202501"
OUTPUT_DIR = Path("source")
SCHEMA_PATH = Path("schemas/AT12/schema_headers.json")


def load_schema() -> Dict[str, List[str]]:
    data = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return {name: list(columns.keys()) for name, columns in data.items()}


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fmt_money(amount: float) -> str:
    return f"{amount:0.2f}"


def build_base_dataframe(columns: List[str]) -> pd.DataFrame:
    extra_columns = {"Nombre_fiduciaria", "Id_Fiduciaria", "Descripción de la Garantía"}
    all_columns = list(dict.fromkeys(columns + list(extra_columns)))

    def base_row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "Fecha": BASE_DATE,
                "Codigo_Banco": "001",
                "Numero_Prestamo": loan_id,
                "Numero_Ruc_Garantia": f"RUC{loan_id[-6:]}",
                "Id_Fideicomiso": f"FID{loan_id[-4:]}",
                "Nombre_Fiduciaria": f"Fiduciaria Desarrollo {idx:02d}",
                "Nombre_fiduciaria": f"FID-{idx:02d}",
                "Id_Fiduciaria": f"ID{idx:04d}",
                "Origen_Garantia": "N",
                "Tipo_Garantia": "0101",
                "Tipo_Facilidad": "01",
                "Id_Documento": loan_id,
                "Nombre_Organismo": f"Organismo {idx:02d}",
                "Valor_Inicial": fmt_money(10000 + idx * 10),
                "Valor_Garantia": fmt_money(9000 + idx * 10),
                "Valor_Ponderado": fmt_money(8000 + idx * 10),
                "Tipo_Instrumento": "PIGNORACION",
                "Calificacion_Emisor": "A",
                "Calificacion_Emisision": "A",
                "Pais_Emision": "591",
                "Fecha_Ultima_Actualizacion": "20240115",
                "Fecha_Vencimiento": "20241231",
                "Tipo_Poliza": "02",
                "Codigo_Region": "101",
                "Clave_Pais": "24",
                "Clave_Empresa": "24",
                "Clave_Tipo_Garantia": "3",
                "Clave_Subtipo_Garantia": "61",
                "Clave_Tipo_Pren_Hipo": "NA",
                "Numero_Garantia": f"NG{loan_id[-6:]}",
                "Numero_Cis_Garantia": f"CIS{loan_id[-6:]}",
                "Numero_Cis_Prestamo": f"CISP{loan_id[-6:]}",
                "Numero_Ruc_Prestamo": f"RUCP{loan_id[-6:]}",
                "Moneda": "USD",
                "Importe": fmt_money(9000 + idx * 10),
                "Status_Garantia": "0",
                "Status_Prestamo": "0",
                "Flag_Val_Prestamo": "Y",
                "Marca_Duplicidad": "N",
                "Codigo_Origen": "001",
                "Segmento": "PRE",
                "Descripción de la Garantía": "Garantía comercial standard",
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []

    scenarios = [
        base_row(0, " 6000000001 ", {
            "Id_Documento": "  0000123456  ",
            "Nombre_Organismo": "  Organismo   con   espacios  ",
        }),
        base_row(1, "6000000002", {
            "Tipo_Garantia": "0301",
            "Id_Documento": "12345678901234567890",
        }),
        base_row(2, "6000000003", {
            "Tipo_Garantia": "0301",
            "Id_Documento": "7011234567",
        }),
        base_row(3, "6000000004", {
            "Tipo_Garantia": "0301",
            "Id_Documento": "0000000101",
        }),
        base_row(4, "6000000005", {
            "Id_Documento": "12,345,678",
        }),
        base_row(5, "6000000006", {
            "Nombre_Fiduciaria": "Fiduciaria Nacional",
            "Nombre_fiduciaria": "508",
            "Id_Fiduciaria": "508",
        }),
        base_row(6, "6000000007", {
            "Nombre_Fiduciaria": "FDE Global Partners",
            "Nombre_fiduciaria": "FDE Global Partners",
            "Origen_Garantia": "N",
            "Codigo_Region": "110",
        }),
        base_row(7, "6000000008", {
            "Descripción de la Garantía": "Contrato Privado de prenda",
            "Nombre_Organismo": "Consejo Empresarial",
        }),
        base_row(8, "6000000009", {
            "Tipo_Garantia": "0208",
            "Tipo_Poliza": "",
            "Nombre_Organismo": "",
        }),
        base_row(9, "6000000010", {
            "Tipo_Garantia": "0207",
            "Tipo_Poliza": "",
            "Nombre_Organismo": "",
        }),
        base_row(10, "6000000011", {
            "Tipo_Garantia": "0101",
            "Id_Documento": " ",
            "Importe": fmt_money(1500),
            "Valor_Garantia": fmt_money(1500),
            "Fecha_Ultima_Actualizacion": "20240105",
            "Fecha_Vencimiento": "20240131",
        }),
        base_row(11, "6000000012", {
            "Tipo_Garantia": "0103",
            "Id_Documento": "",
            "Importe": fmt_money(2200),
            "Valor_Garantia": fmt_money(2200),
        }),
        base_row(12, "6000000013", {
            "Tipo_Garantia": "0101",
            "Id_Documento": "",
            "Importe": fmt_money(1800),
            "Valor_Garantia": fmt_money(1800),
        }),
        base_row(13, "6000000014", {
            "Tipo_Garantia": "0207",
            "Nombre_Organismo": "",
        }),
        base_row(14, "6000000015", {
            "Tipo_Garantia": "0106",
            "Nombre_Organismo": "",
        }),
        base_row(15, "6000000016", {
            "Tipo_Facilidad": "02",
        }),
        base_row(16, "6000000017", {
            "Fecha_Vencimiento": "22000101",
        }),
        base_row(17, "6000000018", {
            "Fecha_Ultima_Actualizacion": "20250215",
        }),
        base_row(18, "6000000019", {
            "Valor_Inicial": "1,250.50",
        }),
        base_row(19, "6000000020", {
            "Valor_Garantia": "1.000,75",
        }),
    ]

    rows.extend(scenarios)

    index = len(rows)
    while len(rows) < 50:
        loan_id = f"600000{index+1:04d}"
        overrides = {
            "Tipo_Garantia": "0102" if index % 3 == 0 else "0101",
            "Tipo_Facilidad": "01" if index % 4 else "02",
            "Nombre_Fiduciaria": f"Fiduciaria Regional {index:02d}",
            "Nombre_fiduciaria": f"FR-{index:02d}",
        }
        rows.append(base_row(index, loan_id, overrides))
        index += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_tdc_dataframe(columns: List[str]) -> pd.DataFrame:
    extra_columns = {"Numero_Garantia", "LIMITE", "SALDO", "Nombre_fiduciaria", "Id_Fiduciaria"}
    all_columns = list(dict.fromkeys(columns + list(extra_columns)))

    def tdc_row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "Fecha": BASE_DATE,
                "Código_Banco": "001",
                "Número_Préstamo": loan_id,
                "Número_Ruc_Garantía": f"RUC{loan_id[-6:]}",
                "Id_Fideicomiso": f"FID{loan_id[-4:]}",
                "Nombre_Fiduciaria": f"TDC Fiduciaria {idx:02d}",
                "Nombre_fiduciaria": f"TD-{idx:02d}",
                "Id_Fiduciaria": f"TDCID{idx:04d}",
                "Origen_Garantía": "N",
                "Tipo_Garantía": "0507",
                "Tipo_Facilidad": "01",
                "Id_Documento": f"TDC{idx:06d}",
                "Nombre_Organismo": f"Org TDC {idx:02d}",
                "Valor_Inicial": fmt_money(5000 + idx * 10),
                "Valor_Garantía": fmt_money(4500 + idx * 8),
                "Valor_Ponderado": fmt_money(4000 + idx * 5),
                "Tipo_Instrumento": "TDC",
                "Calificación_Emisor": "A",
                "Calificación_Emisión": "A",
                "País_Emisión": "591",
                "Fecha_Última_Actualización": "20240110",
                "Fecha_Vencimiento": "20241210",
                "Tipo_Poliza": "NA",
                "Código_Región": "101",
                "Numero_Garantia": f"0000850{idx:03d}",
                "Moneda": "USD",
                "Importe": fmt_money(4500 + idx * 8),
                "Descripción de la Garantía": "Tarjeta Rotativa",
                "LIMITE": fmt_money(6000 + idx * 5),
                "SALDO": fmt_money(2000 + idx * 3),
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []

    # Scenario rows
    rows.append(
        tdc_row(0, "7000000001", {
            "Id_Documento": "1111111111",
            "Tipo_Facilidad": "02",
        })
    )
    rows.append(
        tdc_row(1, "7000000002", {
            "Id_Documento": "2222222222",
            "Tipo_Facilidad": "01",
            "Número_Garantía": "",
        })
    )
    rows.append(
        tdc_row(2, "7000000003", {
            "Id_Documento": "2222222222",
            "Tipo_Facilidad": "01",
        })
    )
    rows.append(
        tdc_row(3, "7000000004", {
            "Id_Documento": "3333333333",
            "Fecha_Última_Actualización": "20240105",
            "Fecha_Vencimiento": "20240131",
        })
    )

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"700000{idx+1:04d}"
        overrides = {
            "Tipo_Facilidad": "01" if idx % 2 == 0 else "02",
            "Id_Documento": f"TDCID{idx:06d}",
        }
        rows.append(tdc_row(idx, loan_id, overrides))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_sobregiro_dataframe(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def sob_row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "Fecha": BASE_DATE,
                "Codigo_Banco": "001",
                "Numero_Prestamo": loan_id,
                "Numero_Ruc_Garantia": f"RUC{loan_id[-6:]}",
                "Id_Fideicomiso": f"FID{loan_id[-4:]}",
                "Nombre_Fiduciaria": f"Sob Fid {idx:02d}",
                "Origen_Garantia": "N",
                "Tipo_Garantia": "0801",
                "Tipo_Facilidad": "02",
                "Id_Documento": f"SG{idx:06d}",
                "Nombre_Organismo": f"Sob Org {idx:02d}",
                "Valor_Inicial": fmt_money(2000 + idx * 5),
                "Valor_Garantia": fmt_money(1500 + idx * 5),
                "valor_ponderado": fmt_money(1200 + idx * 5),
                "Tipo_Instrumento": "LINEA",
                "Calificacion_Emisor": "B",
                "Calificacion_Emisision": "B",
                "Pais_Emision": "591",
                "Fecha_Ultima_Actualizacion": "20240120",
                "Fecha_Vencimiento": "20240720",
                "Tipo_Poliza": "NA",
                "Codigo_Region": "103",
                "Numero_Garantia": "",
                "Numero_Cis_Garantia": "",
                "Moneda": "USD",
                "Importe": fmt_money(1500 + idx * 5),
                "Codigo_Origen": "001",
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []

    rows.append(
        sob_row(0, "8000000001", {
            "Id_Documento": "  SGWHITESPACE001  ",
            "Nombre_Organismo": "  Overdraft   Test  ",
        })
    )
    rows.append(
        sob_row(1, "8000000002", {
            "Tipo_Facilidad": "01",
            "Id_Documento": "SGMAPPED001",
        })
    )
    rows.append(
        sob_row(2, "8000000003", {
            "Tipo_Facilidad": "02",
            "Id_Documento": "SGMAPPED002",
            "Fecha_Ultima_Actualizacion": "20240105",
            "Fecha_Vencimiento": "20240131",
        })
    )

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"800000{idx+1:04d}"
        overrides = {
            "Tipo_Facilidad": "01" if idx % 3 == 0 else "02",
            "Id_Documento": f"SG{idx:06d}",
        }
        rows.append(sob_row(idx, loan_id, overrides))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_valores_dataframe(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def valores_row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "Fecha": BASE_DATE,
                "Codigo_Banco": "001",
                "Numero_Prestamo": loan_id,
                "Numero_Ruc_Garantia": f"RUC{loan_id[-6:]}",
                "Id_Fideicomiso": f"FID{loan_id[-4:]}",
                "Nombre_Fiduciaria": f"Valores Fid {idx:02d}",
                "Origen_Garantia": "N",
                "Tipo_Garantia": "0507",
                "Tipo_Facilidad": "02",
                "Id_Documento": f"VAL{idx:06d}",
                "Nombre_Organismo": f"Valores Org {idx:02d}",
                "Valor_Inicial": fmt_money(3000 + idx * 5),
                "Valor_Garantia": fmt_money(3000 + idx * 5),
                "Valor_Ponderado": fmt_money(2800 + idx * 5),
                "Tipo_Instrumento": "BONO",
                "Calificacion_Emisor": "A",
                "Calificacion_Emisision": "A",
                "Pais_Emision": "591",
                "Fecha_Ultima_Actualizacion": "20240125",
                "Fecha_Vencimiento": "20260125",
                "Tipo_Poliza": "NA",
                "Codigo_Region": "104",
                "Clave_Pais": "24",
                "Clave_Empresa": "24",
                "Clave_Tipo_Garantia": "3",
                "Clave_Subtipo_Garantia": "61",
                "Clave_Tipo_Pren_Hipo": "NA",
                "Numero_Garantia": "",
                "Numero_Cis_Garantia": f"VCIS{loan_id[-6:]}",
                "Numero_Cis_Prestamo": f"VCISP{loan_id[-6:]}",
                "Numero_Ruc_Prestamo": f"VRUCP{loan_id[-6:]}",
                "Moneda": "USD",
                "Importe": fmt_money(3000 + idx * 5),
                "Status_Garantia": "0",
                "Status_Prestamo": "-1",
                "Codigo_Origen": "001",
                "Segmento": "PRE",
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []
    rows.append(
        valores_row(0, "9000000001", {
            "Id_Documento": "Linea Sobregiro de la cuenta 9000000001",
            "Tipo_Facilidad": "01",
        })
    )
    rows.append(
        valores_row(1, "9000000002", {
            "Tipo_Facilidad": "02",
        })
    )

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"900000{idx+1:04d}"
        overrides = {
            "Tipo_Facilidad": "01" if idx % 2 == 0 else "02",
        }
        rows.append(valores_row(idx, loan_id, overrides))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_garantia_autos_dataframe(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def auto_row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "fec_proceso": BASE_DATE,
                "numcred": loan_id,
                "acreditado": f"Cliente Auto {idx:02d}",
                "saldocapital": fmt_money(5000 + idx * 20),
                "producto": "AUTO",
                "dpd": "0",
                "num_poliza": f"AUTO-{loan_id[-4:]}",
                "monto_asegurado": fmt_money(2500 + idx * 10),
                "fec_ini_cob": "20231215",
                "fec_fin_cobe": "20241215",
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []
    rows.append(auto_row(0, "6000000011", {"num_poliza": "AUTO-XYZ-01"}))
    rows.append(auto_row(1, "6000000012", {"num_poliza": "AUTO-XYZ-02"}))
    rows.append(auto_row(2, "6000000013", {"num_poliza": "AUTO-XYZ-03", "monto_asegurado": "Nuevo Desembolso"}))

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"6{idx+1:09d}"
        rows.append(auto_row(idx, loan_id))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_poliza_hipotecaria(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def poliza_row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "fec_proceso": BASE_DATE,
                "numcred": loan_id,
                "acreditado": f"Hipotecario {idx:02d}",
                "saldocapital": fmt_money(8000 + idx * 50),
                "producto": "HIPOTECARIO",
                "dpd": "0",
                "seguro_incendio": "01" if idx % 2 == 0 else "02",
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []
    rows.append(poliza_row(0, "6000000010", {"seguro_incendio": "02"}))

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"6{idx+10:09d}"
        rows.append(poliza_row(idx, loan_id))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_at03_creditos(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def credit_row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "fec_proceso": BASE_DATE,
                "cod_banco": "001",
                "cod_subsidiaria": "001",
                "cod_preferencial": "N",
                "aplica_feci": "N",
                "tipo_credito": "COM",
                "facilidad_cred": "01",
                "clasif_prest": "A",
                "destino": "GENERAL",
                "cod_region": "101",
                "id_cliente": f"CL{idx:05d}",
                "tam_empresa": "MED",
                "cod_genero": "1",
                "num_cta": loan_id,
                "nombre_cliente": f"Cliente Crédito {idx:02d}",
                "valor_inicial": fmt_money(10000 + idx * 100),
                "intereses_x_cobrar": fmt_money(200 + idx),
                "fec_ini_prestamo": "20230510",
                "fec_vencto": "20251231",
                "saldo": fmt_money(9000 + idx * 90),
                "dias_mora": "0",
                "mto_a_pagar": fmt_money(500 + idx * 5),
                "cve_mes": "202501",
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []
    specific_loans = [
        "6000000016",  # removal candidate (Tipo_Facilidad 02)
        "6000000018",  # fecha avalúo correction
        "8000000002",  # Sobregiro Tipo_Facilidad mapping
        "8000000003",
        "9000000001",
        "7000000001",
        "7000000002",
    ]
    for idx, loan in enumerate(specific_loans):
        rows.append(credit_row(idx, loan))

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"{5000000000 + idx:010d}"
        rows.append(credit_row(idx, loan_id))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_at03_tdc() -> pd.DataFrame:
    columns = [
        "num_cta_tdc",
        "facilidad",
        "saldo",
        "cve_mes",
    ]

    def row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        data = {
            "num_cta_tdc": loan_id,
            "facilidad": "01",
            "saldo": fmt_money(4000 + idx * 40),
            "cve_mes": "202501",
        }
        if overrides:
            data.update(overrides)
        return data

    rows: List[Dict[str, str]] = []
    rows.append(row(0, "7000000001", {"facilidad": "01"}))
    rows.append(row(1, "7000000002", {"facilidad": "01"}))

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"{7100000000 + idx:010d}"
        rows.append(row(idx, loan_id, {"facilidad": "01" if idx % 2 == 0 else "02"}))
        idx += 1

    return pd.DataFrame(rows, columns=columns)


def build_at02_dataframe(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def at02_row(idx: int, account_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        row = {col: "" for col in all_columns}
        row.update(
            {
                "Fecha": BASE_DATE,
                "Cod_banco": "001",
                "Cod_Subsidiaria": "001",
                "Tipo_Deposito": "VISTA",
                "Tipo_Cliente": "PN",
                "Tasa": fmt_money(3.5),
                "Origen": "N",
                "Cod_region": "101",
                "Fecha_Inicio": "20240105",
                "Fecha_Vencimiento": "20240705",
                "Monto": fmt_money(1000 + idx * 5),
                "Monto_Pignorado": fmt_money(0),
                "Numero_renovacion": "1",
                "Fecha_Renovacion": "20250105",
                "Intereses_por_Pagar": fmt_money(50 + idx),
                "Periodicidad_pago_intereses": "M",
                "Identificacion_cliente": f"CI{idx:05d}",
                "Identificacion_Cuenta": account_id,
                "Actividad": "SERVICIOS",
                "Tamano_Empresa": "MED",
                "Genero": "M",
                "Beneficiario_declarado": "Y",
                "Estatus_actividad_movimiento": "ACTIVO",
                "Identificacion_cliente_2": f"CI2{idx:05d}",
                "Tipo_Producto": "CUENTA",
                "Subproducto": "CTA",
                "Fecha_proceso": BASE_DATE,
                "Moneda": "USD",
                "Importe": fmt_money(1000 + idx * 5),
                "Importe_por_pagar": fmt_money(0),
                "Segmento": "PRE",
            }
        )
        if overrides:
            row.update(overrides)
        return row

    rows: List[Dict[str, str]] = []
    rows.append(at02_row(0, "1111111111", {"Fecha_Inicio": "20240101", "Fecha_Vencimiento": "20241231"}))
    rows.append(at02_row(1, "2222222222", {"Fecha_Inicio": "20240201", "Fecha_Vencimiento": "20250131"}))
    rows.append(at02_row(2, "SGMAPPED001", {"Fecha_Inicio": "20240103", "Fecha_Vencimiento": "20240603"}))
    rows.append(at02_row(3, "SGMAPPED002", {"Fecha_Inicio": "20240104", "Fecha_Vencimiento": "20240604"}))

    idx = len(rows)
    while len(rows) < 50:
        account_id = f"AC{idx:06d}"
        rows.append(at02_row(idx, account_id))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def build_afectaciones(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        data = {col: "" for col in all_columns}
        data.update(
            {
                "info": "FUERA_CIERRE",
                "fec_corte": BASE_DATE,
                "at_num_prestamo": loan_id,
                "at_id_cliente": f"CL{idx:05d}",
                "at_num_cli": f"NC{idx:05d}",
                "at_num_cliente": f"NC{idx:05d}",
                "at_fecha_inicial_prestamo": "20240101",
                "at_tipo_operacion": "0101" if idx % 2 == 0 else "0301",
                "at_saldo": fmt_money(1000 + idx * 10),
            }
        )
        if overrides:
            data.update(overrides)
        return data

    rows = [row(i, f"600000{2000 + i:04d}") for i in range(50)]
    return pd.DataFrame(rows, columns=all_columns)


def build_valor_minimo(columns: List[str]) -> pd.DataFrame:
    all_columns = columns

    def row(idx: int, loan_id: str, overrides: Dict[str, str] | None = None) -> Dict[str, str]:
        data = {col: "" for col in all_columns}
        data.update(
            {
                "info": "VALOR_MINIMO",
                "fec_corte": BASE_DATE,
                "at_tipo_garantia": "0207" if idx % 2 == 0 else "0208",
                "at_tipo_fac_crediticia": "01",
                "at_numero_garantia": f"VM{idx:04d}",
                "at_num_de_prestamos": loan_id,
                "at_numero_cis_garantia": f"VCIS{idx:04d}",
                "at_numero_cis_prestamo": f"VCISP{idx:04d}",
                "at_valor_garantia": fmt_money(8000 + idx * 50),
                "at_valor_pond_garantia": fmt_money(7800 + idx * 50),
                "factor": "0.85",
                "cu_tipo": "ACTIVA",
                "venta_rapida": fmt_money(6000 + idx * 40),
                "valor_garantia": fmt_money(7500 + idx * 45),
                "nuevo_at_valor_garantia": fmt_money(8000 + idx * 60),
                "nuevo_at_valor_pond_garantia": fmt_money(7800 + idx * 55),
            }
        )
        if overrides:
            data.update(overrides)
        return data

    target_loans = ["6000000008", "6000000009", "6000000010", "6000000011", "6000000012"]
    rows = [row(i, loan) for i, loan in enumerate(target_loans)]

    idx = len(rows)
    while len(rows) < 50:
        loan_id = f"600000{3000 + idx:04d}"
        rows.append(row(idx, loan_id))
        idx += 1

    return pd.DataFrame(rows, columns=all_columns)


def write_dataframe(df: pd.DataFrame, name: str) -> None:
    filename = f"{name}_{BASE_DATE}__run-{RUN_ID}.CSV"
    path = OUTPUT_DIR / filename
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Wrote {path} ({len(df)} rows)")


def main() -> None:
    schema = load_schema()
    ensure_output_dir()

    builders = {
        "BASE_AT12": build_base_dataframe,
        "TDC_AT12": build_tdc_dataframe,
        "SOBREGIRO_AT12": build_sobregiro_dataframe,
        "VALORES_AT12": build_valores_dataframe,
        "GARANTIA_AUTOS_AT12": build_garantia_autos_dataframe,
        "POLIZA_HIPOTECAS_AT12": build_poliza_hipotecaria,
        "AT03_CREDITOS": build_at03_creditos,
        "AT02_CUENTAS": build_at02_dataframe,
        "AFECTACIONES_AT12": build_afectaciones,
        "VALOR_MINIMO_AVALUO_AT12": build_valor_minimo,
    }

    dataframes = {
        name: builder(schema.get(name, [])) if builder is not build_at03_tdc else build_at03_tdc()
        for name, builder in builders.items()
    }
    dataframes["AT03_TDC"] = build_at03_tdc()

    for name, df in dataframes.items():
        if len(df) < 50:
            raise ValueError(f"Dataset {name} generated {len(df)} rows; expected at least 50")
        write_dataframe(df, name)


if __name__ == "__main__":
    main()
