#!/usr/bin/env python3
"""
scripts/reference_oracle.py

Oraculo de correccion secuencial (no distribuido) para SECOP II.
Calcula exactamente:
  - Job A: Indice de competencia por proceso
  - Job B1: Mediana y desviacion de precio por categoria UNSPSC
  - Job B2: Concentracion proveedor-entidad
  - Etapa 2: Reduce-side join (con Left Outer Join) y puntuacion de riesgo combinada

Este script constituye la fuente de verdad (ground truth) para verificar la
exactitud numerica de los jobs distribuidos en Go (HU-3.1).
"""

import argparse
import csv
import json
import os
import sys
from typing import Any, Dict, List, Tuple


def read_csv(filepath: str) -> List[Dict[str, str]]:
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def compute_job_a(procesos: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    results = []
    for row in procesos:
        invitados = int(row.get("proveedores_invitados", 0))
        responden = int(row.get("proveedores_unicos_con_respuestas", 0))
        if invitados > 0:
            indice = round(float(responden) / float(invitados), 4)
        else:
            indice = 0.0

        results.append({
            "id_del_proceso": row["id_del_proceso"],
            "nit_entidad": row["nit_entidad"],
            "proveedores_invitados": invitados,
            "proveedores_responden": responden,
            "indice_competencia": indice,
            "modalidad_de_contratacion": row.get("modalidad_de_contratacion", ""),
            "estado_del_procedimiento": row.get("estado_del_procedimiento", ""),
        })
    return results


def compute_median(values: List[int]) -> int:
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 0:
        return 0
    if n % 2 == 1:
        return sorted_vals[n // 2]
    else:
        return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) // 2


def compute_job_b1(contratos: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    # Agrupar valores por categoria
    category_values: Dict[str, List[int]] = {}
    for row in contratos:
        cat = row["codigo_de_categoria_principal"]
        val = int(row["valor_del_contrato"])
        category_values.setdefault(cat, []).append(val)

    # Calcular mediana por categoria
    category_medians: Dict[str, int] = {
        cat: compute_median(vals) for cat, vals in category_values.items()
    }

    results = []
    for row in contratos:
        cat = row["codigo_de_categoria_principal"]
        val = int(row["valor_del_contrato"])
        mediana = category_medians[cat]
        if mediana > 0:
            desviacion = round(float(val - mediana) / float(mediana), 4)
        else:
            desviacion = 0.0

        results.append({
            "proceso_de_compra": row["proceso_de_compra"],
            "nit_entidad": row["nit_entidad"],
            "documento_proveedor": row["documento_proveedor"],
            "valor_del_contrato": val,
            "categoria_unspsc": cat,
            "mediana_categoria": mediana,
            "desviacion_precio": desviacion,
        })
    return results


def compute_job_b2(contratos: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    total_provider_contracts: Dict[str, int] = {}
    pair_contracts: Dict[Tuple[str, str], int] = {}

    for row in contratos:
        doc = row["documento_proveedor"]
        nit = row["nit_entidad"]
        total_provider_contracts[doc] = total_provider_contracts.get(doc, 0) + 1
        pair = (doc, nit)
        pair_contracts[pair] = pair_contracts.get(pair, 0) + 1

    results = []
    for (doc, nit), count_entity in sorted(pair_contracts.items()):
        total_doc = total_provider_contracts[doc]
        concentracion = round(float(count_entity) / float(total_doc), 4)
        results.append({
            "documento_proveedor": doc,
            "nit_entidad": nit,
            "contratos_con_entidad": count_entity,
            "contratos_totales_proveedor": total_doc,
            "concentracion_proveedor": concentracion,
        })
    return results


def compute_stage2_join(
    job_a_results: List[Dict[str, Any]],
    job_b1_results: List[Dict[str, Any]],
    job_b2_results: List[Dict[str, Any]],
    raw_contratos: List[Dict[str, str]],
) -> Dict[str, Any]:
    b1_by_proc = {row["proceso_de_compra"]: row for row in job_b1_results}
    b2_by_pair = {
        (row["documento_proveedor"], row["nit_entidad"]): row
        for row in job_b2_results
    }
    raw_by_proc = {row["proceso_de_compra"]: row for row in raw_contratos}

    risk_records = []
    high_risk_count = 0

    for a in job_a_results:
        proc_id = a["id_del_proceso"]
        nit_entidad = a["nit_entidad"]
        indice_comp = a["indice_competencia"]

        b1 = b1_by_proc.get(proc_id)

        if b1 is not None:
            # Proceso con contrato asociado
            tiene_contrato = True
            doc_prov = b1["documento_proveedor"]
            raw_c = raw_by_proc.get(proc_id, {})
            nombre_prov = raw_c.get("proveedor_adjudicado", "")
            valor_contrato = b1["valor_del_contrato"]
            desv_precio = b1["desviacion_precio"]

            b2 = b2_by_pair.get((doc_prov, nit_entidad))
            concentracion = b2["concentracion_proveedor"] if b2 else 0.0

            # Calculo de factores de riesgo segun pkg/jobs/join/join.go
            components = [
                1.0 - max(0.0, min(1.0, indice_comp)),
                max(0.0, min(1.0, abs(desv_precio))),
                max(0.0, min(1.0, concentracion)),
            ]
            score = round(sum(components) / len(components), 4)
            flag_alto = score >= 0.5
        else:
            # Left Outer Join: Proceso sin contrato (desierto o cancelado)
            tiene_contrato = False
            doc_prov = ""
            nombre_prov = ""
            valor_contrato = 0
            desv_precio = 0.0
            concentracion = 0.0

            components = [1.0 - max(0.0, min(1.0, indice_comp))]
            score = round(sum(components) / len(components), 4)
            flag_alto = score >= 0.5

        if flag_alto:
            high_risk_count += 1

        risk_records.append({
            "id_del_proceso": proc_id,
            "nit_entidad": nit_entidad,
            "nombre_entidad": nit_entidad,
            "tiene_contrato": tiene_contrato,
            "documento_proveedor": doc_prov,
            "nombre_proveedor": nombre_prov,
            "valor_contrato": valor_contrato,
            "indice_competencia": indice_comp,
            "desviacion_precio": desv_precio,
            "concentracion": concentracion,
            "puntuacion_riesgo": score,
            "flag_riesgo_alto": flag_alto,
        })

    # Ordenar registros por puntuacion_riesgo descendente
    risk_records.sort(key=lambda r: (-r["puntuacion_riesgo"], r["id_del_proceso"]))

    return {
        "job_id": "job_oracle_secop_2026",
        "total_procesos_analizados": len(risk_records),
        "total_procesos_alto_riesgo": high_risk_count,
        "registros_riesgo": risk_records,
    }


def export_csv(filepath: str, data: List[Dict[str, Any]]):
    if not data:
        return
    fieldnames = list(data[0].keys())
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def export_json(filepath: str, data: Any):
    with open(filepath, mode="w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(
        description="Oraculo secuencial de referencia para analisis de riesgo SECOP II"
    )
    parser.add_argument(
        "--procesos",
        required=True,
        help="Ruta al archivo CSV de Procesos de Contratacion",
    )
    parser.add_argument(
        "--contratos",
        required=True,
        help="Ruta al archivo CSV de Contratos Electronicos",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directorio destino para los archivos esperados (expected)",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    procesos_raw = read_csv(args.procesos)
    contratos_raw = read_csv(args.contratos)

    print(f"[Oraculo] Leyendo {len(procesos_raw)} procesos y {len(contratos_raw)} contratos...")

    # Job A
    job_a = compute_job_a(procesos_raw)
    export_json(os.path.join(args.output_dir, "job_a_expected.json"), job_a)
    export_csv(os.path.join(args.output_dir, "job_a_expected.csv"), job_a)
    print(f"[Oraculo] Job A computado: {len(job_a)} registros")

    # Job B1
    job_b1 = compute_job_b1(contratos_raw)
    export_json(os.path.join(args.output_dir, "job_b1_expected.json"), job_b1)
    export_csv(os.path.join(args.output_dir, "job_b1_expected.csv"), job_b1)
    print(f"[Oraculo] Job B1 computado: {len(job_b1)} registros")

    # Job B2
    job_b2 = compute_job_b2(contratos_raw)
    export_json(os.path.join(args.output_dir, "job_b2_expected.json"), job_b2)
    export_csv(os.path.join(args.output_dir, "job_b2_expected.csv"), job_b2)
    print(f"[Oraculo] Job B2 computado: {len(job_b2)} relaciones proveedor-entidad")

    # Etapa 2: Join
    risk_result = compute_stage2_join(job_a, job_b1, job_b2, contratos_raw)
    export_json(os.path.join(args.output_dir, "risk_analysis_expected.json"), risk_result)
    export_csv(
        os.path.join(args.output_dir, "risk_analysis_expected.csv"),
        risk_result["registros_riesgo"],
    )
    print(
        f"[Oraculo] Join de Etapa 2 computado: {risk_result['total_procesos_analizados']} procesos analizados, "
        f"{risk_result['total_procesos_alto_riesgo']} clasificados como alto riesgo"
    )
    print(f"[Oraculo] Resultados exportados en {args.output_dir}")


if __name__ == "__main__":
    main()
