#!/usr/bin/env python3
"""
scripts/test_oracle.py

Verifica las propiedades matematicas y consistencia del oraculo de referencia (HU-3.1).
"""

import json
import os
import unittest


class TestReferenceOracle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.expected_dir = os.path.join(
            os.path.dirname(__file__), "..", "data", "sample", "expected"
        )
        with open(os.path.join(cls.expected_dir, "job_a_expected.json"), "r") as f:
            cls.job_a = json.load(f)
        with open(os.path.join(cls.expected_dir, "job_b1_expected.json"), "r") as f:
            cls.job_b1 = json.load(f)
        with open(os.path.join(cls.expected_dir, "job_b2_expected.json"), "r") as f:
            cls.job_b2 = json.load(f)
        with open(os.path.join(cls.expected_dir, "risk_analysis_expected.json"), "r") as f:
            cls.risk = json.load(f)

    def test_job_a_metrics(self):
        # 15 procesos en total
        self.assertEqual(len(self.job_a), 15)
        by_id = {row["id_del_proceso"]: row for row in self.job_a}

        # PROC-2026-001: 8/10 = 0.8
        self.assertAlmostEqual(by_id["PROC-2026-001"]["indice_competencia"], 0.8, places=3)
        # PROC-2026-002: 1/10 = 0.1
        self.assertAlmostEqual(by_id["PROC-2026-002"]["indice_competencia"], 0.1, places=3)
        # PROC-2026-011: 0 invitados -> 0.0 (evita division por cero)
        self.assertEqual(by_id["PROC-2026-011"]["indice_competencia"], 0.0)

    def test_job_b1_median_and_deviation(self):
        # 12 contratos en total
        self.assertEqual(len(self.job_b1), 12)
        by_proc = {row["proceso_de_compra"]: row for row in self.job_b1}

        # Categoria UNSPSC-7210: valores [120M, 180M, 200M, 300M, 450M] -> Mediana = 200M
        self.assertEqual(by_proc["PROC-2026-001"]["mediana_categoria"], 200000000)
        self.assertAlmostEqual(by_proc["PROC-2026-001"]["desviacion_precio"], -0.4, places=3)
        self.assertAlmostEqual(by_proc["PROC-2026-002"]["desviacion_precio"], 1.25, places=3)
        self.assertAlmostEqual(by_proc["PROC-2026-005"]["desviacion_precio"], 0.0, places=3)

        # Categoria UNSPSC-4321: valores [25M, 28M, 30M, 32M, 85M] -> Mediana = 30M
        self.assertEqual(by_proc["PROC-2026-003"]["mediana_categoria"], 30000000)
        self.assertAlmostEqual(by_proc["PROC-2026-003"]["desviacion_precio"], 1.8333, places=3)

    def test_job_b2_concentration(self):
        # 10 pares unicos proveedor-entidad
        self.assertEqual(len(self.job_b2), 10)
        by_pair = {
            (row["documento_proveedor"], row["nit_entidad"]): row
            for row in self.job_b2
        }

        # PROV-9001: 3 contratos con NIT-1001, 1 con NIT-1002 (total 4)
        p1_e1 = by_pair[("PROV-9001", "NIT-1001")]
        self.assertEqual(p1_e1["contratos_con_entidad"], 3)
        self.assertEqual(p1_e1["contratos_totales_proveedor"], 4)
        self.assertAlmostEqual(p1_e1["concentracion_proveedor"], 0.75, places=3)

        p1_e2 = by_pair[("PROV-9001", "NIT-1002")]
        self.assertEqual(p1_e2["contratos_con_entidad"], 1)
        self.assertEqual(p1_e2["contratos_totales_proveedor"], 4)
        self.assertAlmostEqual(p1_e2["concentracion_proveedor"], 0.25, places=3)

        # PROV-9002: 1 contrato unico con NIT-1001 -> concentracion 1.0
        p2 = by_pair[("PROV-9002", "NIT-1001")]
        self.assertAlmostEqual(p2["concentracion_proveedor"], 1.0, places=3)

    def test_stage2_join_and_left_outer_join(self):
        records = self.risk["registros_riesgo"]
        self.assertEqual(len(records), 15)
        by_id = {row["id_del_proceso"]: row for row in records}

        # Procesos desiertos o en proceso sin contrato (Left Outer Join)
        for unawarded_id in ["PROC-2026-009", "PROC-2026-010", "PROC-2026-015"]:
            rec = by_id[unawarded_id]
            self.assertFalse(rec["tiene_contrato"])
            self.assertEqual(rec["documento_proveedor"], "")
            self.assertEqual(rec["valor_contrato"], 0)
            self.assertEqual(rec["desviacion_precio"], 0.0)
            self.assertEqual(rec["concentracion"], 0.0)

        # Validar conteo y top de alto riesgo
        self.assertEqual(self.risk["total_procesos_alto_riesgo"], 8)
        top_high_risk = [r["id_del_proceso"] for r in records if r["flag_riesgo_alto"]]
        self.assertIn("PROC-2026-003", top_high_risk)
        self.assertIn("PROC-2026-002", top_high_risk)
        self.assertIn("PROC-2026-011", top_high_risk)


if __name__ == "__main__":
    unittest.main()
