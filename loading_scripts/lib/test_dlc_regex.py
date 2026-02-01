import unittest

# Import your compiled regexes and the ANSI cleaner
import dlc_analytics as dlc


def clean(s: str) -> str:
    """Remove ANSI color sequences exactly the way the parser does."""
    return dlc.ANSI_ESCAPE.sub('', s)


class TestDLCRegex(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Raw lines with ANSI escapes (as provided)
        cls.test_lines = {
            "start_line": "2026-01-29 13:41:39.106 CET [[34mmain[0;39m] [34mINFO [0;39m [33mc.a.i.d.i.DataLoadControllerService[0;39m - [dlc, transaction] Starting LOAD operation, operation_id=0, on topic [StaticTopic], with scope {}. Locking stores: [Scenarios]",
            "transaction_started": "2026-01-29 13:41:39.108 CET [[34mactivepivot-health-event-dispatcher[0;39m] [34mINFO [0;39m [33mcom.activeviam.apm.health[0;39m - [datastore, transaction] INFO 2026-01-29T12:41:39.108Z uptime=74869ms com.activeviam.database.datastore.internal.transaction.impl.TransactionManager.emitObservabilityOnTransactionStarted:612 thread=main thread_id=1 event_type=DatastoreTransactionStarted Transaction Started  transaction_id=3 on_stores=[Scenarios]",
            "commit_event": "2026-01-29 13:41:48.810 CET [[34mactivepivot-health-event-dispatcher[0;39m] [34mINFO [0;39m [33ma.s.tech.observability.health-event[0;39m - [activepivot, transaction] INFO 2026-01-29T12:41:48.809Z uptime=84570ms com.activeviam.activepivot.core.impl.private_.transaction.impl.ActivePivotSchemaTransaction$1.execute:284 thread=activeviam-common-pool-worker-48 thread_id=220 event_type=ActivePivotTransactionCommittedEvent user=NO_USER roles=[] ActivePivotSchema = VaR/ESSchema, Pivots = [VaR-ES Cube] ActivePivot transaction 1 was successfully committed on epoch 3. total_duration=654ms, transaction_duration=603ms, commit_duration=51ms",
            "finish_line": "2026-01-29 13:41:48.889 CET [[34mmain[0;39m] [34mINFO [0;39m [33mc.a.i.d.i.DataLoadControllerService[0;39m - [dlc, transaction] Finishing LOAD operation, id 0.",
            "thread_extractor": "2026-01-29 13:51:21.510 CET [[34mmain[0;39m] [34mINFO [0;39m [33mc.a.i.d.i.DataLoadControllerService[0;39m - [dlc, transaction] Starting LOAD operation, operation_id=2, on topic [All], with scope {AsOfDate=2026-01-23}. Locking stores: [GLCodeGroup, ForexMarketDataStore, UnitAreaCfg, Ger_CHILE_IMA, Ger_SHUSA, Ger_ALCO TOTAL, Ger_SLB NO IMA, Buckets_FRTB, BondsMarketDataStore, Product, Effects, Curves_FRTB, EQCorrelationLimit, MarketDataSets, Ger_CAPITAL ECONOMICO, PortfolioType, Ger_ARG-ESTRUCTURA GER, Ger_NY_HYP, Ger_BDE_25_BASE, Ger_FX ESTRUCTURAL VAR, Baskets, Bonds, Ger_SCMHYP, Ger_NYTOT, Seniority, DividendMarketDataStore, TradeSensitivities, PublicationCriteria, CargaMetricTrans, IrCurve, Ger_BDE_25_MULTI_5, Ger_BDE_25_MULTI_4, Counterparties, Ger_BDE_25_MULTI_3, VolatilityCubeMarketDataStore, Ger_Hypothetical Portfolio SLB, Ger_BDE_25_MULTI_2, RepoMarginMarketDataStore, PortfolioFlag, Ger_BDE_25_MULTI_1, Ger_SCIB BOA&SLB, CargaStructures, Ger_USABB, Ger_Hipoteticos, Ger_MADRI, RiskFactorsCatalogue, UndIrLatam, Ger_PT_NEGOCIACION, Ger_COBERTURAS TEMPORALES, Ger_SBNA_HYP, Ger_PERU TOTAL, Ger_GINEBRA, Ger_MIAMI, Ger_HONG KONG ALCO, RfAire, Portfolio, CcyPairs, VolatilitySurfaceMarketDataStore, Tenor, Ger_CHILE, VegaShifts, CreditMarketDataStore, Ger_HIPOTETICOS, DeltaShifts, Ger_ALCO TOTAL CG, IssuersGroup, Countries, PValueDelivery, Ger_POSICION EXPUESTA, CargaRiskConfig, CounterpartyParentChild, Ger_PT_ESTRUTURAL, SensiLadders, GammaShifts, Factor, Ger_DIVERSIFICACION, Scenarios, Ger_COBERTURAS PERMANENTES, Ger_Balance_Matriz_Completa, Mdr, LimitDelivery, IrCurveLatam, GerLevel, Ger_ADR, KDelivery, Proxies, Ger_SLB BANKING_BOOK, SpotsMarketDataStore, Ger_NYSIS, UndIr, MarketShifts, Ger_CIB BOADILLA, RiskGroup, CurvesMarketDataStore, Ger_SUSCM, TradeAttributes, FinalSector, Ger_POLAND, UnderlyingGroup, Maturities, UnderlyingEq, Ger_BRASIL, LegalEntityParentChild, CarryCriteria, Isin, Ger_HONG KONG TRADING, Mdr2, Issuers, Ger_SLB, Ger_SCIB BOA&SLB_CR, TradePnLs, CargaScImpact, Ger_SOVERE, BookParentChild, GerPnlMonthlyAnnualDelivery, TenorGroup, PnL, Ger_BDE_25_10000, Ger_MEXICO_MARS, Ccy]",
            "pivot_link":"2026-01-29 13:46:20.314 CET [[34mactivepivot-health-event-dispatcher[0;39m] [34mINFO [0;39m [33ma.s.tech.observability.health-event[0;39m - [activepivot, transaction] INFO 2026-01-29T12:46:20.314Z uptime=356075ms com.activeviam.activepivot.core.impl.private_.transaction.impl.ActivePivotSchemaTransactionManager.startTransactionOrBlock:238 thread=main thread_id=1 event_type=ActivePivotTransactionStartedEvent user=NO_USER roles=[] ActivePivotSchema = PnlSchema, Pivots = [PLCube] ActivePivot transaction 1 started, fired by database transaction 4",
        }


    # ----------------------
    # THREAD_EXTRACTOR
    # ----------------------
    def test_thread_extractor_matches_expected_lines(self):
        for name, line in self.test_lines.items():
            with self.subTest(line=name):
                line = line.rstrip('\n')
                clean_line = clean(line)
                m = dlc.THREAD_EXTRACTOR.match(clean_line)
                if name in {"start_line", "finish_line", "thread_extractor"}:
                    self.assertIsNotNone(m, f"THREAD_EXTRACTOR should match {name}")
                    self.assertEqual(m.group('thread'), "main")
                else:
                    # other lines use the activepivot dispatcher thread or unrelated patterns;
                    # THREAD_EXTRACTOR should still match because it only checks the prefix format.
                    self.assertIsNotNone(m, f"THREAD_EXTRACTOR should match prefix of {name}")

    # ----------------------
    # DLC_START_EVENT
    # ----------------------
    def test_dlc_start_event(self):
        for name, line in self.test_lines.items():
            with self.subTest(line=name):
                clean_line = clean(line)
                m = dlc.DLC_START_EVENT.search(clean_line)
                if name == "start_line":
                    self.assertIsNotNone(m, "DLC_START_EVENT should match start_line")
                    self.assertEqual(m.group('type'), "LOAD")
                    self.assertEqual(m.group('op_id'), "0")
                    self.assertEqual(m.group('topic'), "StaticTopic")
                    self.assertEqual(m.group('scope'), "")
                    self.assertEqual(m.group('locked_stores'), "Scenarios")
                elif name == "thread_extractor":
                    # This one is also a 'Starting LOAD' line but with a big store list and a non-empty scope
                    self.assertIsNotNone(m, "DLC_START_EVENT should match thread_extractor line too")
                    self.assertEqual(m.group('type'), "LOAD")
                    self.assertEqual(m.group('op_id'), "2")
                    self.assertEqual(m.group('topic'), "All")
                    self.assertEqual(m.group('scope'), "AsOfDate=2026-01-23")
                    # Just check it contains something plausible
                    self.assertIn("Scenarios", m.group('locked_stores'))
                else:
                    self.assertIsNone(m, f"DLC_START_EVENT should not match {name}")

    # ----------------------
    # DLC_FINISH_EVENT
    # ----------------------
    def test_dlc_finish_event(self):
        for name, line in self.test_lines.items():
            with self.subTest(line=name):
                clean_line = clean(line)
                m = dlc.DLC_FINISH_EVENT.search(clean_line)
                if name == "finish_line":
                    self.assertIsNotNone(m, "DLC_FINISH_EVENT should match finish_line")
                    self.assertEqual(m.group('id'), "0")
                else:
                    self.assertIsNone(m, f"DLC_FINISH_EVENT should not match {name}")

    # ----------------------
    # TRANSACTION_START
    # ----------------------
    def test_transaction_start(self):
        for name, line in self.test_lines.items():
            with self.subTest(line=name):
                clean_line = clean(line)
                m = dlc.TRANSACTION_START.search(clean_line)
                if name == "transaction_started":
                    self.assertIsNotNone(m, "TRANSACTION_START should match transaction_started")
                    self.assertEqual(m.group('tx_id'), "3")
                else:
                    self.assertIsNone(m, f"TRANSACTION_START should not match {name}")

    # ----------------------
    # PIVOT_LINK_EVENT
    # ----------------------
    def test_pivot_link_event(self):
        for name, line in self.test_lines.items():
            with self.subTest(line=name):
                clean_line = clean(line)
                m = dlc.PIVOT_LINK_EVENT.search(clean_line)
                if name == "pivot_link":
                    self.assertIsNotNone(m, "PIVOT_LINK_EVENT should match pivot_link")
                    self.assertEqual(m.group('ap_tx'), "1")
                    self.assertEqual(m.group('db_tx'), "4")
                else:
                    self.assertIsNone(m, f"PIVOT_LINK_EVENT should not match {name}")

    # ----------------------
    # COMMIT_EVENT
    # ----------------------
    def test_commit_event(self):
        for name, line in self.test_lines.items():
            with self.subTest(line=name):
                clean_line = clean(line)
                m = dlc.COMMIT_EVENT.search(clean_line)
                if name == "commit_event":
                    self.assertIsNotNone(m, "COMMIT_EVENT should match commit_event")
                    self.assertEqual(m.group('pivots'), "VaR-ES Cube")
                    self.assertEqual(m.group('ap_tx'), "1")
                    self.assertEqual(m.group('tx_dur'), "603")
                    self.assertEqual(m.group('commit_dur'), "51")
                else:
                    self.assertIsNone(m, f"COMMIT_EVENT should not match {name}")


if __name__ == "__main__":
    unittest.main()