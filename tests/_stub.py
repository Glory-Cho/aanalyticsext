import sys, types
m = types.ModuleType("aanalytics2"); m.importConfigFile = lambda p: None
class _L:
    def __init__(self): self.connector = types.SimpleNamespace(config={})
m.Login = _L
class _A:
    def __init__(self, cid): self.header = {}
    def getReportSuites(self): return []
    def getReport(self, jsonFile, limit=50000, n_results='inf', item_id=False):
        return _A.HANDLER(jsonFile, item_id)
m.Analytics = _A
sys.modules["aanalytics2"] = m
