from __future__ import annotations
import json, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

def connection():
    import happybase
    return happybase.Connection(host=os.getenv("HBASE_HOST","hbase"),port=int(os.getenv("HBASE_THRIFT_PORT","9090")),timeout=5000)
class Handler(BaseHTTPRequestHandler):
    def send_json(self,status,body):
        data=json.dumps(body).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        path=urlparse(self.path).path
        if path=="/health": return self.send_json(200,{"status":"ok"})
        if path.startswith("/v1/clients/"):
            cid=path.split("/")[-1]
            if not cid.isdigit(): return self.send_json(400,{"error":"client_id must be numeric"})
            conn=None
            try:
                conn=connection(); row=conn.table("fraud:client_risk").row(cid.encode())
                if not row: return self.send_json(404,{"error":"client not found"})
                body={k.decode().replace("cf:",""):v.decode() for k,v in row.items()}; body["client_id"]=int(cid); return self.send_json(200,body)
            except Exception as exc: return self.send_json(503,{"error":"hbase_unavailable","detail":str(exc)})
            finally:
                if conn:
                    try: conn.close()
                    except Exception: pass
        return self.send_json(404,{"error":"not found"})
    def log_message(self,fmt,*args): print("[serving-api] "+fmt%args)
if __name__=="__main__":
    ThreadingHTTPServer((os.getenv("API_HOST","0.0.0.0"),int(os.getenv("API_PORT","8095"))),Handler).serve_forever()
