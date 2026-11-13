# Mide la estructura del código de los jobs (solo lectura, sin dependencias externas).
# Uso: python medir_estructura.py
import ast, os, hashlib, collections

REPOS = os.environ.get("ALFA_REPOS", "C:/Users/rdpuser/Git/alfa-repos")
RMD = REPOS + "/glue/data-datalake-niif-rmd-ext-estandar-glue-job/"
JOBS = {
    "extraccion": RMD + "src/jobs/extraccion_estandar_optimized.py",
    "scd": RMD + "src/jobs/scd.py",
    "remediacion": RMD + "src/jobs/remediacion_estandar.py",
    "validacion": RMD + "src/jobs/validacion_tecnica.py",
    "silver_arl": REPOS + "/glue/data-datalake-niif-primas-arl-glue-job/src/jobs/silver_primas_arl.py",
}

def profundidad(nodo, d=0):
    bloques = (ast.If, ast.For, ast.While, ast.Try, ast.With)
    hijos = [profundidad(h, d + isinstance(h, bloques)) for h in ast.iter_child_nodes(nodo)]
    return max([d] + hijos)

def lineas_nivel_superior(arbol):
    return sum(n.end_lineno - n.lineno + 1 for n in arbol.body
               if not isinstance(n, (ast.FunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom))
               and not (isinstance(n, ast.Expr) and isinstance(getattr(n, "value", None), ast.Constant)))

firmas = collections.defaultdict(list)
for nombre, ruta in JOBS.items():
    src = open(ruta, encoding="utf-8").read()
    arbol = ast.parse(src)
    funcs = [n for n in ast.walk(arbol) if isinstance(n, ast.FunctionDef)]
    largas = sorted(((f.end_lineno - f.lineno + 1, f.name) for f in funcs), reverse=True)
    imports_internos = sum(1 for f in funcs for n in ast.walk(f) if isinstance(n, (ast.Import, ast.ImportFrom)))
    excepts_generales = sum(1 for n in ast.walk(arbol) if isinstance(n, ast.ExceptHandler)
                            and (n.type is None or getattr(n.type, "id", "") == "Exception"))
    tiene_main = any(isinstance(n, ast.If) and "__main__" in ast.unparse(n.test) for n in arbol.body)
    print(f"\n== {nombre}: {len(src.splitlines())} líneas, {len(funcs)} funciones, "
          f"{lineas_nivel_superior(arbol)} líneas de lógica fuera de funciones, main={tiene_main}")
    print(f"   anidamiento máximo: {profundidad(arbol)} | imports dentro de funciones: {imports_internos} | except genérico: {excepts_generales}")
    print(f"   funciones más largas: {largas[:3]}")
    for f in funcs:
        cuerpo = ast.unparse(f).split("\n", 1)[1] if "\n" in ast.unparse(f) else ""
        firmas[(f.name, hashlib.md5(cuerpo.encode()).hexdigest()[:8])].append(nombre)

print("\n== funciones repetidas en varios jobs (mismo nombre):")
por_nombre = collections.defaultdict(set)
for (n, h), js in firmas.items():
    por_nombre[n].update(js)
for n, js in sorted(por_nombre.items()):
    if len(js) > 1:
        variantes = len({h for (m, h) in firmas if m == n})
        print(f"   {n}: {sorted(js)} ({variantes} versión(es) distinta(s))")

pruebas = [os.path.join(r, f) for r, _, fs in os.walk(RMD) for f in fs if f.startswith("test") and f.endswith(".py")]
print("\n== pruebas en el repo rmd-ext:", pruebas or "ninguna")
for f in ("sonar-project.properties", "tox.ini"):
    p = RMD + f
    if os.path.exists(p):
        print(f"== {f}:", [l.strip() for l in open(p, encoding="utf-8") if any(k in l for k in ("exclusions", "fail_under", "omit", "coverage", "source"))])
