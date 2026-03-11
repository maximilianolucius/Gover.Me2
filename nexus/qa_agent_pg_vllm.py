# qa_agent_pg_vllm_v3_timed_profiles.py
# deps: pip install langchain langchain-openai sqlalchemy psycopg2-binary pandas
# Compatible con PostgreSQL 15+ y servidores vLLM OpenAI-compatibles

import os, re, time
from urllib.parse import quote_plus
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text, event, inspect
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain

# ==========================
# Configuración de la base (desde .env; DB por defecto: nexus)
# ==========================
load_dotenv()

def _build_dsn_default_env() -> str:
    dsn_env = os.getenv("NEXUS_DSN")
    if dsn_env:
        return dsn_env
    host = os.getenv("POSTGRES_HOST", "localhost")
    db   = os.getenv("POSTGRES_DB",   "nexus")  # DB por defecto: nexus
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd  = os.getenv("POSTGRES_PASSWORD", "password")
    port = os.getenv("POSTGRES_PORT", "5432")
    user_q = quote_plus(user)
    pwd_q  = quote_plus(pwd)

    db = 'nexus'

    return f"postgresql+psycopg2://{user_q}:{pwd_q}@{host}:{port}/{db}"

DSN = _build_dsn_default_env()



print(f'DSN: {DSN}')


# ==========================
# Perfiles vLLM (PRIMARY / ALT)
# ==========================
VLLM_API_KEY = os.getenv("VLLM_API_KEY", "dummy")  # requerido por el cliente

# Perfil principal (prod / público)
VLLM_BASE_URL  = os.getenv("VLLM_BASE_URL",  "http://181.66.252.169:8000/v1")
VLLM_MODEL     = os.getenv("VLLM_MODEL",     "Qwen3-8B-AWQ")

# Perfil alternativo (LAN / laboratorio)
ALT_VLLM_BASE_URL = os.getenv("ALT_VLLM_BASE_URL", "http://172.16.0.41:8000/v1")
ALT_VLLM_MODEL    = os.getenv("ALT_VLLM_MODEL",    "Qwen/Qwen3-1.7B")
ALT_VLLM_API_KEY  = os.getenv("ALT_VLLM_API_KEY",  VLLM_API_KEY)

# Selector de perfil por entorno: 'primary' (default) | 'alt'
VLLM_PROFILE_ENV = os.getenv("VLLM_PROFILE", "primary").strip().lower()

# (Opcional) limitar a tablas concretas para mayor precisión/seguridad
DEFAULT_TABLES = [
    "turismo_paises",
    "turismo_espana",
    "turismo_total",
]
INCLUDE_TABLES = [t.strip() for t in os.getenv("NEXUS_TABLES", "").split(",") if t.strip()] or DEFAULT_TABLES

# --- SQLAlchemy engine (timeout + pre_ping) ---
engine = create_engine(
    DSN,
    pool_pre_ping=True,
    connect_args={"options": "-c statement_timeout=15000"}  # 15s
)

# Forzar sesión de solo-lectura y search_path
@event.listens_for(engine, "connect")
def set_session_readonly(dbapi_conn, _):
    with dbapi_conn.cursor() as cur:
        cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        cur.execute("SET search_path TO public")

# --- LangChain DB wrapper (lee esquema y muestras) ---
# Filtrar dinámicamente las tablas incluidas según existan en la BD
try:
    _inspector = inspect(engine)
    _existing = set(_inspector.get_table_names(schema='public'))
except Exception:
    _existing = set()

_include_effective = [t for t in INCLUDE_TABLES if t in _existing]
if not _include_effective:  # Si ninguna existe, no restringir
    _include_effective = None

db = SQLDatabase.from_uri(
    DSN,
    include_tables=_include_effective,
    sample_rows_in_table_info=50,
)

# Construir mapa ligero de columnas por tabla (para prevalidación)
_columns_by_table: dict[str, set[str]] = {}
try:
    _tables_for_cols = _include_effective if _include_effective is not None else list(_existing)
    for _t in _tables_for_cols:
        try:
            _cols = _inspector.get_columns(_t, schema='public')
            _columns_by_table[_t] = {c['name'] for c in _cols}
        except Exception:
            _columns_by_table[_t] = set()
except Exception:
    _columns_by_table = {}

# ==========================
# Utilidades de seguridad SQL
# ==========================
DML_BLOCKLIST = r"\b(insert|update|delete|merge|alter|drop|create|grant|revoke|truncate|comment|vacuum|analyze)\b"


def _looks_like_sql(stmt: str) -> bool:
    t = re.sub(r"/\*.*?\*/|--.*?$", "", stmt, flags=re.S | re.M).strip()
    if not re.match(r"(?is)^(with|select)\b", t):
        return False
    if re.match(r"(?is)^select\s+the\b", t):
        return False
    if not re.search(r"(?is)\bfrom\b", t):
        if not (t.lower().startswith("with") and re.search(r"(?is)select[\s\S]+?\bfrom\b", t)):
            return False
    if re.search(DML_BLOCKLIST, t.lower()):
        return False
    return True


def _is_safe_sql(sql: str) -> bool:
    return _looks_like_sql(sql)


# --- extractor robusto de SQL puro ---
def _extract_sql_only(s: str) -> str:
    """Devuelve solo la sentencia SQL (SELECT o WITH ... SELECT),
    ignorando <think>, 'SQLQuery:', texto explicativo y bloques no SQL."""
    if not s:
        raise ValueError("LLM no devolvió texto.")
    s = str(s).strip()

    candidates = []
    fence_matches = re.findall(r"```(?:sql)?\s*([\s\S]*?)```", s, flags=re.I)
    candidates.extend(fence_matches)
    for pat in (r"(?is)\bwith\b[\s\S]+?(?:;|\Z)", r"(?is)\bselect\b[\s\S]+?(?:;|\Z)"):
        for m in re.finditer(pat, s):
            candidates.append(m.group(0))
    candidates = [c.strip() for c in candidates if _looks_like_sql(c)]
    if not candidates:
        lines = [ln.strip() for ln in s.splitlines()]
        approx, cur, capturing = [], [], False
        for ln in lines:
            if re.match(r"(?is)^\s*(select|with)\b", ln):
                capturing = True; cur = [ln]
            elif capturing:
                cur.append(ln)
            if capturing and (ln.strip().endswith(";") or ln == lines[-1]):
                approx.append("\n".join(cur)); capturing = False
        approx = [a for a in approx if _looks_like_sql(a)]
        if approx:
            candidates = approx
        else:
            raise ValueError("No se pudo extraer una sentencia SQL válida del texto del LLM.")
    chosen = max(candidates, key=len).strip()
    # Si hay ';', recortar estrictamente hasta el primer ';'
    if ";" in chosen:
        chosen = chosen.split(";", 1)[0].strip() + ";"
    # Asegurar que comienza en SELECT/WITH (por si coló texto previo)
    lines = chosen.splitlines()
    cleaned = []
    capturing = False
    for ln in lines:
        if re.match(r"(?is)^\s*(select|with)\b", ln):
            capturing = True
        if capturing:
            cleaned.append(ln)
    chosen = "\n".join(cleaned).strip()
    if not _looks_like_sql(chosen):
        raise ValueError("No se pudo asegurar una sentencia SQL limpia y válida.")
    return chosen


def _strip_quotes(name: str) -> str:
    n = name.strip()
    if n.startswith('"') and n.endswith('"') and len(n) >= 2:
        return n[1:-1]
    return n


def _prevalidate_sql(sql: str):
    """Validación ligera previa a ejecutar contra BD.
    - Verifica que las tablas referenciadas existen y están permitidas.
    - Verifica que identificadores entre comillas dobles existen como columnas en dichas tablas.
    Lanza ValueError con feedback si hay problemas.
    """
    text = sql
    # Tablas tras FROM/JOIN (con o sin comillas, opcional esquema)
    tables_ref: set[str] = set()
    for pat in (r"(?is)\bfrom\s+(\"?[\w\.]+\"?)\s*(?:as\b|\s|$)", r"(?is)\bjoin\s+(\"?[\w\.]+\"?)\s*(?:as\b|\s|$)"):
        for m in re.finditer(pat, text):
            raw = m.group(1)
            base = _strip_quotes(raw).split('.')[-1]
            if base:
                tables_ref.add(base)

    allowed_tables = set(_columns_by_table.keys()) if _columns_by_table else (_include_effective and set(_include_effective)) or _existing
    unknown_tables = sorted([t for t in tables_ref if t not in allowed_tables])
    if unknown_tables:
        raise ValueError(f"Tablas no permitidas o inexistentes: {', '.join(unknown_tables)}")

    # Conjunto de columnas conocidas para tablas referenciadas (o todas si no se detectaron)
    known_cols: set[str] = set()
    source_tables = tables_ref or allowed_tables
    for t in source_tables:
        known_cols |= _columns_by_table.get(t, set())

    # Identificadores entre comillas deben existir como columnas conocidas (salvo alias tras AS)
    bad_cols: set[str] = set()
    for m in re.finditer(r'"([^\"]+)"', text):
        name = m.group(1)
        before = text[:m.start()]
        tail = before.rstrip()
        # evitar falsos positivos para alias '... AS "alias"'
        last_tokens = tail[-10:].lower()
        if last_tokens.strip().endswith(' as'):
            continue
        if name not in known_cols and name not in allowed_tables:
            bad_cols.add(name)

    if bad_cols:
        raise ValueError(f"Identificadores desconocidos (no son columnas de tablas referenciadas): {', '.join(sorted(bad_cols))}")


def _regenerate_with_feedback(llm: ChatOpenAI, question: str, failed_sql: str, error_msg: str) -> str:
    """Pide al LLM que corrija la SQL dada, usando feedback explícito."""
    allowed = ', '.join(sorted((_include_effective or list(_existing)))) if (_include_effective or _existing) else '(sin lista)'
    prompt = (
        _DEF_RULES + _SCHEMA_GUIDE +
        "Corrige la consulta SQL fallida usando SOLAMENTE tablas y columnas existentes.\n"
        f"Consulta fallida:\n{failed_sql}\n"
        f"Error/validación:\n{error_msg}\n\n"
        f"Tablas permitidas: {allowed}\n"
        "Devuelve UNA sola sentencia SQL válida para PostgreSQL, terminada en ';' y sin texto extra.\n"
        f"Pregunta original: {question}\n"
    )
    out = llm.invoke(prompt)
    content = _strip_think_blocks(getattr(out, 'content', str(out)))
    sql2 = _extract_sql_only(content)
    _prevalidate_sql(sql2)
    return sql2


_SCHEMA_GUIDE = (
    "Contexto: Todo el esquema se refiere a datos de turismo de Andalucía (destino Andalucía).\n"
    "Tablas disponibles y columnas (PostgreSQL):\n"
    "- turismo_paises(año INT, mes INT, codigo_pais VARCHAR(3), nombre_pais VARCHAR(50), "
    "viajeros_hoteles INT, pernoctaciones_hoteles BIGINT)\n"
    "- turismo_espana(año INT, mes INT, origen VARCHAR(20), viajeros_hoteles INT, pernoctaciones_hoteles BIGINT, "
    "llegadas_aeropuertos INT, turistas_millones DECIMAL(10,2), estancia_media_dias DECIMAL(4,1), gasto_medio_diario DECIMAL(8,2))\n"
    "- turismo_total(año INT, mes INT, categoria VARCHAR(20), viajeros_hoteles INT, pernoctaciones_hoteles BIGINT, "
    "llegadas_aeropuertos INT, turistas_millones DECIMAL(10,2), estancia_media_dias DECIMAL(4,1), gasto_medio_diario DECIMAL(8,2))\n\n"
    "Reglas de salida y convenciones:\n"
    "1) Siempre que sea relevante, crea la columna 'periodo' como make_date(\"año\", \"mes\", 1).\n"
    "2) Usa comillas dobles para columnas con acentos o eñes, p.ej. \"año\", y NUNCA las renombres en FROM.\n"
    "3) Alias estandarizados: 'pais'/'codigo_pais'/'nombre_pais', 'origen', 'categoria', "
    "'viajeros' (viajeros_hoteles), 'pernoctaciones' (pernoctaciones_hoteles), 'llegadas' (llegadas_aeropuertos), "
    "'turistas_millones', 'estancia_media_dias', 'gasto_medio_diario'.\n"
    "4) Devuelve UNA sola sentencia SQL (SELECT o WITH), terminada en ';', sin markdown ni explicación.\n"
)

_DEF_RULES = (
    "Genera EXACTAMENTE una sola sentencia SQL (SELECT o WITH) para PostgreSQL y TERMÍNALA con ';'.\n"
    "- Devuelve SOLO la SQL, sin explicaciones, comentarios ni markdown.\n"
    "- En WHERE une condiciones con AND (NUNCA con comas).\n"
    "- No incluyas texto adicional después del ';'.\n"
    "- Respeta estas convenciones de esquema y salida.\n\n"
)


def _strip_think_blocks(obj) -> str:
    """Limpia <think>…</think> del contenido devuelto por el LLM."""
    text_ = getattr(obj, "content", obj)
    if isinstance(text_, dict):
        text_ = text_.get("content") or text_.get("text") or str(text_)
    text_ = str(text_)
    return re.sub(r"(?is)<\s*think\s*>.*?<\s*/\s*think\s*>", "", text_).strip()


# ==========================
# Selección dinámica de LLM/Chain
# ==========================

_LLM_POOL: dict[str, ChatOpenAI] = {}
_CHAIN_POOL = {}

def _choose_llm(profile: str | None = None) -> ChatOpenAI:
    use = (profile or VLLM_PROFILE_ENV or "primary").strip().lower()
    if use in _LLM_POOL:
        return _LLM_POOL[use]

    if use == "alt":
        llm = ChatOpenAI(
            model=ALT_VLLM_MODEL,
            base_url=ALT_VLLM_BASE_URL,
            api_key=ALT_VLLM_API_KEY,
            temperature=0,
            max_retries=2,
            timeout=120,
            max_tokens=10000,
        )
    else:
        llm = ChatOpenAI(
            model=VLLM_MODEL,
            base_url=VLLM_BASE_URL,
            api_key=VLLM_API_KEY,
            temperature=0,
            max_retries=2,
            timeout=120,
            max_tokens=10000,
        )
    _LLM_POOL[use] = llm
    return llm


def _make_sql_chain(llm: ChatOpenAI, profile: str) -> any:
    key = f"sql_chain:{profile}"
    ch = _CHAIN_POOL.get(key)
    if ch is None:
        ch = create_sql_query_chain(llm, db)
        _CHAIN_POOL[key] = ch
    return ch


# ==========================
# API principal
# ==========================

def ask(question: str, summarize: bool = True, return_timings: bool = False, llm_profile: str | None = None):
    """
    Genera SQL (solo SELECT), ejecuta y devuelve (df, sql, resumen opcional).
    - Mide tiempos: generación SQL (LLM), ejecución DB, resumen LLM y total.
    - Permite cambiar de servidor/modelo vLLM por llamada con `llm_profile` ('primary'|'alt').
    - Si `return_timings=True`, devuelve (df, sql, summary, timings_dict); si False, mantiene (df, sql, summary).
    """
    timings = {}
    t0 = time.perf_counter()

    # 1) Elegir LLM/chain según perfil
    profile = (llm_profile or VLLM_PROFILE_ENV or "primary").strip().lower()
    llm = _choose_llm(profile)
    chain = _make_sql_chain(llm, profile)

    events: list[str] = []

    # 2) Prompt + generación SQL
    t1 = time.perf_counter()
    prompt = _DEF_RULES + _SCHEMA_GUIDE + question
    raw = chain.invoke({"question": prompt})
    raw = _strip_think_blocks(raw)
    sql = _extract_sql_only(raw)
    # Prevalidación con posibilidad de reintentos (hasta 3)
    max_retries = 3
    tries = 0
    while True:
        try:
            _prevalidate_sql(sql)
            break
        except Exception as e:
            events.append(f"Prevalidación falló: {e}")
            if tries >= max_retries:
                raise
            tries += 1
            sql = _regenerate_with_feedback(llm, question, sql, str(e))
    if not _is_safe_sql(sql):
        raise ValueError(f"SQL potencialmente no seguro tras limpieza:\n{sql}")
    t2 = time.perf_counter(); timings["t_llm_sql_s"] = t2 - t1

    # 3) Ejecución en DB
    t3 = time.perf_counter()
    with engine.connect() as conn:
        exec_tries = 0
        while True:
            try:
                result = conn.execute(text(sql))
                break
            except Exception as e:
                print("\n[ERROR] Falló la ejecución de la SQL. Consulta enviada:", flush=True)
                print(sql, flush=True)
                events.append(f"Ejecución falló: {e}")
                if exec_tries >= max_retries:
                    raise
                exec_tries += 1
                # Regenerar con feedback y volver a intentar
                sql = _regenerate_with_feedback(llm, question, sql, str(e))
    rows = result.fetchall()
    cols = list(result.keys())
    df = pd.DataFrame(rows, columns=cols)
    t4 = time.perf_counter(); timings["t_db_s"] = t4 - t3

    # 4) Resumen (opcional)
    summary = None
    if summarize:
        t5 = time.perf_counter()
        # Reducir tamaño de preview para acelerar el resumen sin perder calidad
        preview_df = df.head(10)
        preview = preview_df.to_markdown(index=False) if not preview_df.empty else "(sin filas)"
        summary = llm.invoke(
            "Resume en español estos resultados SQL de forma breve y clara. "
            "Si está vacío, di que no hay filas coincidentes.\n\n" + preview
        ).content
        summary = _strip_think_blocks(summary)
        t6 = time.perf_counter(); timings["t_summary_s"] = t6 - t5

    timings["llm_profile"] = profile
    timings["t_total_s"] = time.perf_counter() - t0
    timings["retries"] = tries
    if events:
        timings["events"] = events

    if return_timings:
        return df, sql, summary, timings
    return df, sql, summary


if __name__ == "__main__":
    # Ejemplo: usar perfil ALT (LAN) y ver tiempos
    q = (
        "Top 5 países acumulado de viajeros en año 2025. "
        "Incluye pais y ordena descendente."
    )
    df, sql, summary, t = ask(q, summarize=True, return_timings=True, llm_profile=os.getenv("ASK_PROFILE"))
    print("SQL:\n", sql)
    print("Resumen:\n", summary)
    print(df.head())
    print("Timings (s):", t)

    # Ejemplo: usar perfil ALT (LAN) y ver tiempos
    df, sql, summary, t = ask(q, summarize=True, return_timings=True, llm_profile='alt')
    print("SQL:\n", sql)
    print("Resumen:\n", summary)
    print(df.head())
    print("Timings (s):", t)
