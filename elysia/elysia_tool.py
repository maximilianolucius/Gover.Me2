#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Herramienta Elysia News Oracle para LangChain.
Integra Elysia Tree para buscar noticias en Weaviate y devolver respuestas con fuentes.
"""
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')
load_dotenv('elysia1/.env')
load_dotenv('elysia/.elysia_env')
load_dotenv()


import logging
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any
from unittest.mock import patch

from langchain_core.tools import BaseTool
from pydantic import Field

logger = logging.getLogger(__name__)
NO_RESULTS_FALLBACK = (
    "No he podido encontrar noticias relacionadas con la consulta en nuestra base de datos."
)

# -----------------------------
# Configuración de Elysia
# -----------------------------

def _normalize_gemini_model_name(model_name: str) -> str:
    """
    Normalize Gemini model names for Elysia.
    Elysia expects model names WITHOUT the provider prefix because it adds it internally.
    """

    normalized = (model_name or "").strip()

    # Remove "models/" prefix if present
    if normalized.startswith("models/"):
        normalized = normalized[len("models/") :]

    # Remove "gemini/" prefix if present (Elysia adds it internally)
    if normalized.startswith("gemini/"):
        normalized = normalized[len("gemini/") :]

    return normalized


def _normalize_gemini_api_base(api_base: Optional[str]) -> Optional[str]:
    """Adapt Google API base URLs to the format expected by the Gemini provider."""

    if not api_base:
        return None

    base = api_base.strip().rstrip("/")
    if not base:
        return None

    # Si la base apunta al endpoint OpenAI-compatible, dejamos que LiteLLM use su valor por defecto.
    lowered = base.lower()
    if "/openai" in lowered:
        return None

    if not base.endswith("/v1beta"):
        base = f"{base}/v1beta"

    return base


def get_elysia_config_from_provider(provider: str) -> dict:
    """Devuelve configuración de Elysia basada en el proveedor del chatbot."""
    _load_env_defaults()
    provider = provider.lower()

    if provider == "gemini":
        base_model = os.getenv("GOOGLE_BASE_MODEL", "gemini-2.5-flash-lite")
        complex_model = os.getenv("GOOGLE_COMPLEX_MODEL", "gemini-2.5-flash")
        api_key = os.getenv("GOOGLE_API_KEY", "")
        if not api_key:
            raise RuntimeError("GOOGLE_API_KEY no está definido en el entorno.")

        config = {
            "base_provider": "gemini",
            "complex_provider": "gemini",
            "base_model": _normalize_gemini_model_name(base_model),
            "complex_model": _normalize_gemini_model_name(complex_model),
            "gemini_api_key": api_key,
        }

        api_base = _normalize_gemini_api_base(os.getenv("GOOGLE_API_BASE"))
        if api_base:
            config["model_api_base"] = api_base

        return config

    if provider == "vllm":
        base_model = os.getenv("BASE_MODEL", "Qwen3-8B-AWQ")
        return {
            "base_provider": "openai",
            "complex_provider": "openai",
            "base_model": base_model,
            "complex_model": os.getenv("COMPLEX_MODEL", base_model),
            "openai_api_key": os.getenv("VLLM_API_KEY", "sk-local-noop"),
            "openai_api_base": os.getenv("MODEL_API_BASE", "http://localhost:8000/v1"),
        }

    raise ValueError(f"Proveedor no soportado: {provider}")


def resolve_weaviate_cfg() -> dict:
    """Resuelve configuración de Weaviate desde variables de entorno."""
    return {
        "weaviate_is_local": True,
        "weaviate_http_port": 8080,
        "weaviate_grpc_port": 50051,
        "weaviate_url": "http://localhost:8080",
        "weaviate_api_key": "",
    }

    return {
        "weaviate_is_local": os.getenv("WEAVIATE_IS_LOCAL", "1") in ("1", "true", "True"),
        "weaviate_http_port": int(os.getenv("LOCAL_WEAVIATE_PORT", os.getenv("WEAVIATE_HTTP_PORT", 8080))),
        "weaviate_grpc_port": int(os.getenv("LOCAL_WEAVIATE_GRPC_PORT", os.getenv("WEAVIATE_GRPC_PORT", 50051))),
        "weaviate_url": os.getenv("WCD_URL", os.getenv("WEAVIATE_URL", "http://localhost:8080")),
        "weaviate_api_key": os.getenv("WCD_API_KEY", ""),
    }


def make_patched_get_client():
    """Crea cliente Weaviate con configuración correcta."""
    cfg = resolve_weaviate_cfg()

    def _patched_get_client(self):
        import weaviate
        from weaviate.auth import AuthCredentials

        if cfg["weaviate_is_local"]:
            client = weaviate.WeaviateClient(
                connection_params=weaviate.connect.ConnectionParams(
                    http=weaviate.connect.ProtocolParams(
                        host="localhost", port=cfg["weaviate_http_port"], secure=False
                    ),
                    grpc=weaviate.connect.ProtocolParams(
                        host="localhost", port=cfg["weaviate_grpc_port"], secure=False
                    ),
                )
            )
            client.connect()
            return client
        else:
            auth = (
                AuthCredentials.from_api_key(cfg["weaviate_api_key"])
                if cfg["weaviate_api_key"] else None
            )
            client = weaviate.WeaviateClient(
                connection_params=weaviate.connect.ConnectionParams.from_url(
                    url=cfg["weaviate_url"], grpc_port=cfg["weaviate_grpc_port"]
                ),
                auth_client_secret=auth,
            )
            client.connect()
            return client

    return _patched_get_client


# -----------------------------
# Inicializador de Elysia Tree
# -----------------------------

_tree_instance = None
_tree_provider = None
_env_loaded = False


def _load_env_defaults() -> None:
    """Carga variables desde archivos .elysia_env si existen."""

    global _env_loaded
    if _env_loaded:
        return

    candidates = []
    env_file = os.getenv("ELYSIA_ENV_FILE")
    if env_file:
        candidates.append(Path(env_file))

    module_dir = Path(__file__).resolve().parent
    candidates.append(module_dir / ".elysia_env")
    candidates.append(module_dir.parent / "elysia" / ".elysia_env")
    candidates.append(module_dir.parent / ".elysia_env")

    for path in candidates:
        if not path or not path.is_file():
            continue

        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue

                if "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                value = os.path.expandvars(value)

                if key and value and (key not in os.environ or not os.environ[key]):
                    os.environ[key] = value

    _env_loaded = True


def initialize_elysia_tree(provider: str):
    """Inicializa Elysia Tree con la configuración del proveedor."""
    global _tree_instance, _tree_provider

    # Reutilizar instancia si el proveedor no cambió
    if _tree_instance is not None and _tree_provider == provider:
        return _tree_instance

    from elysia import Settings, Tree
    from elysia.util.client import ClientManager

    # Obtener configuración
    llm_config = get_elysia_config_from_provider(provider)
    weaviate_config = resolve_weaviate_cfg()

    # IMPORTANTE: LiteLLM necesita GEMINI_API_KEY en el entorno global
    # Setear antes de crear Settings
    if "gemini_api_key" in llm_config and llm_config["gemini_api_key"]:
        os.environ["GEMINI_API_KEY"] = llm_config["gemini_api_key"]

    # Crear Settings personalizado
    my_settings = Settings()

    # Configurar usando el método configure del Settings
    configure_kwargs: Dict[str, Any] = {
        "base_provider": llm_config["base_provider"],
        "complex_provider": llm_config["complex_provider"],
        "base_model": llm_config["base_model"],
        "complex_model": llm_config["complex_model"],
        "weaviate_is_local": weaviate_config["weaviate_is_local"],
        "local_weaviate_port": int(weaviate_config["weaviate_http_port"]),
        "local_weaviate_grpc_port": int(weaviate_config["weaviate_grpc_port"]),
    }

    configure_kwargs["wcd_url"] = weaviate_config["weaviate_url"]
    if weaviate_config["weaviate_api_key"]:
        configure_kwargs["wcd_api_key"] = weaviate_config["weaviate_api_key"]

    # Propaga claves API relevantes sin asumir proveedor específico
    for key in ("openai_api_key", "openai_api_base", "gemini_api_key", "model_api_base"):
        value = llm_config.get(key)
        if value:
            configure_kwargs[key] = value

    my_settings.configure(**configure_kwargs)

    # my_settings.configure(**resolve_weaviate_cfg())
    weaviate_config = resolve_weaviate_cfg()
    my_settings.configure(
        weaviate_is_local=weaviate_config["weaviate_is_local"],
        wcd_url=weaviate_config["weaviate_url"],
        local_weaviate_port=weaviate_config["weaviate_http_port"],
        local_weaviate_grpc_port=weaviate_config["weaviate_grpc_port"],
    )

    # Crear Tree con Settings personalizado y patch del cliente
    with patch.object(ClientManager, "get_client", make_patched_get_client()):
        tree = Tree(
            low_memory=False,
            settings=my_settings,  # Pasar Settings personalizado
            style="""
            Responde ÚNICAMENTE en español.
            NUNCA uses inglés en las respuestas.
            Proporciona respuestas concisas y directas basadas en las noticias encontradas.
            Enfócate en la información más relevante y reciente.
            Cuando la consulta pida "últimas" o noticias recientes, interpreta ese rango como los últimos 7 días (desde hoy menos 7 días, inclusive) en lugar de sólo el día actual. Aplica ese criterio al filtrar por fechas.
            """
        )

    _tree_instance = tree
    _tree_provider = provider

    return tree


# -----------------------------
# Herramienta LangChain
# -----------------------------

class ElysiaNewsOracleTool(BaseTool):
    """Herramienta para buscar noticias en la base de datos Weaviate usando Elysia."""

    name: str = "elysia_news_oracle"
    description: str = (
        "Busca noticias en la base de datos local de noticias de Málaga y España. "
        "Usa esta herramienta cuando necesites información específica sobre noticias locales, "
        "eventos en Málaga/España, o cualquier tema periodístico almacenado en nuestra base de datos. "
        "Devuelve respuestas basadas en artículos reales con sus fuentes."
    )
    provider: str = Field(default="vllm", description="Proveedor LLM (gemini o vllm)")

    def _run(self, query: str) -> str:
        """Ejecuta la búsqueda en Elysia y formatea los resultados."""
        response: str = ""
        objects: Optional[List[Any]] = None
        tree_error: Optional[Exception] = None

        try:
            # Inicializar Tree
            tree = initialize_elysia_tree(self.provider)

            # Ejecutar consulta con patch del cliente
            from elysia.util.client import ClientManager
            with patch.object(ClientManager, "get_client", make_patched_get_client()):
                response, objects = tree(query)

        except Exception as exc:
            tree_error = exc
            logger.warning(
                "Fallo en Elysia Tree; usando búsqueda directa como fallback: %s",
                exc,
                exc_info=True,
            )

        # Formatear respuesta con fuentes (permite que el fallback recupere resultados)
        formatted_response = self._format_response(query, response, objects)
        if formatted_response.strip():
            return formatted_response

        if tree_error:
            return (
                f"{NO_RESULTS_FALLBACK} "
                "Se intentó una búsqueda directa tras un error temporal del árbol principal."
            )

        return NO_RESULTS_FALLBACK

    async def _arun(self, query: str) -> str:
        """Versión asíncrona (usa la síncrona por ahora)."""
        return self._run(query)

    def _format_response(self, query: str, response: str, objects: Optional[List[Any]]) -> str:
        """Formatea la respuesta con las fuentes encontradas."""
        result = response.strip()

        articles: List[Dict[str, str]] = []
        if objects:
            articles = self._extract_articles(objects)

        needs_recency_boost = self._should_expand_for_latest(query)

        if not articles or self._looks_like_no_results(result):
            fallback_limit = 16 if needs_recency_boost else 30
            fallback_articles = self._direct_weaviate_search(
                query,
                limit=fallback_limit,
            )
            if fallback_articles:
                articles = fallback_articles
            elif not articles:
                return result or NO_RESULTS_FALLBACK
        elif needs_recency_boost and len(articles) < 12:
            boosted_articles = self._direct_weaviate_search(query, limit=16)
            if boosted_articles:
                articles = boosted_articles

        articles = self._filter_recent_articles(articles, days=7)
        articles = self._sort_articles_by_date_desc(articles)
        if needs_recency_boost:
            articles = articles[:16]

        result = self._build_summary_from_articles(query, articles)

        # Agregar sección de fuentes
        result += "\n\n📰 **Fuentes consultadas:**"
        for i, article in enumerate(articles[:5], 1):
            title = article.get("title", "Sin título")
            url = article.get("url", article.get("sourceUrl", "URL no disponible"))
            source = article.get("source", "")

            result += f"\n{i}. {title}"
            if source:
                result += f" — _{source}_"
            if url and url != "URL no disponible":
                result += f"\n   {url}"

        return result

    def _extract_articles(self, objects: List[Any]) -> List[Dict[str, str]]:
        """Extrae información de artículos de los objetos devueltos."""
        articles = []

        for obj in objects:
            if isinstance(obj, dict):
                article = {
                    "title": obj.get("title", ""),
                    "url": obj.get("sourceUrl", obj.get("url", obj.get("link", ""))),
                    "source": obj.get("source", ""),
                    "published": obj.get("publishedAt") or obj.get("published_at") or obj.get("date"),
                }
                if article["title"]:  # Solo agregar si tiene título
                    articles.append(article)
            elif hasattr(obj, '__iter__'):
                # Manejar objetos anidados
                try:
                    for item in obj:
                        if isinstance(item, dict):
                            article = {
                                "title": item.get("title", ""),
                                "url": item.get("sourceUrl", item.get("url", item.get("link", ""))),
                                "source": item.get("source", ""),
                                "published": item.get("publishedAt") or item.get("published_at") or item.get("date"),
                            }
                            if article["title"]:
                                articles.append(article)
                except Exception:
                    pass

        seen = set()
        deduped: List[Dict[str, str]] = []
        for article in articles:
            key = (article["title"], article["url"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(article)

        return deduped

    # -----------------------------
    # Helpers de formateo
    # -----------------------------

    def _looks_like_no_results(self, text: str) -> bool:
        lowered = text.lower()
        patterns = [
            "no he encontrado",
            "no se encontraron",
            "no dispongo de noticias",
            "no hay noticias",
            "no se hallaron",
        ]
        return any(p in lowered for p in patterns)

    def _parse_datetime(self, value: str) -> Optional[datetime]:
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _filter_recent_articles(self, articles: List[Dict[str, str]], days: int) -> List[Dict[str, str]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        filtered: List[Dict[str, str]] = []
        for article in articles:
            dt = self._parse_datetime(article.get("published", ""))
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                cutoff_comp = cutoff
                if cutoff_comp.tzinfo is None:
                    cutoff_comp = cutoff_comp.replace(tzinfo=timezone.utc)
                if dt >= cutoff_comp:
                    filtered.append(article)
        return filtered or articles

    def _sort_articles_by_date_desc(self, articles: List[Dict[str, str]]) -> List[Dict[str, str]]:
        def _sort_key(article: Dict[str, str]) -> datetime:
            dt = self._parse_datetime(article.get("published", "")) or datetime.min.replace(tzinfo=timezone.utc)
            return dt

        return sorted(articles, key=_sort_key, reverse=True)

    def _should_expand_for_latest(self, query: str) -> bool:
        lowered = query.lower()
        keywords = (
            "última",
            "ultimas",
            "últimas",
            "último",
            "últimos",
            "recientes",
            "última hora",
            "último momento",
            "actualidad",
        )
        return any(keyword in lowered for keyword in keywords)

    def _build_summary_from_articles(self, query: str, articles: List[Dict[str, str]]) -> str:
        pieces: List[str] = []
        pieces.append(
            f"Aquí tienes las noticias más recientes encontradas para la consulta: {query}"
        )

        for article in articles[:5]:
            title = article.get("title", "Sin título")
            source = article.get("source", "Fuente no indicada") or "Fuente no indicada"
            published = article.get("published", "Fecha no disponible") or "Fecha no disponible"
            line = f"- {title} — {source} ({published})"
            pieces.append(line)

        if len(articles) > 5:
            pieces.append(f"… y {len(articles) - 5} artículos adicionales en los últimos 7 días.")

        return "\n".join(pieces)

    def _direct_weaviate_search(
        self,
        query: str,
        days: int = 7,
        limit: int = 30,
        sort_field: Optional[str] = "publishedAt",
    ) -> List[Dict[str, str]]:
        try:
            import requests
        except ImportError:
            return []

        cfg = resolve_weaviate_cfg()
        base_url = cfg["weaviate_url"].rstrip("/")
        headers = {"Content-Type": "application/json"}
        if cfg["weaviate_api_key"]:
            headers["Authorization"] = f"Bearer {cfg['weaviate_api_key']}"

        class_name = os.getenv("WEAVIATE_NEWS_CLASS", "News")

        try:
            schema_resp = requests.get(f"{base_url}/v1/schema", headers=headers, timeout=10)
            schema_resp.raise_for_status()
            schema = schema_resp.json()
        except Exception:
            schema = {}

        prop_configs: Dict[str, Dict[str, Any]] = {}
        for cls in schema.get("classes", []):
            if cls.get("class") == class_name:
                for prop in cls.get("properties", []):
                    name = prop.get("name")
                    if name:
                        prop_configs[name] = prop
                break

        def _is_filterable(prop: Dict[str, Any]) -> bool:
            if not prop:
                return False

            if "indexFilterable" in prop:
                return bool(prop.get("indexFilterable"))

            # Compatibilidad con versiones antiguas de Weaviate (<1.24)
            if "indexInverted" in prop:
                return bool(prop.get("indexInverted"))

            # Si no hay información explícita asumimos que es filtrable (comportamiento clásico)
            return True

        filterable_props = {name for name, cfg in prop_configs.items() if _is_filterable(cfg)}

        default_candidates = [
            os.getenv("WEAVIATE_NEWS_DATE_FIELD", 'publishedAt'),
            "published_at",
            "publishedAt",
            "publishedDate",
            "date",
            "published",
            "timestamp",
        ]
        candidate_dates = [c for c in default_candidates if c]

        fields_to_try: List[Optional[str]] = []
        if filterable_props:
            for candidate in candidate_dates:
                if candidate in filterable_props and candidate not in fields_to_try:
                    fields_to_try.append(candidate)
        if sort_field and sort_field in filterable_props and sort_field not in fields_to_try:
            fields_to_try.insert(0, sort_field)
        if None not in fields_to_try:
            fields_to_try.append(None)

        bm25_props = [
            prop for prop in ("title", "body") if prop in prop_configs
        ]
        if not bm25_props:
            bm25_props = ["title"]
        bm25_props_serialized = ", ".join(f"\"{prop}\"" for prop in bm25_props)

        windows = [days]
        if days < 21:
            windows.append(21)

        safe_query = query.replace("\\", "\\\\").replace("\"", "\\\"")

        for window in windows:
            now_utc = datetime.now(timezone.utc)
            start = now_utc - timedelta(days=window)
            end = now_utc
            start_iso = start.strftime("%Y-%m-%dT00:00:00Z")
            end_iso = end.strftime("%Y-%m-%dT23:59:59Z")

            for date_field in fields_to_try:
                where_clause = ""
                if date_field:
                    where_clause = f"""
      where: {{
        operator: And,
        operands: [
          {{ path: [\"{date_field}\"], operator: GreaterThanEqual, valueDate: \"{start_iso}\" }},
          {{ path: [\"{date_field}\"], operator: LessThanEqual, valueDate: \"{end_iso}\" }}
        ]
      }}
"""

                graphql_query = f"""
{{
  Get {{
    {class_name}(
      bm25: {{
        query: \"{safe_query}\",
        properties: [{bm25_props_serialized}]
      }}
{where_clause}
      limit: {limit}
    ) {{
      title
      source
      sourceUrl
      publishedAt
      _additional {{ score }}
    }}
  }}
}}
"""

                try:
                    resp = requests.post(
                        f"{base_url}/v1/graphql",
                        headers=headers,
                        json={"query": graphql_query},
                        timeout=20,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception:
                    continue

                if data.get("errors"):
                    continue

                hits = (
                    data.get("data", {})
                    .get("Get", {})
                    .get(class_name, [])
                )

                articles: List[Dict[str, str]] = []
                for hit in hits:
                    title = hit.get("title")
                    if not title:
                        continue
                    published = (
                        hit.get("publishedAt")
                        or hit.get("published_at")
                        or hit.get("publishedDate")
                        or hit.get("date")
                    )
                    url = hit.get("sourceUrl") or hit.get("url") or hit.get("link")
                    source = hit.get("source") or ""
                    score = None
                    additional = hit.get("_additional")
                    if isinstance(additional, dict):
                        score = additional.get("score")

                    entry = {
                        "title": str(title).strip(),
                        "source": str(source).strip(),
                        "url": str(url).strip() if url else "",
                        "published": str(published).strip() if published else "",
                    }
                    if score is not None:
                        entry["score"] = score
                    articles.append(entry)

                if articles:
                    return articles

        return []


# -----------------------------
# Función helper para crear la herramienta
# -----------------------------

def create_elysia_tool(provider: str) -> ElysiaNewsOracleTool:
    """Crea una instancia de la herramienta Elysia configurada con el proveedor."""
    return ElysiaNewsOracleTool(provider=provider)
