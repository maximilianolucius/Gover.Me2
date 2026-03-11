#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from elysia import configure, Tree, preprocess
from unittest.mock import patch


# -----------------------------
# Logging utilities (from ingest script)
# -----------------------------

def log(msg: str) -> None:
    """Enhanced logging with timestamps"""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)


# -----------------------------
# Tolerant Elysia settings resolver
# -----------------------------
GEMINI_MODEL = False
ElysiaSettings = None
elysia_settings_singleton = None

try:
    from elysia import Settings as ElysiaSettings  # type: ignore
except Exception:
    try:
        from elysia.settings import settings as elysia_settings_singleton  # type: ignore
    except Exception:
        pass


def resolve_cfg():
    """Return a dict with Weaviate config, regardless of Elysia version."""
    if ElysiaSettings is not None:
        s = ElysiaSettings()
        return {
            "weaviate_is_local": getattr(s, "weaviate_is_local", True),
            "weaviate_http_port": int(getattr(s, "weaviate_http_port", 8080)),
            "weaviate_grpc_port": int(getattr(s, "weaviate_grpc_port", 50051)),
            "weaviate_url": getattr(s, "weaviate_url", "http://localhost:8080"),
            "weaviate_api_key": getattr(s, "weaviate_api_key", os.getenv("WCD_API_KEY", "")),
        }
    if elysia_settings_singleton is not None:
        s = elysia_settings_singleton
        return {
            "weaviate_is_local": bool(getattr(s, "weaviate_is_local", True)),
            "weaviate_http_port": int(getattr(s, "weaviate_http_port", 8080)),
            "weaviate_grpc_port": int(getattr(s, "weaviate_grpc_port", 50051)),
            "weaviate_url": getattr(s, "weaviate_url", "http://localhost:8080"),
            "weaviate_api_key": getattr(s, "weaviate_api_key", os.getenv("WCD_API_KEY", "")),
        }
    # Fallback to env vars
    return {
        "weaviate_is_local": os.getenv("WEAVIATE_IS_LOCAL", "1") in ("1", "true", "True"),
        "weaviate_http_port": int(os.getenv("LOCAL_WEAVIATE_PORT", os.getenv("WEAVIATE_HTTP_PORT", 8080))),
        "weaviate_grpc_port": int(os.getenv("LOCAL_WEAVIATE_GRPC_PORT", os.getenv("WEAVIATE_GRPC_PORT", 50051))),
        "weaviate_url": os.getenv("WCD_URL", os.getenv("WEAVIATE_URL", "http://localhost:8080")),
        "weaviate_api_key": os.getenv("WCD_API_KEY", ""),
    }


# -----------------------------
# Enhanced networking helpers (improved from ingest script)
# -----------------------------

def create_session():
    """Create a configured requests session"""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session


def test_weaviate_connection(timeout=(10, 30)):
    """Enhanced Weaviate connection test with better error handling"""
    session = create_session()
    weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080").rstrip("/")

    try:
        log("🔍 Testing Weaviate connection...")
        response = session.get(f"{weaviate_url}/v1/meta", timeout=timeout)
        if response.status_code == 200:
            meta = response.json()
            version = meta.get('version', 'unknown')
            log(f"✅ Weaviate is running (version: {version})")
            return True, meta
        else:
            log(f"⚠️ Weaviate responded with status {response.status_code}")
            return False, None
    except requests.exceptions.ConnectionError:
        log(f"❌ Cannot connect to Weaviate at {weaviate_url}")
        return False, None
    except requests.exceptions.Timeout:
        log(f"⏰ Timeout connecting to Weaviate at {weaviate_url}")
        return False, None
    except Exception as e:
        log(f"❌ Unexpected error testing Weaviate connection: {e}")
        return False, None


def check_weaviate_readiness(timeout=(5, 15)):
    """Check if Weaviate is ready to accept requests"""
    session = create_session()
    weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080").rstrip("/")

    try:
        response = session.get(f"{weaviate_url}/v1/.well-known/ready", timeout=timeout)
        if response.status_code == 200:
            log("✅ Weaviate is ready")
            return True
        else:
            log(f"⚠️ Weaviate not ready: {response.status_code}")
            return False
    except Exception as e:
        log(f"⚠️ Could not verify Weaviate readiness: {e}")
        return False


def check_weaviate_modules():
    """Check available Weaviate modules with enhanced error handling"""
    session = create_session()
    weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080").rstrip("/")

    try:
        response = session.get(f"{weaviate_url}/v1/modules", timeout=(5, 15))
        if response.status_code == 200:
            modules = response.json()
            module_names = list(modules.keys())
            log(f"📦 Available Weaviate modules: {module_names}")

            # Check for important modules
            recommended_modules = ["text2vec-transformers", "qna-transformers", "bm25"]
            missing_modules = [m for m in recommended_modules if m not in module_names]
            if missing_modules:
                log(f"⚠️ Missing recommended modules: {missing_modules}")

            return modules
        else:
            log(f"⚠️ Could not check modules: HTTP {response.status_code}")
            return {}
    except Exception as e:
        log(f"⚠️ Could not check modules: {e}")
        return {}


def check_news_class_exists():
    """Check if the News class exists in Weaviate schema"""
    session = create_session()
    weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080").rstrip("/")

    try:
        response = session.get(f"{weaviate_url}/v1/schema", timeout=(5, 15))
        if response.status_code == 200:
            schema = response.json()
            classes = schema.get("classes", [])
            news_classes = [c for c in classes if c.get("class") == "News"]

            if news_classes:
                news_class = news_classes[0]
                vectorizer = news_class.get("vectorizer", "none")
                properties = [p.get("name") for p in news_class.get("properties", [])]
                log(f"✅ News class found with vectorizer: {vectorizer}")
                log(f"📋 Properties: {properties}")
                return True, news_class
            else:
                log("⚠️ News class not found in schema")
                return False, None
        else:
            log(f"⚠️ Could not check schema: HTTP {response.status_code}")
            return False, None
    except Exception as e:
        log(f"⚠️ Could not check News class: {e}")
        return False, None


def count_news_objects():
    """Count objects in the News class"""
    session = create_session()
    weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080").rstrip("/")

    try:
        # Use GraphQL aggregate query to count objects
        query = {
            "query": "{ Aggregate { News { meta { count } } } }"
        }
        response = session.post(f"{weaviate_url}/v1/graphql",
                                json=query, timeout=(5, 15))
        if response.status_code == 200:
            data = response.json()
            if "data" in data and "Aggregate" in data["data"]:
                count = data["data"]["Aggregate"]["News"][0]["meta"]["count"]
                log(f"📊 News objects in database: {count}")
                return count

        # Fallback method if GraphQL fails
        response = session.get(f"{weaviate_url}/v1/objects?class=News&limit=1",
                               timeout=(5, 15))
        if response.status_code == 200:
            data = response.json()
            total_results = data.get("totalResults", 0)
            log(f"📊 News objects in database: {total_results}")
            return total_results

        return 0
    except Exception as e:
        log(f"⚠️ Could not count News objects: {e}")
        return 0


def make_patched_get_client():
    cfg = resolve_cfg()

    def _patched_get_client(self):
        import weaviate
        from weaviate.auth import AuthCredentials

        def _configure_weaviate_vectorizer(c):
            """Configure Weaviate to use built-in vectorizers instead of forcing 'none'"""
            try:
                from weaviate.classes.config import Configure
                orig_create = c.collections.create

                async def create_with_builtin_vectorizer(*args, **kwargs):
                    # Only set vectorizer if not already specified
                    if "vectorizer_config" not in kwargs:
                        # Try to use text2vec-transformers (most common)
                        # If it fails, Weaviate will fall back to default behavior
                        try:
                            kwargs["vectorizer_config"] = Configure.Vectorizer.text2vec_transformers()
                        except Exception:
                            # If text2vec-transformers is not available, try other common ones
                            try:
                                kwargs["vectorizer_config"] = Configure.Vectorizer.text2vec_openai()
                            except Exception:
                                # Let Weaviate use its default vectorizer
                                pass

                    return await orig_create(*args, **kwargs)

                c.collections.create = create_with_builtin_vectorizer  # type: ignore
            except Exception as e:
                # If anything goes wrong, keep default behavior
                log(f"⚠️ Could not configure vectorizer: {e}")
                pass

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
            _configure_weaviate_vectorizer(client)
            return client
        else:
            auth = (
                AuthCredentials.from_api_key(cfg["weaviate_api_key"]) if cfg["weaviate_api_key"] else None
            )
            client = weaviate.WeaviateClient(
                connection_params=weaviate.connect.ConnectionParams.from_url(
                    url=cfg["weaviate_url"], grpc_port=cfg["weaviate_grpc_port"]
                ),
                auth_client_secret=auth,
            )
            client.connect()
            _configure_weaviate_vectorizer(client)
            return client

    return _patched_get_client


# -----------------------------
# Enhanced preprocessing with progress tracking
# -----------------------------

def run_preprocessing_with_monitoring():
    """Run preprocessing with enhanced monitoring and error handling"""

    return True


    log("🔄 Starting News collection preprocessing...")
    log("This may take several minutes depending on data size...")

    start_time = time.time()
    try:
        preprocess(
            ["News"],
            max_sample_size=5,
            force=True,
        )

        elapsed = time.time() - start_time
        log(f"✅ Preprocessing completed successfully in {elapsed:.2f} seconds!")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        log(f"❌ Preprocessing failed after {elapsed:.2f} seconds: {e}")
        import traceback
        traceback.print_exc()
        return False


# -----------------------------
# Enhanced query testing with better output formatting
# -----------------------------

def run_enhanced_query_test():
    """Run query test with enhanced output formatting and error handling"""
    log("=" * 60)
    log("🔍 Testing Elysia Tree with sample queries...")
    log("=" * 60)

    try:
        tree = Tree(low_memory=False,
                    style="""
        Responde ÚNICAMENTE en español. 
        NUNCA uses inglés en las respuestas.
        NUNCA expliques qué estás haciendo o buscando - simplemente da la respuesta.
        NO digas cosas como "Estoy buscando...", "He encontrado...", "Déjame revisar...".
        """,
                    )

        # Test queries in Spanish (matching the domain)
        test_queries = [
            # "¿Cuáles son las novedades de Málaga?",
            # "Noticias sobre tecnología recientes",
            # "¿Qué está pasando en España?",
            "Dime las ultimas noticias de incendios?",
        ]
        test_queries = [
            "Dime las ultimas noticias de incendios?"
        ]

        for i, question in enumerate(test_queries, 1):
            log(f"\n🔍 Query {i}: {question}")

            start_time = time.time()
            try:
                response, objects = tree(question)
                query_time = time.time() - start_time

                log("\n📊 Response:")
                log("-" * 50)
                print(response)

                log(f"\n⏱️ Query completed in {query_time:.2f} seconds")
                log(f"📈 Retrieved {len(objects) if objects else 0} objects")

                if objects:
                    display_enhanced_results(objects)
                else:
                    log("\n🤔 No objects retrieved (the model may have answered from its knowledge)")

                # Add spacing between queries
                if i < len(test_queries):
                    log("\n" + "─" * 60)

            except Exception as e:
                query_time = time.time() - start_time
                log(f"\n❌ Query failed after {query_time:.2f} seconds: {e}")

        return True

    except Exception as e:
        log(f"\n❌ Query test setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def display_enhanced_results(objects):
    """Enhanced result display with better formatting and Spanish support"""
    # Enhanced Top-8 sources display (improved scoring and formatting)
    log("\n📚 Top-8 fuentes más importantes:")
    log("-" * 50)

    def get_score(o: dict):
        # Try various common score field names
        for k in ("score", "_score", "similarity", "certainty"):
            if isinstance(o.get(k, None), (int, float)):
                return float(o[k])
        # Distance: lower is better, invert for descending sort
        if isinstance(o.get("distance", None), (int, float)):
            try:
                return 1.0 / (1e-9 + float(o["distance"]))
            except Exception:
                return 0.0
        return None

    def get_label(o: dict):
        # Choose readable label from source
        for k in ("title", "source", "url", "link", "filename", "name"):
            v = o.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        body = o.get("body")
        if isinstance(body, str) and body.strip():
            return (body.strip()[:80] + "...") if len(body) > 80 else body.strip()
        # Generic fallback
        return "[fuente sin metadatos reconocibles]"

    # Sort by score if available, otherwise maintain original order
    indexed = list(enumerate(objects))

    print(f'objects: {objects}')


    if any(isinstance(obj, dict) and get_score(obj) is not None for obj in objects):
        indexed.sort(key=lambda iv: (get_score(iv[1]) or 0.0), reverse=True)

    top_k = [iv[1] for iv in indexed[:8]]
    for i, obj in enumerate(top_k, 1):
        print(obj)
        if isinstance(obj, dict):
            label = get_label(obj)
            score = get_score(obj)
            if score is not None:
                log(f"{i:2d}. {label}  (score: {score:.4f})")
            else:
                log(f"{i:2d}. {label}")
        else:
            log(f"{i:2d}. {str(obj)[:80]}...")
            # Handle nested news objects
            if hasattr(obj, '__iter__'):
                try:
                    for news in obj:
                        if isinstance(news, dict):
                            title = news.get("title", "No disponible")
                            source = news.get("source", "No disponible")
                            published = news.get("publishedAt", "No disponible")
                            source_url = news.get("sourceUrl", "No disponible")
                            log(f'    Title: {title}')
                            log(f'    Source: {source}')
                            log(f'    Published: {published}')
                            log(f'    URL: {source_url}')
                except Exception:
                    pass

    # Enhanced detailed object display
    log("\n🔍 Detailed Retrieved Objects:")
    log("-" * 50)
    for i, obj in enumerate(objects[:5], 1):  # Limit to first 5 for readability
        log(f"\nObject {i}:")
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "body":
                    # Truncate body for readability
                    if isinstance(value, str) and len(value) > 200:
                        log(f"  {key}: {value[:200]}...")
                    else:
                        log(f"  {key}: {value}")
                else:
                    log(f"  {key}: {value}")
        else:
            log(f"  {str(obj)[:200]}...")

    if len(objects) > 5:
        log(f"\n... and {len(objects) - 5} more objects")


# -----------------------------
# Main function with comprehensive error handling
# -----------------------------

def main():
    log("🚀 Starting Enhanced Elysia Local Demo")
    log("=" * 60)

    # Load environment variables
    load_dotenv(dotenv_path=".elysia_env", override=True)

    # Suppress CUDA warnings for CPU-only runs
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    # Enhanced Weaviate connection testing
    is_connected, meta = test_weaviate_connection()
    if not is_connected:
        log("❌ Please ensure Weaviate is running on localhost:8080")
        log("💡 Try: docker run -p 8080:8080 -p 50051:50051 cr.weaviate.io/semitechnologies/weaviate:1.26.1")
        sys.exit(1)

    # Check readiness
    if not check_weaviate_readiness():
        log("⚠️ Weaviate is not ready, but continuing anyway...")

    # Check available modules
    modules = check_weaviate_modules()

    # Check News class and data
    class_exists, news_class = check_news_class_exists()
    if not class_exists:
        log("⚠️ News class not found. You may need to run ingestion first.")
        log("💡 Try running: python ingest_weaviate_news_2.py")
    else:
        object_count = count_news_objects()
        if object_count == 0:
            log("⚠️ News class exists but contains no data.")
            log("💡 Try running: python ingest_weaviate_news_2.py")

    # Import after env load to respect settings
    try:
        from elysia.util.client import ClientManager
    except ImportError as e:
        log(f"❌ Failed to import Elysia ClientManager: {e}")
        sys.exit(1)

    # Enhanced model configuration
    vllm_api_key = os.getenv("VLLM_API_KEY", "sk-local-elysia-noop")
    model_api_base = os.getenv("MODEL_API_BASE", "http://localhost:8000/v1")
    base_model = os.getenv("BASE_MODEL", "Qwen3-8B-AWQ")
    complex_model = os.getenv("COMPLEX_MODEL", base_model)

    # Weaviate url (used by configure; actual client comes from our patch)
    wcd_url = os.getenv("WCD_URL", "http://localhost:8080")

    # Point all OpenAI-compatible clients to vLLM (exclusively)
    os.environ["OPENAI_API_KEY"] = vllm_api_key
    os.environ["OPENAI_BASE_URL"] = model_api_base
    os.environ["OPENAI_API_BASE"] = model_api_base

    log("\n🔧 Configuring Elysia...")
    try:
        if GEMINI_MODEL:
            api_key = os.getenv("GOOGLE_API_KEY", "AIzaSyDdzsIWFVdKy-gWl8pfrFV_f39F2ns4gAI")
            openai_api_base = os.getenv("GOOGLE_API_BASE", "https://generativelanguage.googleapis.com/v1beta/openai/")
            base_model = os.getenv("GOOGLE_BASE_MODEL", "gemini-2.5-flash-lite")
            complex_model = os.getenv("GOOGLE_COMPLEX_MODEL", 'gemini-2.5-flash')

            log(f"📡 Using Gemini models: {base_model} / {complex_model}")
            configure(
                base_provider="openai",
                complex_provider="openai",
                base_model=base_model,
                complex_model=complex_model,
                openai_api_key=api_key,
                openai_api_base=openai_api_base,
                weaviate_is_local=True,
                wcd_url=wcd_url,
                local_weaviate_port=int(os.getenv("LOCAL_WEAVIATE_PORT", 8080)),
                local_weaviate_grpc_port=int(os.getenv("LOCAL_WEAVIATE_GRPC_PORT", 50051)),
            )
        else:
            log(f"🤖 Using local models: {base_model} / {complex_model}")
            configure(
                # LLM settings (vLLM with OpenAI-compatible API)
                base_provider="openai",
                complex_provider="openai",
                base_model=base_model,
                complex_model=complex_model,
                openai_api_key=vllm_api_key,
                openai_api_base=model_api_base,
                # Weaviate settings
                weaviate_is_local=True,  # local demo
                wcd_url=wcd_url,
                local_weaviate_port=int(os.getenv("LOCAL_WEAVIATE_PORT", 8080)),
                local_weaviate_grpc_port=int(os.getenv("LOCAL_WEAVIATE_GRPC_PORT", 50051)),
            )
    except Exception as e:
        log(f"❌ Failed to configure Elysia: {e}")
        sys.exit(1)

    # Patch the ClientManager.get_client to support Weaviate built-in vectorizers
    log("🔧 Configuring Weaviate client to use built-in embeddings...")
    with patch.object(ClientManager, "get_client", make_patched_get_client()):
        try:
            # Enhanced preprocessing with monitoring
            if not run_preprocessing_with_monitoring():
                log("❌ Preprocessing failed, but continuing with query test...")

            # Enhanced query testing
            if not run_enhanced_query_test():
                log("❌ Query test failed")
                sys.exit(1)

            log("\n🎉 Enhanced demo completed successfully!")

        except KeyboardInterrupt:
            log("\n⏸️ Demo interrupted by user")
            sys.exit(0)
        except Exception as e:
            log(f"\n❌ Unexpected error during execution: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()