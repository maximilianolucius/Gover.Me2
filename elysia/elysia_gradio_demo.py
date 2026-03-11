#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gradio app para Elysia Local Demo 5

Lanza una interfaz sencilla para:
- Formular una pregunta contra la colección "News"
- Ver la respuesta del árbol de Elysia
- Ver las Top‑8 fuentes que contribuyeron (según score u orden)

Uso:
  python elysia_local_demo_5_gradio.py

Requisitos:
- Weaviate en localhost:8080
- vLLM (o proxy OpenAI‑compatible) expuesto en http://localhost:8000/v1 (por defecto)
- Variables en .elysia_env según tu entorno
"""

import os
import sys
import re
from typing import Any, Dict, List, Tuple
from collections import OrderedDict

import gradio as gr
from dotenv import load_dotenv
from unittest.mock import patch

from elysia import configure, Tree, preprocess


# -----------------------------
# Resolución de settings de Elysia/Weaviate
# -----------------------------

GEMINI_MODEL = True
ElysiaSettings = None
elysia_settings_singleton = None

try:
    from elysia import Settings as ElysiaSettings  # type: ignore
except Exception:
    try:
        from elysia.settings import settings as elysia_settings_singleton  # type: ignore
    except Exception:
        pass


def resolve_cfg() -> Dict[str, Any]:
    """Devuelve un dict con config de Weaviate compatible con distintas versiones de Elysia."""
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
    return {
        "weaviate_is_local": os.getenv("WEAVIATE_IS_LOCAL", "1") in ("1", "true", "True"),
        "weaviate_http_port": int(os.getenv("LOCAL_WEAVIATE_PORT", os.getenv("WEAVIATE_HTTP_PORT", 8080))),
        "weaviate_grpc_port": int(os.getenv("LOCAL_WEAVIATE_GRPC_PORT", os.getenv("WEAVIATE_GRPC_PORT", 50051))),
        "weaviate_url": os.getenv("WCD_URL", os.getenv("WEAVIATE_URL", "http://localhost:8080")),
        "weaviate_api_key": os.getenv("WCD_API_KEY", ""),
    }


# -----------------------------
# Networking helpers
# -----------------------------

def test_weaviate_connection() -> bool:
    import requests
    try:
        response = requests.get("http://localhost:8080/v1/meta")
        if response.status_code == 200:
            print(f"✅ Weaviate is running (version: {response.json().get('version')})")
            return True
    except Exception:
        pass
    print("❌ Cannot connect to Weaviate at http://localhost:8080")
    return False


def make_patched_get_client():
    cfg = resolve_cfg()

    def _patched_get_client(self):
        import weaviate
        from weaviate.auth import AuthCredentials

        def _force_vectorizer_none(c):
            """Monkey patch: forzar vectorizer 'none' en colecciones dinámicas.
            Evita 422 cuando el servidor no tiene módulo text2vec instalado.
            """
            try:
                from weaviate.classes.config import Configure  # v4 client API
                orig_create = c.collections.create

                async def create_with_none(*args, **kwargs):
                    kwargs["vectorizer_config"] = Configure.Vectorizer.none()
                    return await orig_create(*args, **kwargs)

                c.collections.create = create_with_none  # type: ignore
            except Exception:
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
            _force_vectorizer_none(client)
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
            _force_vectorizer_none(client)
            return client

    return _patched_get_client


# -----------------------------
# Helpers Top‑K fuentes
# -----------------------------

def _get_score(o: Dict[str, Any]) -> float | None:
    for k in ("score", "_score", "similarity", "certainty"):
        if isinstance(o.get(k, None), (int, float)):
            return float(o[k])
    if isinstance(o.get("distance", None), (int, float)):
        try:
            return 1.0 / (1e-9 + float(o["distance"]))
        except Exception:
            return 0.0
    return None


def _get_label(o: Dict[str, Any]) -> str:
    for k in ("title", "source", "url", "link", "filename", "name"):
        v = o.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    body = o.get("body")
    if isinstance(body, str) and body.strip():
        return (body.strip()[:80] + "...") if len(body) > 80 else body.strip()
    return "[fuente sin metadatos reconocibles]"


# -----------------------------
# Init Elysia (config + patch + preprocess)
# -----------------------------

def init_elysia(force_preprocess: bool = False):
    load_dotenv(dotenv_path=".elysia_env", override=True)
    os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    if not test_weaviate_connection():
        print("Please ensure Weaviate is running on localhost:8080")
        sys.exit(1)

    from elysia.util.client import ClientManager

    vllm_api_key = os.getenv("VLLM_API_KEY", "sk-local-elysia-noop")
    model_api_base = os.getenv("MODEL_API_BASE", "http://localhost:8000/v1")
    base_model = os.getenv("BASE_MODEL", "Qwen3-8B-AWQ")
    complex_model = os.getenv("COMPLEX_MODEL", base_model)
    wcd_url = os.getenv("WCD_URL", "http://localhost:8080")

    os.environ["OPENAI_API_KEY"] = vllm_api_key
    os.environ["OPENAI_BASE_URL"] = model_api_base
    os.environ["OPENAI_API_BASE"] = model_api_base

    print("\nConfiguring Elysia...")
    if GEMINI_MODEL:
        api_key = os.getenv("GOOGLE_API_KEY", "AIzaSyDdzsIWFVdKy-gWl8pfrFV_f39F2ns4gAI")
        openai_api_base = os.getenv("GOOGLE_API_BASE", "https://generativelanguage.googleapis.com/v1beta/openai/")
        base_model = os.getenv("GOOGLE_BASE_MODEL", "gemini-2.5-flash-lite")
        complex_model = os.getenv("GOOGLE_COMPLEX_MODEL", 'gemini-2.5-flash')

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
        configure(
            base_provider="openai",
            complex_provider="openai",
            base_model=base_model,
            complex_model=complex_model,
            openai_api_key=vllm_api_key,
            openai_api_base=model_api_base,
            weaviate_is_local=True,
            wcd_url=wcd_url,
            local_weaviate_port=int(os.getenv("LOCAL_WEAVIATE_PORT", 8080)),
            local_weaviate_grpc_port=int(os.getenv("LOCAL_WEAVIATE_GRPC_PORT", 50051)),
        )

    print("Patching Weaviate client for anonymous access...")
    patcher = patch.object(ClientManager, "get_client", make_patched_get_client())
    patcher.start()

    print("\nPreprocessing News collection...")
    print("This may take a moment...")
    try:
        preprocess(["News"], max_sample_size=10, force=force_preprocess)
        print("✅ Preprocessing completed successfully!")
    except Exception as e:
        print(f"⚠️ Preprocessing skipped due to error: {e}")

    return patcher


# -----------------------------
# Gradio App
# -----------------------------

def build_demo():
    # Asegura init una sola vez
    if not hasattr(build_demo, "_initialized"):
        build_demo._initialized = True  # type: ignore
        # force_pp = os.getenv("ELYSIA_FORCE_PREPROCESS", "0") in ("1", "true", "True")
        force_pp = True
        build_demo._patcher = init_elysia(force_preprocess=force_pp)  # type: ignore

    tree = Tree()

    ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

    def _strip_ansi(s: str) -> str:
        return ANSI_RE.sub("", s)

    def _parse_boxes(terminal_text: str) -> "OrderedDict[str, str]":
        """Parsea bloques tipo caja (╭─ title ─╮ ... ╰─╯) en un dict ordenado.
        key = título del box, value = contenido (sin bordes), preservando saltos.
        """
        boxes: "OrderedDict[str, str]" = OrderedDict()
        lines = terminal_text.splitlines()
        i = 0
        current_title = None
        current_content: List[str] = []
        while i < len(lines):
            line = _strip_ansi(lines[i])
            # Detect start: line with top border and centered title
            if line.startswith("╭") and line.endswith("╮"):
                # extrae título entre los bordes eliminando relleno de '─'
                m = re.search(r"╭[─\s]*([^╮]+?)[─\s]*╮", line)
                title = (m.group(1).strip() if m else "")
                current_title = title
                current_content = []
                i += 1
                # consume content lines hasta cierre
                while i < len(lines):
                    l2 = _strip_ansi(lines[i])
                    if l2.startswith("╰") and l2.endswith("╯"):
                        # cierre del box
                        boxes[current_title] = "\n".join(current_content).rstrip()
                        current_title = None
                        current_content = []
                        break
                    # líneas internas con bordes laterales
                    if l2.startswith("│") and l2.endswith("│"):
                        inner = l2[1:-1].rstrip()
                        current_content.append(inner.strip())
                    else:
                        # Otras líneas (por si hay variaciones)
                        current_content.append(l2)
                    i += 1
            else:
                i += 1
        return boxes

    def ask(question: str, top_k: int = 8):
        q = (question or "").strip()
        if not q:
            return "Por favor, escribe una pregunta.", "", "(Sin fuentes)"

        # Capturar los "rectángulos" que Elysia imprime en terminal
        from io import StringIO
        from contextlib import redirect_stdout
        buf = StringIO()
        try:
            with redirect_stdout(buf):
                response, objects = tree(q)
            terminal_blocks = buf.getvalue()
            parsed_boxes = _parse_boxes(terminal_blocks)

            # Top‑K fuentes usando el formato solicitado (Title, source, publishedAt, sourceUrl)
            indexed = list(enumerate(objects or []))
            if any(isinstance(obj, dict) and _get_score(obj) is not None for obj in (objects or [])):
                indexed.sort(key=lambda iv: (_get_score(iv[1]) or 0.0), reverse=True)
            top_objs = [obj for _, obj in indexed[: int(top_k)]]
            lines = []
            if objects:
                for obj in objects:
                    if isinstance(obj, dict):
                        lines.append(f"Title: {obj.get('title', 'No disponible')}")
                        lines.append(f"source: {obj.get('source', 'No disponible')}")
                        lines.append(f"publishedAt: {obj.get('publishedAt', 'No disponible')}")
                        lines.append(f"sourceUrl: {obj.get('sourceUrl', 'No disponible')}")
                        lines.append("")
                    else:
                        lines.append(f"Title: {str(obj)[:80]}...")
                        lines.append("")

            print(f'objects: {objects}')

            # Construir panel de trazas sin cajas, como Titulo + contenido
            parsed_text_parts: List[str] = []
            for title, content in parsed_boxes.items():
                parsed_text_parts.append(f"{title}\n{content}\n")
            rects_text = "\n".join(parsed_text_parts).strip()

            # Preferir el último "Assistant response" como respuesta final
            display_answer = str(response)
            if parsed_boxes:
                for title, content in reversed(list(parsed_boxes.items())):
                    if title.strip().lower() == "assistant response":
                        display_answer = content.strip() or display_answer
                        break
            # Orden de salida: respuesta, trazas (formateadas), fuentes, cajas parseadas
            return display_answer, (rects_text or _strip_ansi(terminal_blocks).strip()), ("\n".join(lines).rstrip() if lines else "(Sin fuentes)"), parsed_boxes

        except Exception as e:
            import traceback
            terminal_blocks = buf.getvalue()
            tb = traceback.format_exc()
            raw_err = f"{type(e).__name__}: {e}"

            # Mensaje explicativo para errores comunes
            msg = "⚠️ Ocurrió un error al procesar tu consulta."
            err_text = str(e)
            if "ContextWindowExceededError" in err_text or "maximum context length" in err_text:
                msg += ("\n\nTu petición excede la ventana de contexto del modelo. "
                        "Prueba a acortar la pregunta o a reducir el contenido recuperado.")
            else:
                msg += "\n\nIntenta de nuevo o ajusta tu consulta."

            blocks_md = ("Trazas capturadas (terminal)\n" + _strip_ansi(terminal_blocks).strip() +
                         "\n\nTraceback\n" + tb.strip() +
                         "\n\nError\n" + raw_err)

            # Devolver mensaje de error, trazas, fuentes vacías y sin cajas
            return msg, blocks_md, "(Sin fuentes por error)", {}

    with gr.Blocks(title="Nikola Local Demo 5") as demo:
        gr.Markdown("""
        # 🌳 Nikola Local Demo 5
        Consulta la colección "News" y obtén respuesta + Top-k fuentes.
        """)

        with gr.Row():
            q = gr.Textbox(label="Pregunta", placeholder="¿Qué hay de nuevo en Málaga?", lines=2)
        with gr.Row():
            k = gr.Slider(1, 20, value=8, step=1, label="Top-k fuentes")
        btn = gr.Button("Preguntar", variant="primary")

        with gr.Row():
            ans = gr.Markdown(label="Respuesta")
        with gr.Row():
            rects = gr.Markdown(label="Trazas (User prompt, Assistant response, Current Decision, …)")
        with gr.Row():
            src = gr.Textbox(label="Top-k fuentes más importantes:", lines=10)
        with gr.Row():
            boxes_json = gr.JSON(label="Cajas (parseadas)")
        with gr.Row():
            pp_status = gr.Markdown(label="Estado del preprocesado")
            pp_btn = gr.Button("Reprocesar metadata News", variant="secondary")

        btn.click(ask, inputs=[q, k], outputs=[ans, rects, src, boxes_json])

        def run_preprocess():
            try:
                preprocess(["News"], max_sample_size=10, force=True)
                return "✅ Preprocessing completado"
            except Exception as e:
                return f"⚠️ Error en preprocess: {e}"

        pp_btn.click(run_preprocess, inputs=None, outputs=pp_status)

        gr.Markdown("Nota: Weaviate debe estar activo en localhost:8080.")

    return demo


if __name__ == "__main__":
    demo = build_demo()
    demo.launch(server_name="0.0.0.0", server_port=int(os.getenv("PORT", "8011")), share=True)
