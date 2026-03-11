#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Servicio web (Gradio) para consultar PostgreSQL vía qa_agent_pg_vllm_3.py (vLLM OpenAI‑compatible).

Provee una interfaz con:
- Pregunta en lenguaje natural
- Opción de resumen con LLM
- Perfil de LLM (primary/alt), por defecto el principal
- Salida: SQL generada, resumen, resultados tabulares y tiempos

Uso:
  python nexus/qa_agent_pg_vllm_3_gradio.py

Variables útiles:
- NEXUS_DSN, VLLM_BASE_URL, VLLM_MODEL, VLLM_PROFILE, etc. (ver qa_agent_pg_vllm_3.py)
- PORT (puerto del servidor, default 8004)
"""

import os
import warnings
import gradio as gr
import pandas as pd

try:
    from transformers.utils import logging as hf_logging
    hf_logging.set_verbosity_error()
except Exception:
    pass
warnings.filterwarnings("ignore")

from nexus.qa_agent_pg_vllm_3 import ask as qa_ask


def _fmt_timings(t: dict) -> str:
    if not t:
        return ""
    order = ["t_llm_sql_s", "t_db_s", "t_summary_s", "t_total_s", "llm_profile", "retries"]
    parts = []
    for k in order:
        v = t.get(k)
        if v is None:
            continue
        if isinstance(v, (int, float)):
            parts.append(f"{k}={v:.2f}s")
        else:
            parts.append(f"{k}={v}")
    # Añadir eventos de prevalidación/ejecución si existen
    ev = t.get("events")
    if ev:
        parts.append("events=" + " || ".join(str(e) for e in ev))
    return " | ".join(parts)


def run_query(question: str, summarize: bool, profile: str):
    if not question or not question.strip():
        return "", "", pd.DataFrame(), ""

    df, sql, summary, timings = qa_ask(
        question.strip(),
        summarize=bool(summarize),
        return_timings=True,
        llm_profile=(profile or None),
    )

    sql_md = f"```sql\n{sql}\n```" if sql else ""
    timings_str = _fmt_timings(timings)
    return sql_md, (summary or ""), df, timings_str


def build_app():
    default_profile = os.getenv("VLLM_PROFILE", "primary").strip().lower()

    with gr.Blocks(title="QA SQL (vLLM + PostgreSQL)") as demo:
        gr.Markdown("""
        # 🧠 QA sobre PostgreSQL (vLLM)
        Escribe una pregunta en lenguaje natural. El agente generará una consulta SQL segura, la ejecutará y mostrará el resultado.
        """
        )

        with gr.Row():
            q = gr.Textbox(label="Pregunta", placeholder="Ej.: Top 5 países por viajeros en 2025", lines=2)
        with gr.Row():
            summarize = gr.Checkbox(value=True, label="Generar resumen")
            profile = gr.Dropdown(
                choices=["primary", "alt"],
                value=(default_profile if default_profile in ("primary", "alt") else "primary"),
                label="Perfil LLM"
            )
        run_btn = gr.Button("Consultar", variant="primary")

        with gr.Row():
            sql_out = gr.Markdown(label="SQL generada")
        with gr.Row():
            summary_out = gr.Markdown(label="Resumen")
        with gr.Row():
            table_out = gr.Dataframe(label="Resultados", interactive=False)
        with gr.Row():
            timings_out = gr.Textbox(label="Tiempos", interactive=False)

        run_btn.click(
            fn=run_query,
            inputs=[q, summarize, profile],
            outputs=[sql_out, summary_out, table_out, timings_out]
        )

    return demo


if __name__ == "__main__":
    app = build_app()
    # Puerto por defecto 8004; override con $PORT. share=True genera enlace público temporal.
    app.launch(server_name="0.0.0.0", server_port=int(os.getenv("PORT", "8004")), share=True)
