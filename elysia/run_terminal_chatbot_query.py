#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ejecuta el chatbot de terminal con una consulta fija y muestra la respuesta."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Asegura que el paquete raíz esté en el path cuando se ejecuta el script directamente.
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from elysia1.terminal_chatbot import ConversationMemory, build_agent_executor


DEFAULT_QUERY = "Dime las últimas noticias que incluyan temas de conciertos y festivales de música."


def parse_args() -> argparse.Namespace:
    """Parámetros CLI para elegir proveedor y consulta."""
    parser = argparse.ArgumentParser(
        description="Replica una ejecución de terminal_chatbot.py con una consulta predefinida."
    )
    parser.add_argument(
        "--provider",
        choices=["gemini", "vllm"],
        default=os.getenv("DEFAULT_PROVIDER", "vllm"),
        help="Proveedor de modelo a usar para el chatbot.",
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help="Consulta que se enviará al chatbot.",
    )
    return parser.parse_args()


def run_query(provider: str, query: str) -> str:
    """Ejecuta internamente el flujo del chatbot para la consulta dada."""
    agent_executor = build_agent_executor(provider)
    memory = ConversationMemory()

    response = agent_executor.invoke({"input": query, "chat_history": memory.as_messages()})
    ai_text = str(response.get("output", ""))

    # Mantiene la lógica de terminal_chatbot para ocultar el razonamiento interno.
    end_tag = "</think>"
    if end_tag in ai_text:
        ai_text = ai_text.split(end_tag)[-1].strip()

    return ai_text


def main() -> None:
    args = parse_args()
    answer = run_query(args.provider, args.query)
    print(answer)


if __name__ == "__main__":
    main()
