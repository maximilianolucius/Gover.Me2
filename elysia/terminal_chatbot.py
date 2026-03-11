#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chatbot de terminal con memoria de conversación que puede usar Gemini o un endpoint vLLM
compatible con OpenAI. Integra una herramienta de búsqueda DuckDuckGo para datos recientes.
Requiere instalar langchain-community y langchain-openai.

Uso:
    python terminal_chatbot.py --provider gemini
    python terminal_chatbot.py --provider vllm
"""

from dotenv import load_dotenv

# Load environment variables
load_dotenv('elysia/.elysia_env')
load_dotenv('.env')
load_dotenv()

import argparse
import os
import readline  # noqa: F401  (habilita historial y edición en terminal)
from typing import List, Tuple

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.language_models import BaseChatModel

from langchain_openai import ChatOpenAI

try:  # Gemini support (langchain-google-genai)
    from langchain_google_genai import ChatGoogleGenerativeAI
except ImportError:  # pragma: no cover - optional dependency
    ChatGoogleGenerativeAI = None  # type: ignore

from elysia1.nexus_tool import create_nexus_tool
from elysia1.deepsearch_tool import create_deepsearch_tool


# ---------------------------------------------------------------------------
# Helpers para construir el modelo según proveedor seleccionado
# ---------------------------------------------------------------------------

def build_llm(provider: str) -> BaseChatModel:
    """Configura el modelo en función del proveedor elegido."""
    provider = provider.lower()
    if provider == "gemini":
        if ChatGoogleGenerativeAI is None:
            raise RuntimeError(
                "langchain-google-genai no está instalado. Ejecuta `pip install langchain-google-genai`."
            )

        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError("GOOGLE_API_KEY no está definido en el entorno.")

        model = os.getenv("GOOGLE_BASE_MODEL", "gemini-2.0-flash")
        temperature = float(os.getenv("GEMINI_TEMPERATURE", "0.3"))

        return ChatGoogleGenerativeAI(
            model=model,
            google_api_key=api_key,
            temperature=temperature,
        )

    if provider == "vllm":
        api_key = os.getenv("VLLM_API_KEY", "sk-local-noop")
        api_base = os.getenv("MODEL_API_BASE", "http://localhost:8000/v1")
        model = os.getenv("BASE_MODEL", "Qwen3-8B-AWQ")
        return ChatOpenAI(
            model=model,
            api_key=api_key,
            base_url=api_base,
            temperature=float(os.getenv("VLLM_TEMPERATURE", "0.3")),
        )

    raise ValueError("Proveedor no soportado. Usa 'gemini' o 'vllm'.")


# ---------------------------------------------------------------------------
# Agente ReAct con herramienta de búsqueda
# ---------------------------------------------------------------------------

def build_agent_executor(provider: str) -> AgentExecutor:
    """Crea un agente con DeepSearch, DuckDuckGo y Nexus como herramientas auxiliares."""
    llm = build_llm(provider)

    # Herramienta 1: Nexus para datos turísticos oficiales de Andalucía
    nexus_tool = create_nexus_tool()

    # Herramienta 2: DeepSearch para noticias y análisis periodístico con citaciones
    deepsearch_tool = create_deepsearch_tool()

    # Herramienta 3: DuckDuckGo para búsquedas web generales y rápidas
    duckduckgo_tool = DuckDuckGoSearchRun(
        name="duckduckgo_search",
        description=(
            "Realiza búsquedas web en DuckDuckGo y devuelve resúmenes breves. "
            "Úsalo para obtener información general de internet, noticias globales, "
            "o cuando ninguna otra herramienta tenga información suficiente.")
    )

    tools = [nexus_tool, deepsearch_tool, duckduckgo_tool]

    # Bind tools to LLM for native function calling
    llm_with_tools = llm.bind_tools(tools)

    agent_prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ]
    )

    agent = create_tool_calling_agent(llm_with_tools, tools, agent_prompt)

    verbose = os.getenv("CHATBOT_VERBOSE", "1").lower() in {"1", "true", "yes"}
    return AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=verbose,
        handle_parsing_errors=True,
        max_iterations=15,
        max_execution_time=120,
        return_intermediate_steps=True,
    )


# ---------------------------------------------------------------------------
# RAG básico: integrador de herramientas para noticias de Málaga/España, apoyado en
# DeepSearch (búsqueda iterativa), DuckDuckGo y datos turísticos locales. Aquí solo
# gestionamos la conversación.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Eres Octavio, un asistente periodístico especializado en noticias de Málaga y España.

Instrucciones CRÍTICAS:
- Responde SIEMPRE en español.
- Mantén un tono claro y profesional.
- NO REVELES que eres un modelo de lenguaje entrenado.
- Usa la historia previa de la conversación para mantener coherencia.

USO OBLIGATORIO DE HERRAMIENTAS:

1. **deepsearch_news_oracle** (PRIORIDAD MÁXIMA - USA SIEMPRE PARA NOTICIAS):
   - Úsala para CUALQUIER pregunta que incluya la palabra "noticias", "últimas", "actualidad", "eventos", "sucesos".
   - Úsala para investigar artículos periodísticos, reportajes e información contextual recientes en la web abierta.
   - Úsala para noticias sobre turismo, cultura, deportes, política, economía, sucesos, etc.
   - Esta herramienta realiza múltiples búsquedas y devuelve resúmenes con fuentes verificables; cítalas SIEMPRE al final.
   - EJEMPLOS de cuándo usar:
     * "Noticias sobre turismo" → USA deepsearch_news_oracle
     * "Últimas noticias de Málaga" → USA deepsearch_news_oracle
     * "Eventos en Sevilla" → USA deepsearch_news_oracle
     * "Qué pasó en Andalucía" → USA deepsearch_news_oracle

2. **nexus_tourism_oracle** (SOLO PARA ESTADÍSTICAS OFICIALES):
   - Úsala ÚNICAMENTE cuando pidan CIFRAS, NÚMEROS, ESTADÍSTICAS, COMPARACIONES de turismo oficial.
   - Proporciona métricas concretas (número de turistas, gasto medio, variaciones porcentuales).
   - La herramineta tiene datos haste el 2025 inclusive!
   - EJEMPLOS de cuándo usar:
     * "Cuántos turistas visitaron Málaga en 2024" → USA nexus_tourism_oracle
     * "Comparativa de turismo 2023 vs 2024" → USA nexus_tourism_oracle
     * "Estadísticas de pernoctaciones en Andalucía" → USA nexus_tourism_oracle
   - NO uses esta herramienta para noticias generales sobre turismo.

3. **duckduckgo_search** (COMPLEMENTARIA):
   - Úsala cuando necesites una consulta rápida o resultados adicionales de la web.
   - Úsala para clima en tiempo real, hora actual, noticias internacionales muy recientes.
   - También puede servir como refuerzo si deepsearch_news_oracle no ofrece resultados suficientes.

REGLA DE ORO:
- Si la pregunta menciona "noticias", "última hora", "actualidad", "eventos" → USA SIEMPRE deepsearch_news_oracle PRIMERO
- Si la pregunta pide "cifras", "cuántos", "estadísticas", "comparativa" → USA nexus_tourism_oracle
- NUNCA inventes información. Si ninguna herramienta da resultados, admítelo.
- Solo responde sin herramientas para saludos y conversación básica.

FORMATO DE RESPUESTA:
- Respuestas directas y amigables basadas en los resultados.
- Incluye las fuentes obtenidas (de deepsearch_news_oracle y/o duckduckgo_search) al final, en formato lista.
- Si no hay información suficiente, explícalo brevemente.
"""


# ---------------------------------------------------------------------------
# Memoria de conversación en memoria RAM
# ---------------------------------------------------------------------------

class ConversationMemory:
    """Simple envoltorio de InMemoryChatMessageHistory con helpers."""

    def __init__(self) -> None:
        self._history = InMemoryChatMessageHistory()

    @property
    def history(self) -> InMemoryChatMessageHistory:
        return self._history

    def as_messages(self) -> List[BaseMessage]:
        return self._history.messages

    def append(self, message: BaseMessage) -> None:
        self._history.add_message(message)

    def append_pair(self, human: str, ai: str) -> None:
        self.append(HumanMessage(content=human))
        self.append(AIMessage(content=ai))


# ---------------------------------------------------------------------------
# Bucle principal
# ---------------------------------------------------------------------------

def interactive_chat(provider: str) -> None:
    agent_executor = build_agent_executor(provider)
    memory = ConversationMemory()

    print("Chatbot periodístico listo. Escribe 'salir' para terminar.\n")

    while True:
        try:
            user_input = input("Usuario> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nAdiós.")
            break

        if user_input.lower() in {"salir", "exit", "quit"}:
            print("Hasta la próxima.")
            break

        if not user_input:
            continue

        response = agent_executor.invoke(
            {"input": user_input, "chat_history": memory.as_messages()}
        )

        ai_text = str(response.get("output", ""))
        # Manten estas lineas de codigo. Es para remover el razonamiento.
        end_tag = '</think>'
        if end_tag in ai_text:
            ai_text = ai_text.split(end_tag)[-1].strip()
        # ---------------------------------------------------------------

        print(f"Asistente> {ai_text}\n")

        memory.append_pair(user_input, ai_text)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def launch_gradio_chat(provider: str) -> None:
    """Inicia una interfaz web con Gradio compartida públicamente."""
    import gradio as gr

    agent_executor = build_agent_executor(provider)

    def respond(
        user_message: str,
        history: List[Tuple[str, str]],
        memory_state: List[Tuple[str, str]],
    ) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        memory_state = memory_state or []

        if not user_message.strip():
            return history, memory_state

        conversation = ConversationMemory()
        for human_msg, ai_msg in memory_state:
            conversation.append_pair(human_msg, ai_msg)

        response = agent_executor.invoke(
            {"input": user_message, "chat_history": conversation.as_messages()}
        )

        ai_text = str(response.get("output", ""))
        end_tag = "</think>"
        if end_tag in ai_text:
            ai_text = ai_text.split(end_tag)[-1].strip()

        conversation.append_pair(user_message, ai_text)
        updated_history = history + [(user_message, ai_text)]
        updated_state = memory_state + [(user_message, ai_text)]
        return updated_history, updated_state

    def reset_chat() -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        return [], []

    with gr.Blocks(title="Octavio - Asistente periodístico") as demo:
        gr.Markdown(
            """# Octavio
Asistente periodístico especializado en noticias de Málaga y España."""
        )
        chatbot = gr.Chatbot()
        state = gr.State([])
        msg = gr.Textbox(label="Escribe tu pregunta", placeholder="¿Qué noticias hay hoy en Málaga?", lines=3)
        submit = gr.Button("Enviar")
        clear = gr.Button("Reiniciar conversación")

        msg.submit(respond, [msg, chatbot, state], [chatbot, state])
        submit.click(respond, [msg, chatbot, state], [chatbot, state])
        msg.submit(lambda: "", None, msg)
        submit.click(lambda: "", None, msg)
        clear.click(reset_chat, None, [chatbot, state])

    demo.launch(share=True)


def parse_args() -> Tuple[str, str]:
    parser = argparse.ArgumentParser(description="Chatbot de terminal con LangChain.")
    parser.add_argument(
        "--provider",
        choices=["gemini", "vllm"],
        default=os.getenv("DEFAULT_PROVIDER", "vllm"),
        help="Proveedor de modelo a usar.",
    )
    parser.add_argument(
        "--mode",
        choices=["terminal", "gradio"],
        default=os.getenv("CHATBOT_MODE", "terminal"),
        help="Modo de ejecución: 'terminal' (CLI) o 'gradio' (web).",
    )
    args = parser.parse_args()
    return args.provider, args.mode


def main() -> None:
    provider, mode = parse_args()
    try:
        if mode == "gradio":
            launch_gradio_chat(provider)
        else:
            interactive_chat(provider)
    except Exception as exc:
        print(f"[Error] {exc}")


if __name__ == "__main__":

    print(f'GEMINI_API_KEY: {os.getenv("GEMINI_API_KEY")}')
    print(f'GOOGLE_API_KEY: {os.getenv("GOOGLE_API_KEY")}')

    main()
