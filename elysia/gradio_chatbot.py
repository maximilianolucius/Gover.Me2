#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interfaz web con Gradio para el chatbot periodístico Octavio.
Expone el chatbot con historial conversacional y acceso público.
"""

import os
import argparse
from typing import List, Tuple
from datetime import datetime

import gradio as gr

# Importar componentes del chatbot terminal
from terminal_chatbot import (
    build_agent_executor,
    ConversationMemory,
)


# ---------------------------------------------------------------------------
# Configuración global
# ---------------------------------------------------------------------------

# Usar Gemini por defecto (no se menciona en la UI)
DEFAULT_PROVIDER = os.getenv("DEFAULT_PROVIDER", "gemini")

# Estado global del chatbot
agent_executor = None
memory = None


def initialize_chatbot(provider: str = DEFAULT_PROVIDER):
    """Inicializa el agente y la memoria conversacional."""
    global agent_executor, memory

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Inicializando chatbot...")
    agent_executor = build_agent_executor(provider)
    memory = ConversationMemory()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Chatbot listo!")


# ---------------------------------------------------------------------------
# Función principal de chat para Gradio
# ---------------------------------------------------------------------------

def chat_function(message: str, history: List[List[str]]) -> str:
    """
    Procesa un mensaje del usuario y devuelve la respuesta del asistente.

    Args:
        message: Mensaje actual del usuario
        history: Historial de conversación en formato Gradio [[user_msg, bot_msg], ...]

    Returns:
        Respuesta del asistente
    """
    if not message or not message.strip():
        return "Por favor, escribe un mensaje."

    try:
        # Ejecutar agente
        response = agent_executor.invoke(
            {"input": message, "chat_history": memory.as_messages()}
        )

        ai_text = str(response.get("output", ""))

        # Remover etiquetas de razonamiento si existen
        end_tag = '</think>'
        if end_tag in ai_text:
            ai_text = ai_text.split(end_tag)[-1].strip()

        # Guardar en memoria
        memory.append_pair(message, ai_text)

        return ai_text

    except Exception as e:
        error_msg = f"Lo siento, ocurrió un error al procesar tu mensaje: {str(e)}"
        print(f"[ERROR] {error_msg}")
        return error_msg


# ---------------------------------------------------------------------------
# Interfaz Gradio
# ---------------------------------------------------------------------------

def create_gradio_interface():
    """Crea y configura la interfaz Gradio."""

    # CSS personalizado para mejorar la apariencia
    custom_css = """
    .gradio-container {
        font-family: 'Arial', sans-serif;
    }
    .chat-message {
        padding: 10px;
        margin: 5px 0;
    }
    footer {
        display: none !important;
    }
    """

    # Tema personalizado
    theme = gr.themes.Soft(
        primary_hue="blue",
        secondary_hue="slate",
    )

    # Crear interfaz de chat
    with gr.Blocks(css=custom_css, theme=theme, title="Octavio - Asistente Periodístico") as demo:

        gr.Markdown(
            """
            # 📰 Octavio - Asistente Periodístico

            Tu asistente especializado en noticias de Málaga y España.
            Pregúntame sobre eventos locales, actualidad regional, o cualquier tema periodístico.
            """
        )

        chatbot = gr.Chatbot(
            label="Conversación",
            height=500,
            bubble_full_width=False,
            avatar_images=(
                None,  # Usuario sin avatar
                None,  # Asistente sin avatar
            ),
            show_copy_button=True,
        )

        with gr.Row():
            msg = gr.Textbox(
                label="Tu mensaje",
                placeholder="Escribe tu pregunta aquí...",
                scale=4,
                lines=2,
                max_lines=5,
            )
            submit_btn = gr.Button("Enviar", variant="primary", scale=1)

        with gr.Row():
            clear_btn = gr.Button("🗑️ Limpiar conversación", variant="secondary")

        gr.Markdown(
            """
            ---
            💡 **Ejemplos de preguntas:**
            - ¿Cuáles son las últimas noticias de Málaga?
            - ¿Qué eventos importantes hay en Andalucía?
            - Dame información sobre incendios recientes en España
            - ¿Cuál es la temperatura actual en Málaga?
            """
        )

        # Función para procesar mensaje y actualizar historial
        def respond(message, chat_history):
            if not message.strip():
                return "", chat_history

            # Obtener respuesta del bot
            bot_message = chat_function(message, chat_history)

            # Agregar al historial
            chat_history.append([message, bot_message])

            return "", chat_history

        # Función para limpiar conversación
        def clear_conversation():
            global memory
            memory = ConversationMemory()
            return None

        # Conectar eventos
        msg.submit(respond, [msg, chatbot], [msg, chatbot])
        submit_btn.click(respond, [msg, chatbot], [msg, chatbot])
        clear_btn.click(clear_conversation, None, chatbot)

        # Mensaje de bienvenida al cargar
        demo.load(
            lambda: [[None, "¡Hola! Soy Octavio, tu asistente periodístico. ¿En qué puedo ayudarte hoy?"]],
            None,
            chatbot
        )

    return demo


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> Tuple[str, str, int, bool]:
    """Parsear argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Interfaz web Gradio para el chatbot periodístico Octavio"
    )
    parser.add_argument(
        "--provider",
        choices=["gemini", "vllm"],
        default=DEFAULT_PROVIDER,
        help="Proveedor de modelo LLM (default: gemini)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host para el servidor Gradio (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=7860,
        help="Puerto para el servidor Gradio (default: 7860)",
    )
    parser.add_argument(
        "--no-share",
        action="store_true",
        help="Desactivar share=True (por defecto está activo)",
    )

    args = parser.parse_args()
    return args.provider, args.host, args.port, not args.no_share


def main():
    """Función principal."""
    provider, host, port, share = parse_args()

    print("=" * 60)
    print("🚀 Iniciando Octavio - Asistente Periodístico")
    print("=" * 60)

    # Inicializar chatbot
    initialize_chatbot(provider)

    # Crear interfaz
    demo = create_gradio_interface()

    # Información de inicio
    print(f"\n📡 Servidor Gradio iniciando...")
    print(f"   Host: {host}")
    print(f"   Puerto: {port}")
    print(f"   Share: {'Sí (acceso público)' if share else 'No (solo local)'}")

    if share:
        print(f"\n⚠️  IMPORTANTE: Se generará un enlace público temporal")
        print(f"   Este enlace será válido por 72 horas")

    print("\n" + "=" * 60)

    # Lanzar servidor
    try:
        demo.launch(
            server_name=host,
            server_port=port,
            share=share,
            show_error=True,
            favicon_path=None,
        )
    except KeyboardInterrupt:
        print("\n\n⏸️  Servidor detenido por el usuario")
    except Exception as e:
        print(f"\n❌ Error al iniciar servidor: {e}")
        raise


if __name__ == "__main__":
    main()
