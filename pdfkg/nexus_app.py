"""
Interfaz web Gradio para el sistema Nexus de análisis de datos de turismo.
Proporciona una UI interactiva para consultas y visualización de datos.
"""

import gradio as gr
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Tuple, Optional
import json
from datetime import datetime

if __package__:
    from .nexus_query import NexusQueryEngine
    from .nexus_db import initialize_nexus_db
else:  # pragma: no cover - ejecución directa
    from nexus_query import NexusQueryEngine  # type: ignore
    from nexus_db import initialize_nexus_db  # type: ignore

# Inicializar el motor de consultas
query_engine = None


def initialize_engine():
    """Inicializa el motor de consultas de forma lazy."""
    global query_engine
    if query_engine is None:
        query_engine = NexusQueryEngine()
    return query_engine


def process_question(question: str, history: List[Tuple[str, str]]) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Procesa una pregunta del usuario y actualiza el historial del chat.

    Args:
        question: Pregunta del usuario
        history: Historial de conversación

    Returns:
        Tupla (respuesta_vacia, historial_actualizado)
    """
    if not question.strip():
        return "", history

    try:
        # Inicializar motor si es necesario
        engine = initialize_engine()

        # Procesar pregunta
        result = engine.answer_question(question, save_history=True)

        # Formatear respuesta con metadata
        answer = result['answer']

        if result.get('num_results', 0) > 0:
            answer += f"\n\n---\n📊 {result['num_results']} registros encontrados | "
            answer += f"⏱️ {result['duration_seconds']:.2f}s"

        # Agregar al historial
        history.append((question, answer))

        return "", history

    except Exception as e:
        error_msg = f"❌ Error al procesar la pregunta: {str(e)}"
        history.append((question, error_msg))
        return "", history


def get_database_stats() -> str:
    """
    Obtiene y formatea estadísticas de la base de datos.

    Returns:
        str: Estadísticas formateadas en markdown
    """
    try:
        db = initialize_nexus_db()
        if not db:
            return "❌ Error al conectar con la base de datos"

        stats = db.get_stats()
        db.close()

        # Formatear estadísticas
        output = "# 📊 Estadísticas de la Base de Datos\n\n"
        output += f"**Total de métricas:** {stats.get('total_metricas', 0):,}\n\n"

        anios = stats.get('anios_cubiertos', [])
        if anios:
            output += f"**Años cubiertos:** {', '.join(map(str, anios))}\n\n"

        categorias = stats.get('categorias', [])
        if categorias:
            output += f"## Categorías ({len(categorias)})\n\n"
            output += "| Categoría | Métricas |\n"
            output += "|-----------|----------|\n"

            for cat_info in sorted(categorias, key=lambda x: x.get('count', 0), reverse=True)[:15]:
                categoria = cat_info.get('categoria', 'N/A')
                count = cat_info.get('count', 0)
                output += f"| {categoria} | {count:,} |\n"

        return output

    except Exception as e:
        return f"❌ Error al obtener estadísticas: {str(e)}"


def export_to_csv(history: List[Tuple[str, str]]) -> Optional[str]:
    """
    Exporta el historial de conversación a un archivo CSV.

    Args:
        history: Historial de conversación

    Returns:
        str: Ruta al archivo CSV generado
    """
    if not history:
        return None

    try:
        # Crear DataFrame
        df = pd.DataFrame(history, columns=["Pregunta", "Respuesta"])
        df["Timestamp"] = datetime.now().isoformat()

        # Guardar CSV
        filename = f"nexus_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')

        return filename

    except Exception as e:
        print(f"Error al exportar: {e}")
        return None


def create_category_chart() -> go.Figure:
    """
    Crea un gráfico de distribución de métricas por categoría.

    Returns:
        Figura de Plotly
    """
    try:
        db = initialize_nexus_db()
        if not db:
            return go.Figure()

        stats = db.get_stats()
        db.close()

        categorias = stats.get('categorias', [])
        if not categorias:
            return go.Figure()

        # Preparar datos
        cats = [c.get('categoria', 'N/A') for c in categorias[:15]]
        counts = [c.get('count', 0) for c in categorias[:15]]

        # Crear gráfico de barras
        fig = go.Figure(data=[
            go.Bar(
                x=cats,
                y=counts,
                marker_color='lightblue',
                text=counts,
                textposition='auto',
            )
        ])

        fig.update_layout(
            title="Distribución de Métricas por Categoría",
            xaxis_title="Categoría",
            yaxis_title="Número de Métricas",
            xaxis={'tickangle': -45},
            height=500
        )

        return fig

    except Exception as e:
        print(f"Error al crear gráfico: {e}")
        return go.Figure()


def clear_history() -> List:
    """Limpia el historial de conversación."""
    return []


# Ejemplos de preguntas
EXAMPLE_QUESTIONS = [
    "¿Cuántos turistas británicos hubo en enero 2025?",
    "¿Cómo varió el turismo de cruceros entre 2024 y 2025?",
    "¿Qué provincia tuvo más turistas: Málaga o Sevilla?",
    "¿Cuál fue el gasto medio diario en el primer trimestre de 2024?",
    "¿Cuántas pernoctaciones hubo en Granada en 2024?",
]


def create_gradio_interface():
    """Crea y configura la interfaz de Gradio."""

    with gr.Blocks(
        title="Nexus - Análisis de Turismo en Andalucía",
        theme=gr.themes.Soft()
    ) as app:

        gr.Markdown("""
        # 🏖️ Nexus - Sistema de Análisis de Datos de Turismo en Andalucía

        Haz preguntas en lenguaje natural sobre estadísticas de turismo y obtén respuestas
        precisas basadas en datos reales.
        """)

        with gr.Tabs():
            # Tab 1: Chat
            with gr.TabItem("💬 Consultas"):
                with gr.Row():
                    with gr.Column(scale=2):
                        chatbot = gr.Chatbot(
                            label="Conversación",
                            height=500,
                            show_label=True
                        )

                        with gr.Row():
                            question_input = gr.Textbox(
                                placeholder="Escribe tu pregunta aquí...",
                                label="Pregunta",
                                lines=2,
                                scale=4
                            )
                            submit_btn = gr.Button("Enviar", variant="primary", scale=1)

                        with gr.Row():
                            clear_btn = gr.Button("Limpiar historial", size="sm")
                            export_btn = gr.Button("Exportar a CSV", size="sm")

                        export_output = gr.File(label="Archivo exportado", visible=False)

                    with gr.Column(scale=1):
                        gr.Markdown("### 📝 Ejemplos de preguntas")
                        examples = gr.Examples(
                            examples=EXAMPLE_QUESTIONS,
                            inputs=question_input,
                            label=None
                        )

                        gr.Markdown("""
                        ### 💡 Consejos
                        - Sé específico con fechas y categorías
                        - Puedes preguntar por comparaciones
                        - Menciona provincias específicas
                        - Pregunta por métricas como:
                          - Número de turistas
                          - Pernoctaciones
                          - Gasto medio diario
                          - Variación interanual
                        """)

            # Tab 2: Estadísticas
            with gr.TabItem("📊 Estadísticas"):
                with gr.Row():
                    refresh_stats_btn = gr.Button("Actualizar estadísticas", variant="primary")

                stats_output = gr.Markdown(value=get_database_stats())

                gr.Markdown("### 📈 Visualización de Datos")
                chart_output = gr.Plot(value=create_category_chart())

            # Tab 3: Ayuda
            with gr.TabItem("❓ Ayuda"):
                gr.Markdown("""
                ## Guía de Uso

                ### Tipos de Preguntas Soportadas

                #### 1. Queries Numéricas Simples
                - "¿Cuántos turistas británicos hubo en enero 2025?"
                - "¿Cuál fue el gasto medio diario en marzo 2024?"

                #### 2. Comparaciones Temporales
                - "¿Hubo más turismo en 2023 respecto a 2024?"
                - "¿Cómo varió el turismo de cruceros entre Q1 2024 y Q1 2025?"

                #### 3. Agregaciones
                - "¿Cuántos turistas ingresaron por crucero en el primer trimestre de 2024?"
                - "¿Cuál fue el total de pernoctaciones en Málaga durante el verano 2024?"

                #### 4. Comparaciones entre Categorías
                - "¿Qué provincia tuvo más turistas en 2024: Málaga o Sevilla?"
                - "¿Hubo más turistas británicos o alemanes en diciembre 2023?"

                ### Categorías Disponibles

                **Origen de turistas:**
                - Total turistas
                - Españoles, Andaluces, Resto de España
                - Extranjeros, Británicos, Alemanes, Otros mercados

                **Tipos de turismo:**
                - Litoral, Interior
                - Cruceros, Ciudad, Cultural

                **Provincias:**
                - Almería, Cádiz, Córdoba, Granada
                - Huelva, Jaén, Málaga, Sevilla

                ### Métricas Disponibles

                - Número de viajeros en establecimientos hoteleros
                - Número de pernoctaciones
                - Cuota sobre total España (%)
                - Llegadas de pasajeros a aeropuertos
                - Número de turistas (millones)
                - Estancia media (días)
                - Gasto medio diario (euros)
                - Variación interanual (%)

                ### Datos Disponibles

                - **Periodo:** Enero 2023 - Mayo 2025
                - **Frecuencia:** Mensual
                - **Fuente:** Oficina del Dato - Turismo y Deporte de Andalucía

                ### Soporte Técnico

                Si encuentras problemas o tienes sugerencias, por favor contacta al administrador del sistema.
                """)

        # Event handlers
        submit_btn.click(
            fn=process_question,
            inputs=[question_input, chatbot],
            outputs=[question_input, chatbot]
        )

        question_input.submit(
            fn=process_question,
            inputs=[question_input, chatbot],
            outputs=[question_input, chatbot]
        )

        clear_btn.click(
            fn=clear_history,
            outputs=chatbot
        )

        export_btn.click(
            fn=export_to_csv,
            inputs=chatbot,
            outputs=export_output
        ).then(
            lambda: gr.update(visible=True),
            outputs=export_output
        )

        refresh_stats_btn.click(
            fn=get_database_stats,
            outputs=stats_output
        ).then(
            fn=create_category_chart,
            outputs=chart_output
        )

    return app


def launch_app(share: bool = False, port: int = 7860):
    """
    Lanza la aplicación Gradio.

    Args:
        share: Si True, crea un enlace público
        port: Puerto para el servidor
    """
    app = create_gradio_interface()

    print("\n" + "=" * 80)
    print("🚀 LANZANDO NEXUS WEB APP")
    print("=" * 80)
    print(f"\n📍 URL local: http://localhost:{port}")

    if share:
        print("🌐 Se generará una URL pública para compartir...")

    print("\n💡 Presiona Ctrl+C para detener el servidor\n")
    print("=" * 80 + "\n")

    app.launch(
        share=share,
        server_port=port,
        server_name="0.0.0.0"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Nexus Web App")
    parser.add_argument("--share", action="store_true",
                       help="Crear enlace público compartible")
    parser.add_argument("--port", type=int, default=7860,
                       help="Puerto del servidor (default: 7860)")

    args = parser.parse_args()

    launch_app(share=args.share, port=args.port)
