"""
Catálogo de ejes por topic para evaluación radar.
"""

AXES_BY_TOPIC = {
    "vivienda": [
        "Precio", "Disponibilidad", "Oferta pública",
        "Nuevas construcciones", "Costo a crédito"
    ],
    "economia": [
        "Inflación", "Empleo", "Crecimiento del PIB",
        "Salario real", "Inversión"
    ],
    "sanidad": [
        "Listas de espera", "Atención primaria",
        "Cobertura", "Personal sanitario",
        "Infraestructura hospitalaria"
    ],
    "seguridad": [
        "Delitos", "Respuesta policial",
        "Percepción de seguridad", "Recursos y equipamiento",
        "Eficacia judicial"
    ],
    "educacion": [
        "Resultados académicos", "Infraestructura",
        "Docentes", "Acceso y becas", "Digitalización"
    ],
    "transporte": [
        "Oferta y frecuencia", "Infraestructura vial",
        "Puntualidad y confiabilidad", "Accesibilidad",
        "Seguridad vial"
    ],
}


def get_axes_for_topic(topic: str) -> list:
    """
    Get axes list for a given topic.

    Args:
        topic: Topic key (e.g., "vivienda", "economia")

    Returns:
        List of axis names, or empty list if topic not found
    """
    return AXES_BY_TOPIC.get(topic, [])


def has_axes(topic: str) -> bool:
    """Check if a topic has defined axes."""
    return topic in AXES_BY_TOPIC
