"""
OpenAI-compatible vLLM client with caching and retry logic.
"""
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import requests

logger = logging.getLogger(__name__)


class NLPClient:
    """Client for vLLM OpenAI-compatible API with caching."""

    CLASSIFICATION_SYSTEM = "Eres un analista de prensa en español. Responde SOLO JSON válido y minificado."

    CLASSIFICATION_TEMPLATE = """Texto (máx 2500 palabras):
{title}
{body}

Tareas:
1) primary_topic en {{"vivienda","educacion","sanidad","economia","seguridad","transporte"}}.
2) subtopics: lista corta de 1–4 términos.
3) sentiment_label en {{"negative","neutral","positive"}} y sentiment_score en [-1,1].
4) stance_by_party: objeto con claves {{"pp","psoe","vox","bng","sumar","podemos"}} y valores numéricos en [-1,1] (negativo=contra, positivo=a favor).
5) argument_affinity_index en [0,100], coherente con 4).

Formato de salida EXCLUSIVO:
{{"primary_topic":"...","subtopics":[...],"sentiment_label":"...","sentiment_score":0.0,"stance_by_party":{{"pp":0.0,"psoe":0.0,"vox":0.0,"bng":0.0,"sumar":0.0,"podemos":0.0}},"argument_affinity_index":0.0}}"""

    SUMMARY_SYSTEM = "Eres un resumidor conciso en español. Responde SOLO JSON válido y minificado."

    SUMMARY_TEMPLATE = """Título: {title}
Cuerpo: {body}

Devuelve:
{{"summary_abstractive":"2–4 frases concisas","bullets_extractive":["• punto 1","• punto 2"],"keywords":["kw1","kw2","kw3"]}}"""

    ENTITIES_SYSTEM = "Eres un extractor de entidades en español. Responde SOLO JSON válido y minificado."

    ENTITIES_TEMPLATE = """Título: {title}
Cuerpo: {body}

Extrae del texto:
1) persons: lista de nombres de personas mencionadas (ej: ["Juan Pérez", "María García"])
2) orgs: lista de organizaciones/empresas (ej: ["Renfe", "PSOE", "Ayuntamiento"])
3) locations: lista de lugares/ciudades (ej: ["Madrid", "Barcelona"])
4) parties_present: lista de partidos políticos mencionados, usando claves: ["pp", "psoe", "vox", "bng", "sumar", "podemos"] (solo los que aparezcan)

Devuelve SOLO JSON:
{{"persons":[],"orgs":[],"locations":[],"parties_present":[]}}"""

    RADAR_SYSTEM = "Eres un evaluador político de noticias en español. Responde SOLO JSON válido y minificado."

    RADAR_TEMPLATE = """Título: {title}
Cuerpo: {body}

Este artículo trata sobre el tema: {topic}

Ejes a evaluar (5 dimensiones):
{axes_list}

Evalúa cada eje con un score de 0 a 100 desde CUATRO perspectivas:

1) **pp**: Postura del PP sobre este eje según el artículo (0-100 o null si no se menciona)
2) **vox**: Postura de VOX sobre este eje según el artículo (0-100 o null si no se menciona)
3) **psoe**: Postura del PSOE sobre este eje según el artículo (0-100 o null si no se menciona)
4) **programa**: Estado actual/situación descrita en el artículo para este eje (0-100 o null si no se menciona)

Escala de valoración:
- 0: Muy negativo/deficiente/crítico
- 50: Neutral/equilibrado/sin cambios
- 100: Muy positivo/excelente/mejora significativa

IMPORTANTE:
- Si un partido NO es mencionado en el artículo, usa [null, null, null, null, null]
- Si un eje NO se menciona para un partido o en el programa, usa null en esa posición
- El orden de los valores en cada array debe coincidir con el orden de los ejes listados arriba

Devuelve SOLO JSON con esta estructura exacta:
{{"pp":{example_array},"vox":{example_array},"psoe":{example_array},"programa":{example_array}}}"""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        cache_dir: str = "./.nlp_cache",
        timeout: int = 30,
        max_retries: int = 3
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.model = model
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.timeout = timeout
        self.max_retries = max_retries

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })

    def _get_cache_key(self, payload: Dict) -> str:
        """Generate cache key from payload."""
        content = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()

    def _get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Get cached response."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.debug(f"Cache read error: {e}")
        return None

    def _save_to_cache(self, cache_key: str, data: Dict):
        """Save response to cache."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f)
        except Exception as e:
            logger.debug(f"Cache write error: {e}")

    def _call_api(self, payload: Dict) -> Tuple[Optional[Dict], int, float]:
        """
        Call vLLM API with retries.

        Returns:
            (response_dict, tokens_used, latency_ms)
        """
        cache_key = self._get_cache_key(payload)

        # Check cache
        cached = self._get_from_cache(cache_key)
        if cached:
            logger.debug("Cache hit")
            return cached.get('result'), cached.get('tokens', 0), 0.0

        # Make API call with retries
        for attempt in range(self.max_retries):
            try:
                start = time.time()
                response = self.session.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    timeout=self.timeout
                )
                latency_ms = (time.time() - start) * 1000

                if response.status_code == 200:
                    data = response.json()
                    content = data['choices'][0]['message']['content']
                    if '</think>' in content:
                        content = content.split('</think>')[-1]

                    print(f'content: {content}')

                    tokens = data.get('usage', {}).get('total_tokens', 0)

                    # Parse JSON from content
                    try:
                        result = json.loads(content.strip())
                    except json.JSONDecodeError:
                        # Try to extract JSON from markdown code blocks
                        import re
                        match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', content, re.DOTALL)
                        if match:
                            result = json.loads(match.group(1))
                        else:
                            # Try to find JSON object in text
                            match = re.search(r'\{.*\}', content, re.DOTALL)
                            if match:
                                result = json.loads(match.group(0))
                            else:
                                raise ValueError("No valid JSON found in response")

                    # Cache successful response
                    self._save_to_cache(cache_key, {
                        'result': result,
                        'tokens': tokens,
                        'latency_ms': latency_ms
                    })

                    return result, tokens, latency_ms
                else:
                    logger.warning(f"API error {response.status_code}: {response.text}")

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    sleep_time = 2 ** attempt  # Exponential backoff
                    time.sleep(sleep_time)

        return None, 0, 0.0

    def classify(self, title: str, body: str) -> Tuple[Optional[Dict], int, float]:
        """
        Get classification for news article.

        Returns:
            (classification_dict, tokens_used, latency_ms)
        """
        # Truncate body to ~8000 chars
        body_truncated = body[:8000] if len(body) > 8000 else body

        user_prompt = self.CLASSIFICATION_TEMPLATE.format(
            title=title,
            body=body_truncated
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.CLASSIFICATION_SYSTEM},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0,
            "max_tokens": 1024*5
        }

        return self._call_api(payload)

    def evaluate_axes(self, title: str, body: str, topic: str, axes: list) -> Tuple[Optional[Dict], int, float]:
        """
        Evaluate article on topic-specific axes.

        Args:
            title: Article title
            body: Article body
            topic: Topic key (e.g., "transporte")
            axes: List of axis names for this topic

        Returns:
            (axes_scores_dict, tokens_used, latency_ms)
        """
        if not axes:
            return {}, 0, 0.0

        # Truncate body to ~8000 chars
        body_truncated = body[:8000] if len(body) > 8000 else body

        # Build axes list text
        axes_list = "\n".join([f"- {axis}" for axis in axes])

        # Build example JSON structure
        axes_json_example = "{"
        axes_json_example += ",".join([f'"{axis}":50' for axis in axes])
        axes_json_example += "}"

        user_prompt = self.AXES_TEMPLATE.format(
            title=title,
            body=body_truncated,
            topic=topic,
            axes_list=axes_list,
            axes_json_example=axes_json_example
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.AXES_SYSTEM},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0,
            "max_tokens": 1024*5 # 512
        }

        return self._call_api(payload)

    def summarize(self, title: str, body: str) -> Tuple[Optional[Dict], int, float]:
        """
        Get summary and keywords for news article.

        Returns:
            (summary_dict, tokens_used, latency_ms)
        """
        # Truncate body to ~8000 chars
        body_truncated = body[:8000] if len(body) > 8000 else body

        user_prompt = self.SUMMARY_TEMPLATE.format(
            title=title,
            body=body_truncated
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.SUMMARY_SYSTEM},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0,
            "max_tokens": 1024*5 # 512
        }

        return self._call_api(payload)

    def extract_entities(self, title: str, body: str) -> Tuple[Optional[Dict], int, float]:
        """
        Extract entities (persons, orgs, locations, parties) from news article.

        Returns:
            (entities_dict, tokens_used, latency_ms)
        """
        # Truncate body to ~8000 chars
        body_truncated = body[:8000] if len(body) > 8000 else body

        user_prompt = self.ENTITIES_TEMPLATE.format(
            title=title,
            body=body_truncated
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.ENTITIES_SYSTEM},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0,
            "max_tokens": 1024 * 5 # 512
        }

        return self._call_api(payload)

    def evaluate_topic_radar(self, title: str, body: str, topic: str, axes: list, parties_present: list) -> Tuple[Optional[Dict], int, float]:
        """
        Evaluate article on topic-specific axes from multiple party perspectives.

        Args:
            title: Article title
            body: Article body
            topic: Topic key (e.g., "transporte")
            axes: List of axis names for this topic (should be 5 elements)
            parties_present: List of parties mentioned in article (e.g., ["pp", "psoe"])

        Returns:
            (radar_dict, tokens_used, latency_ms)
            radar_dict structure: {"pp": [v1,v2,v3,v4,v5], "vox": [...], "psoe": [...], "programa": [...]}
        """
        if not axes:
            return {"pp": [None]*5, "vox": [None]*5, "psoe": [None]*5, "programa": [None]*5}, 0, 0.0

        # Truncate body to ~8000 chars
        body_truncated = body[:8000] if len(body) > 8000 else body

        # Build axes list text with numbering
        axes_list = "\n".join([f"{i+1}. {axis}" for i, axis in enumerate(axes)])

        # Build example array
        example_array = "[50, 50, 50, 50, 50]"

        user_prompt = self.RADAR_TEMPLATE.format(
            title=title,
            body=body_truncated,
            topic=topic,
            axes_list=axes_list,
            example_array=example_array
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.RADAR_SYSTEM},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0,
            "max_tokens": 1024*5
        }

        result, tokens, latency = self._call_api(payload)

        # Normalize result: ensure parties not present get null arrays
        if result:
            for party in ["pp", "vox", "psoe"]:
                if party not in parties_present:
                    result[party] = [None] * len(axes)

        return result, tokens, latency
