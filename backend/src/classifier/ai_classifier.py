"""
Theme Park Downtime Tracker - AI-Based Ride Classifier
Implements AI-powered tier classification using LLM API (Priority 4 in classification hierarchy).
"""

import os
import json
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

try:
    from ..utils.logger import logger
except ImportError:
    from utils.logger import logger


VALID_CATEGORIES = ['ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE']


@dataclass
class AIClassificationResult:
    """Result of AI-based classification."""
    tier: int
    category: str  # 'ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE'
    confidence: float
    reasoning: str
    research_sources: List[str]


class AIClassifier:
    """
    AI-powered ride tier classifier using Zen MCP chat tool (Priority 4).

    Uses mcp__zen__chat with Gemini-2.5-pro and web search to classify rides
    based on research about the attraction (capacity, thrill level, popularity).

    Confidence scores: 0.50-0.95 (varies based on AI certainty)
    """

    CLASSIFICATION_PROMPT_TEMPLATE = """You are a theme park ride classification expert. Your task is to classify the following ride into one of three tiers AND one of four categories based on its type, significance, capacity, and guest impact:

**Ride Information:**
- Ride Name: {ride_name}
- Park Name: {park_name}
- Park Location: {park_location}

**Classification Tiers:**
- **Tier 1 (Major Attractions, 3x weight)**: E-ticket attractions, major roller coasters, signature rides with high capacity and long wait times. Examples: Space Mountain, Expedition Everest, Millennium Falcon, major dark rides. Will routinely have the longest wait times, averaging over 30m a most times of the day.

- **Tier 2 (Standard Attractions, 2x weight)**: Standard rides and shows with moderate capacity. Most dark rides, water rides, and standard coasters fall here. Examples: Pirates of the Caribbean (if not park's signature), standard flume rides.  Wait times are low most of the time but can peak to 30 minutes during busy days.

- **Tier 3 (Minor Attractions, 1x weight)**: Kiddie rides, carousels, low-capacity flat rides, playground areas, and walk-through attractions. Examples: Dumbo, Prince Charming Regal Carrousel, teacups, character meet areas. These rides almost never have a wait.

**Category Classification:**
In addition to tier, classify the attraction into ONE of these categories:
- **ATTRACTION**: Traditional mechanical rides - roller coasters, dark rides, water rides, flat rides, spinning rides, drop towers, simulators. Anything guests physically ride on.
- **MEET_AND_GREET**: Character encounters - "Meet Mickey Mouse", "Character Spot", "Princess Fairytale Hall", photo opportunities with characters. These open/close based on character schedules, not mechanical issues.
- **SHOW**: Theater shows, presentations, stage performances, 4D films, sing-alongs, stunt shows, fireworks, parades. Scheduled entertainment with set times.
- **EXPERIENCE**: Walk-through attractions, exhibits, trails, discovery centers, interactive play areas. Non-ride experiences guests walk through at their own pace.

**Classification Criteria:**
1. **Capacity & Throughput**: High-capacity rides (1000+ guests/hour) lean toward Tier 1/2
2. **Thrill Level**: Major coasters and high-thrill experiences typically Tier 1
3. **Popularity & Wait Times**: Rides with consistently high demand are typically Tier 1
4. **Guest Impact**: Signature attractions that define a park are Tier 1
5. **Investment Level**: E-ticket budgets ($50M+) typically indicate Tier 1

**Instructions:**
1. Research the ride using web search to find:
   - Ride type (coaster, dark ride, flat ride, show, character meet, etc.)
   - Capacity/throughput data
   - Opening year and construction cost
   - Guest reviews and popularity metrics
   - Manufacturer and technical specs

2. Determine the appropriate tier (1, 2, or 3)

3. Determine the appropriate category (ATTRACTION, MEET_AND_GREET, SHOW, or EXPERIENCE)

4. Provide your confidence score (0.50 to 1.00):
   - 0.90-1.00: Definitive information available
   - 0.75-0.89: Strong evidence with minor uncertainty
   - 0.60-0.74: Moderate evidence, some assumptions
   - 0.50-0.59: Limited information, best guess

5. Return your response in **EXACT** JSON format:
```json
{{
  "tier": 1,
  "category": "ATTRACTION",
  "confidence": 0.85,
  "reasoning": "Space Mountain is a signature indoor roller coaster at Magic Kingdom, opened in 1975. It's one of the park's most popular E-ticket attractions with capacity of 1800 guests/hour. Consistently maintains 60+ minute wait times.",
  "research_sources": [
    "https://rcdb.com/...",
    "https://en.wikipedia.org/wiki/..."
  ]
}}
```

**CRITICAL**: Return ONLY valid JSON with these exact fields: tier, category, confidence, reasoning, research_sources. Do not include any additional text outside the JSON structure.
"""

    def __init__(self, working_directory: Optional[str] = None):
        """
        Initialize AI classifier.

        Args:
            working_directory: Absolute path for temporary files (required by zen)
        """
        self.working_directory = working_directory or os.getcwd()

    def classify(
        self,
        ride_name: str,
        park_name: str,
        park_location: Optional[str] = None
    ) -> AIClassificationResult:
        """
        Classify ride using AI with web research.

        Args:
            ride_name: Name of the ride
            park_name: Name of the park
            park_location: Park location (city, state/province)

        Returns:
            AIClassificationResult with tier, confidence, reasoning, sources

        Raises:
            AIClassifierError: If AI classification fails
        """
        location_str = park_location or "Unknown location"

        prompt = self.CLASSIFICATION_PROMPT_TEMPLATE.format(
            ride_name=ride_name,
            park_name=park_name,
            park_location=location_str
        )

        try:
            # Check if OpenAI is available
            if OpenAI is None:
                raise AIClassifierError(
                    "OpenAI package not installed. Run: pip install openai"
                )

            # Get API key from environment
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise AIClassifierError(
                    "OPENAI_API_KEY environment variable not set. "
                    "Please set it to your OpenAI API key."
                )

            logger.info(f"AI classification requested for: {ride_name} at {park_name}")

            # Initialize OpenAI client and make API call
            client = OpenAI(api_key=api_key)

            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Much cheaper than gpt-4 (~100x), sufficient for classification
                messages=[
                    {
                        "role": "system",
                        "content": "You are a theme park ride classification expert. Return ONLY valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=1000
            )

            response_text = response.choices[0].message.content
            logger.debug(f"AI response for {ride_name}: {response_text[:200]}...")

            # Parse and return the classification result
            result = self.parse_ai_response(response_text)
            logger.info(
                f"AI classified {ride_name} as Tier {result.tier} "
                f"(confidence: {result.confidence:.2f})"
            )
            return result

        except AIClassifierError:
            # Re-raise our custom errors
            raise

        except Exception as e:
            logger.error(f"AI classification failed for {ride_name}: {e}")
            raise AIClassifierError(f"Failed to classify {ride_name}: {e}")

    def batch_classify(
        self,
        rides: List[Dict[str, Any]],
        max_concurrent: int = 5
    ) -> Dict[int, AIClassificationResult]:
        """
        Classify multiple rides using AI (with rate limiting).

        Args:
            rides: List of dicts with keys: ride_id, ride_name, park_name, park_location
            max_concurrent: Maximum concurrent AI requests

        Returns:
            Dictionary mapping ride_id to AIClassificationResult
        """
        # This will be implemented in classification_service.py
        # with proper ThreadPoolExecutor and MCP integration
        raise NotImplementedError(
            "Batch AI classification is handled by ClassificationService"
        )

    def parse_ai_response(self, response_text: str) -> AIClassificationResult:
        """
        Parse JSON response from AI model.

        Args:
            response_text: Raw response text from AI

        Returns:
            AIClassificationResult

        Raises:
            AIClassifierError: If response cannot be parsed
        """
        try:
            # Extract JSON from response (handle markdown code blocks)
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1

            if json_start == -1 or json_end == 0:
                raise ValueError("No JSON found in response")

            json_str = response_text[json_start:json_end]
            data = json.loads(json_str)

            # Validate required fields
            required = ['tier', 'category', 'confidence', 'reasoning', 'research_sources']
            for field in required:
                if field not in data:
                    raise ValueError(f"Missing required field: {field}")

            # Validate tier value
            if data['tier'] not in [1, 2, 3]:
                raise ValueError(f"Invalid tier value: {data['tier']}")

            # Validate category value
            if data['category'] not in VALID_CATEGORIES:
                raise ValueError(f"Invalid category value: {data['category']}. Must be one of: {VALID_CATEGORIES}")

            # Validate confidence range
            if not (0.50 <= data['confidence'] <= 1.00):
                raise ValueError(f"Confidence out of range: {data['confidence']}")

            return AIClassificationResult(
                tier=data['tier'],
                category=data['category'],
                confidence=data['confidence'],
                reasoning=data['reasoning'],
                research_sources=data['research_sources']
            )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI JSON response: {e}")
            logger.debug(f"Response text: {response_text}")
            raise AIClassifierError(f"Invalid JSON in AI response: {e}")

        except (ValueError, KeyError) as e:
            logger.error(f"Invalid AI response structure: {e}")
            raise AIClassifierError(f"Invalid AI response: {e}")


class AIClassifierError(Exception):
    """Raised when AI classification fails."""
    pass
