"""
Theme Park Downtime Tracker - AI-Based Ride Classifier
Implements AI-powered tier classification using Zen MCP (Priority 4 in classification hierarchy).
"""

import os
import json
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from ..utils.logger import logger


@dataclass
class AIClassificationResult:
    """Result of AI-based classification."""
    tier: int
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

    CLASSIFICATION_PROMPT_TEMPLATE = """You are a theme park ride classification expert. Your task is to classify the following ride into one of three tiers based on its significance, capacity, and guest impact:

**Ride Information:**
- Ride Name: {ride_name}
- Park Name: {park_name}
- Park Location: {park_location}

**Classification Tiers:**
- **Tier 1 (Major Attractions, 3x weight)**: E-ticket attractions, major roller coasters, signature rides with high capacity and long wait times. Examples: Space Mountain, Expedition Everest, Millennium Falcon, major dark rides.

- **Tier 2 (Standard Attractions, 2x weight)**: Standard rides and shows with moderate capacity. Most dark rides, water rides, and standard coasters fall here. Examples: Pirates of the Caribbean (if not park's signature), standard flume rides.

- **Tier 3 (Minor Attractions, 1x weight)**: Kiddie rides, carousels, low-capacity flat rides, playground areas, and walk-through attractions. Examples: Dumbo, Prince Charming Regal Carrousel, teacups, character meet areas.

**Classification Criteria:**
1. **Capacity & Throughput**: High-capacity rides (1000+ guests/hour) lean toward Tier 1/2
2. **Thrill Level**: Major coasters and high-thrill experiences typically Tier 1
3. **Popularity & Wait Times**: Rides with consistently high demand are typically Tier 1
4. **Guest Impact**: Signature attractions that define a park are Tier 1
5. **Investment Level**: E-ticket budgets ($50M+) typically indicate Tier 1

**Instructions:**
1. Research the ride using web search to find:
   - Ride type (coaster, dark ride, flat ride, etc.)
   - Capacity/throughput data
   - Opening year and construction cost
   - Guest reviews and popularity metrics
   - Manufacturer and technical specs

2. Determine the appropriate tier (1, 2, or 3)

3. Provide your confidence score (0.50 to 1.00):
   - 0.90-1.00: Definitive information available
   - 0.75-0.89: Strong evidence with minor uncertainty
   - 0.60-0.74: Moderate evidence, some assumptions
   - 0.50-0.59: Limited information, best guess

4. Return your response in **EXACT** JSON format:
```json
{{
  "tier": 1,
  "confidence": 0.85,
  "reasoning": "Space Mountain is a signature indoor roller coaster at Magic Kingdom, opened in 1975. It's one of the park's most popular E-ticket attractions with capacity of 1800 guests/hour. Consistently maintains 60+ minute wait times.",
  "research_sources": [
    "https://rcdb.com/...",
    "https://en.wikipedia.org/wiki/..."
  ]
}}
```

**CRITICAL**: Return ONLY valid JSON with these exact fields: tier, confidence, reasoning, research_sources. Do not include any additional text outside the JSON structure.
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
            # Use mcp__zen__chat tool for AI classification
            # This will be called via the MCP integration
            # For now, return a placeholder indicating AI call is needed
            logger.info(f"AI classification needed for: {ride_name} at {park_name}")

            # This is a placeholder - the actual implementation will use MCP
            # The classification service will handle the MCP call
            raise NotImplementedError(
                "AI classification requires MCP zen__chat integration. "
                "Use ClassificationService.classify() instead of calling AIClassifier directly."
            )

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
            required = ['tier', 'confidence', 'reasoning', 'research_sources']
            for field in required:
                if field not in data:
                    raise ValueError(f"Missing required field: {field}")

            # Validate tier value
            if data['tier'] not in [1, 2, 3]:
                raise ValueError(f"Invalid tier value: {data['tier']}")

            # Validate confidence range
            if not (0.50 <= data['confidence'] <= 1.00):
                raise ValueError(f"Confidence out of range: {data['confidence']}")

            return AIClassificationResult(
                tier=data['tier'],
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
