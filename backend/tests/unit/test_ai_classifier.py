"""
Theme Park Downtime Tracker - AI Classifier Unit Tests

Tests AIClassifier:
- JSON response parsing (extract from markdown, validate fields)
- Tier validation (1, 2, 3 only)
- Category validation (ATTRACTION, MEET_AND_GREET, SHOW, EXPERIENCE)
- Confidence range validation (0.50 to 1.00)
- Error handling (invalid JSON, missing fields, out-of-range values)
- AIClassificationResult dataclass

Note: classify() and batch_classify() are not tested here as they require
MCP integration. These will be tested in integration tests.

Priority: P2 - Important for AI classification system
"""

import os
import pytest
from classifier.ai_classifier import AIClassifier, AIClassificationResult, AIClassifierError

# Skip tests that require real OpenAI API key
requires_openai_key = pytest.mark.skipif(
    not os.environ.get('OPENAI_API_KEY'),
    reason="OPENAI_API_KEY not set - skipping live API test"
)


class TestAIClassifierInit:
    """Test AI classifier initialization."""

    def test_init_with_working_directory(self):
        """__init__() should accept working directory parameter."""
        classifier = AIClassifier(working_directory="/tmp/test")

        assert classifier.working_directory == "/tmp/test"

    def test_init_defaults_to_cwd(self):
        """__init__() should default to current working directory."""
        import os
        classifier = AIClassifier()

        assert classifier.working_directory == os.getcwd()


class TestParseAIResponse:
    """Test parsing AI JSON responses."""

    def test_parse_valid_json_response(self):
        """parse_ai_response() should parse valid JSON response."""
        classifier = AIClassifier()

        response = """{
  "tier": 1,
  "category": "ATTRACTION",
  "confidence": 0.85,
  "reasoning": "Space Mountain is a signature E-ticket attraction",
  "research_sources": ["https://rcdb.com/1234", "https://wikipedia.org/space_mountain"]
}"""

        result = classifier.parse_ai_response(response)

        assert result.tier == 1
        assert result.category == "ATTRACTION"
        assert result.confidence == 0.85
        assert result.reasoning == "Space Mountain is a signature E-ticket attraction"
        assert len(result.research_sources) == 2
        assert "rcdb.com" in result.research_sources[0]

    def test_parse_json_with_markdown_code_blocks(self):
        """parse_ai_response() should extract JSON from markdown code blocks."""
        classifier = AIClassifier()

        response = """Here is the classification:

```json
{
  "tier": 2,
  "category": "ATTRACTION",
  "confidence": 0.70,
  "reasoning": "Standard dark ride with moderate capacity",
  "research_sources": ["https://example.com"]
}
```

Hope this helps!"""

        result = classifier.parse_ai_response(response)

        assert result.tier == 2
        assert result.category == "ATTRACTION"
        assert result.confidence == 0.70

    def test_parse_json_with_extra_text(self):
        """parse_ai_response() should extract JSON even with surrounding text."""
        classifier = AIClassifier()

        response = """Based on my research, here's the classification:
{
  "tier": 3,
  "category": "ATTRACTION",
  "confidence": 0.65,
  "reasoning": "Kiddie ride with low capacity",
  "research_sources": []
}
This is my final answer."""

        result = classifier.parse_ai_response(response)

        assert result.tier == 3
        assert result.category == "ATTRACTION"
        assert result.confidence == 0.65
        assert result.research_sources == []

    def test_parse_tier_1_classification(self):
        """parse_ai_response() should accept tier 1."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": 0.90, "reasoning": "Major coaster", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.tier == 1

    def test_parse_tier_2_classification(self):
        """parse_ai_response() should accept tier 2."""
        classifier = AIClassifier()

        response = '{"tier": 2, "category": "ATTRACTION", "confidence": 0.75, "reasoning": "Standard ride", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.tier == 2

    def test_parse_tier_3_classification(self):
        """parse_ai_response() should accept tier 3."""
        classifier = AIClassifier()

        response = '{"tier": 3, "category": "ATTRACTION", "confidence": 0.60, "reasoning": "Kiddie ride", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.tier == 3

    def test_parse_minimum_confidence(self):
        """parse_ai_response() should accept confidence = 0.50."""
        classifier = AIClassifier()

        response = '{"tier": 2, "category": "ATTRACTION", "confidence": 0.50, "reasoning": "Limited info", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.confidence == 0.50

    def test_parse_maximum_confidence(self):
        """parse_ai_response() should accept confidence = 1.00."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": 1.00, "reasoning": "Definitive", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.confidence == 1.00

    def test_parse_empty_research_sources(self):
        """parse_ai_response() should accept empty research_sources list."""
        classifier = AIClassifier()

        response = '{"tier": 2, "category": "ATTRACTION", "confidence": 0.70, "reasoning": "Test", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.research_sources == []

    def test_parse_multiple_research_sources(self):
        """parse_ai_response() should accept multiple research sources."""
        classifier = AIClassifier()

        response = """{
  "tier": 1,
  "category": "ATTRACTION",
  "confidence": 0.95,
  "reasoning": "Well-documented",
  "research_sources": [
    "https://rcdb.com/1234",
    "https://wikipedia.org/article",
    "https://themeparks.com/ride"
  ]
}"""

        result = classifier.parse_ai_response(response)

        assert len(result.research_sources) == 3

    def test_parse_meet_and_greet_category(self):
        """parse_ai_response() should accept MEET_AND_GREET category."""
        classifier = AIClassifier()

        response = '{"tier": 3, "category": "MEET_AND_GREET", "confidence": 0.90, "reasoning": "Character encounter", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.category == "MEET_AND_GREET"

    def test_parse_show_category(self):
        """parse_ai_response() should accept SHOW category."""
        classifier = AIClassifier()

        response = '{"tier": 2, "category": "SHOW", "confidence": 0.85, "reasoning": "Theater performance", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.category == "SHOW"

    def test_parse_experience_category(self):
        """parse_ai_response() should accept EXPERIENCE category."""
        classifier = AIClassifier()

        response = '{"tier": 3, "category": "EXPERIENCE", "confidence": 0.80, "reasoning": "Walk-through exhibit", "research_sources": []}'

        result = classifier.parse_ai_response(response)

        assert result.category == "EXPERIENCE"


class TestParseAIResponseErrors:
    """Test error handling in AI response parsing."""

    def test_parse_invalid_json(self):
        """parse_ai_response() should raise AIClassifierError for invalid JSON."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": 0.85, invalid json}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "Invalid JSON" in str(exc_info.value)

    def test_parse_no_json_in_response(self):
        """parse_ai_response() should raise AIClassifierError when no JSON found."""
        classifier = AIClassifier()

        response = "This is just plain text with no JSON"

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "Invalid AI response" in str(exc_info.value)

    def test_parse_missing_tier_field(self):
        """parse_ai_response() should raise AIClassifierError for missing tier."""
        classifier = AIClassifier()

        response = '{"category": "ATTRACTION", "confidence": 0.85, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "tier" in str(exc_info.value).lower()

    def test_parse_missing_category_field(self):
        """parse_ai_response() should raise AIClassifierError for missing category."""
        classifier = AIClassifier()

        response = '{"tier": 1, "confidence": 0.85, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "category" in str(exc_info.value).lower()

    def test_parse_missing_confidence_field(self):
        """parse_ai_response() should raise AIClassifierError for missing confidence."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "confidence" in str(exc_info.value).lower()

    def test_parse_missing_reasoning_field(self):
        """parse_ai_response() should raise AIClassifierError for missing reasoning."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": 0.85, "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "reasoning" in str(exc_info.value).lower()

    def test_parse_missing_research_sources_field(self):
        """parse_ai_response() should raise AIClassifierError for missing research_sources."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": 0.85, "reasoning": "Test"}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "research_sources" in str(exc_info.value).lower()

    def test_parse_invalid_tier_value_0(self):
        """parse_ai_response() should reject tier = 0."""
        classifier = AIClassifier()

        response = '{"tier": 0, "category": "ATTRACTION", "confidence": 0.85, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "tier" in str(exc_info.value).lower()

    def test_parse_invalid_tier_value_4(self):
        """parse_ai_response() should reject tier = 4."""
        classifier = AIClassifier()

        response = '{"tier": 4, "category": "ATTRACTION", "confidence": 0.85, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "tier" in str(exc_info.value).lower()

    def test_parse_invalid_category_value(self):
        """parse_ai_response() should reject invalid category values."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "INVALID", "confidence": 0.85, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "category" in str(exc_info.value).lower()

    def test_parse_lowercase_category_rejected(self):
        """parse_ai_response() should reject lowercase category values."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "attraction", "confidence": 0.85, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "category" in str(exc_info.value).lower()

    def test_parse_confidence_below_minimum(self):
        """parse_ai_response() should reject confidence < 0.50."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": 0.49, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "confidence" in str(exc_info.value).lower()

    def test_parse_confidence_above_maximum(self):
        """parse_ai_response() should reject confidence > 1.00."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": 1.01, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "confidence" in str(exc_info.value).lower()

    def test_parse_negative_confidence(self):
        """parse_ai_response() should reject negative confidence."""
        classifier = AIClassifier()

        response = '{"tier": 1, "category": "ATTRACTION", "confidence": -0.50, "reasoning": "Test", "research_sources": []}'

        with pytest.raises(AIClassifierError) as exc_info:
            classifier.parse_ai_response(response)

        assert "confidence" in str(exc_info.value).lower()


class TestClassify:
    """Test classify() method - requires MCP integration."""

    @requires_openai_key
    def test_classify_space_mountain_returns_tier_1(self):
        """classify() should successfully classify Space Mountain as Tier 1 using real API."""
        classifier = AIClassifier()

        result = classifier.classify("Space Mountain", "Magic Kingdom", "Orlando, FL")

        # Verify we got a valid result
        assert result.tier in [1, 2, 3], f"Expected tier 1-3, got {result.tier}"
        assert result.category in ["ATTRACTION", "MEET_AND_GREET", "SHOW", "EXPERIENCE"], f"Expected valid category, got {result.category}"
        assert result.confidence >= 0.5, f"Expected confidence >= 0.5, got {result.confidence}"
        assert len(result.reasoning) > 0, "Expected reasoning text"

        # Space Mountain should be classified as Tier 1 ATTRACTION (iconic E-ticket)
        assert result.tier == 1, f"Space Mountain should be Tier 1, got Tier {result.tier}"
        assert result.category == "ATTRACTION", f"Space Mountain should be ATTRACTION, got {result.category}"


class TestBatchClassify:
    """Test batch_classify() method - requires MCP integration."""

    def test_batch_classify_raises_not_implemented(self):
        """batch_classify() should raise NotImplementedError (handled by ClassificationService)."""
        classifier = AIClassifier()

        rides = [
            {"ride_id": 1, "ride_name": "Test", "park_name": "Park", "park_location": "USA"}
        ]

        with pytest.raises(NotImplementedError) as exc_info:
            classifier.batch_classify(rides)

        assert "ClassificationService" in str(exc_info.value)


class TestAIClassificationResult:
    """Test AIClassificationResult dataclass."""

    def test_ai_classification_result_fields(self):
        """AIClassificationResult should have tier, category, confidence, reasoning, research_sources."""
        result = AIClassificationResult(
            tier=1,
            category="ATTRACTION",
            confidence=0.85,
            reasoning="Test reasoning",
            research_sources=["https://example.com"]
        )

        assert result.tier == 1
        assert result.category == "ATTRACTION"
        assert result.confidence == 0.85
        assert result.reasoning == "Test reasoning"
        assert result.research_sources == ["https://example.com"]

    def test_ai_classification_result_empty_sources(self):
        """AIClassificationResult should allow empty research_sources."""
        result = AIClassificationResult(
            tier=2,
            category="ATTRACTION",
            confidence=0.70,
            reasoning="Limited research",
            research_sources=[]
        )

        assert result.research_sources == []

    def test_ai_classification_result_meet_and_greet(self):
        """AIClassificationResult should accept MEET_AND_GREET category."""
        result = AIClassificationResult(
            tier=3,
            category="MEET_AND_GREET",
            confidence=0.90,
            reasoning="Character encounter",
            research_sources=[]
        )

        assert result.category == "MEET_AND_GREET"

    def test_ai_classification_result_show(self):
        """AIClassificationResult should accept SHOW category."""
        result = AIClassificationResult(
            tier=2,
            category="SHOW",
            confidence=0.85,
            reasoning="Theater performance",
            research_sources=[]
        )

        assert result.category == "SHOW"

    def test_ai_classification_result_experience(self):
        """AIClassificationResult should accept EXPERIENCE category."""
        result = AIClassificationResult(
            tier=3,
            category="EXPERIENCE",
            confidence=0.80,
            reasoning="Walk-through exhibit",
            research_sources=[]
        )

        assert result.category == "EXPERIENCE"


class TestAIClassifierError:
    """Test AIClassifierError exception."""

    def test_ai_classifier_error_is_exception(self):
        """AIClassifierError should be an Exception subclass."""
        error = AIClassifierError("Test error")

        assert isinstance(error, Exception)
        assert str(error) == "Test error"

    def test_ai_classifier_error_can_be_raised(self):
        """AIClassifierError should be raiseable."""
        with pytest.raises(AIClassifierError) as exc_info:
            raise AIClassifierError("Classification failed")

        assert "Classification failed" in str(exc_info.value)
