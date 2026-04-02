"""Tests for Planner._build_execution_prompt — constraint section stripping."""

from orchestrator.planning.planner import Planner


# A minimal system prompt that mirrors the real structure.
MOCK_SYSTEM_PROMPT = """\
# Solar Energy Analysis Platform

## ROLE AND IDENTITY

You are an expert solar energy analyst.

---

## CRITICAL ARCHITECTURAL CONSTRAINT

**You CANNOT call tools directly.** When any tool call is needed, you MUST \
follow the PLAN-THEN-EXECUTE WORKFLOW below to generate an ANALYSIS PLAN.

**NEVER write text that simulates or narrates tool execution.**

**Respond conversationally (without a plan) ONLY when no tool call is needed.**

---

## DOMAIN KNOWLEDGE

### System Design Parameters

DC/AC ratio, GCR, tilt, azimuth, etc.

---

## PLAN-THEN-EXECUTE WORKFLOW

### Step 1: Understand the Request

Parse the user's request.

## FUNCTION DEFINITIONS

### search_modules

Search the CEC module database."""

# Same prompt but without the constraint section.
MOCK_PROMPT_NO_CONSTRAINT = """\
# Solar Energy Analysis Platform

## ROLE AND IDENTITY

You are an expert solar energy analyst.

---

## DOMAIN KNOWLEDGE

### System Design Parameters

DC/AC ratio, GCR, tilt, azimuth, etc.

---

## PLAN-THEN-EXECUTE WORKFLOW

### Step 1: Understand the Request

Parse the user's request.

## FUNCTION DEFINITIONS

### search_modules

Search the CEC module database."""


class TestBuildExecutionPrompt:
    """Tests for stripping the CRITICAL ARCHITECTURAL CONSTRAINT section."""

    def test_strips_constraint_section(self) -> None:
        """Execution prompt does not contain the constraint heading or body."""
        result = Planner._build_execution_prompt(MOCK_SYSTEM_PROMPT)
        assert "CRITICAL ARCHITECTURAL CONSTRAINT" not in result
        assert "You CANNOT call tools directly" not in result
        assert "generate an ANALYSIS PLAN" not in result

    def test_preserves_domain_knowledge(self) -> None:
        """DOMAIN KNOWLEDGE section survives the stripping."""
        result = Planner._build_execution_prompt(MOCK_SYSTEM_PROMPT)
        assert "## DOMAIN KNOWLEDGE" in result
        assert "DC/AC ratio" in result

    def test_preserves_plan_workflow(self) -> None:
        """PLAN-THEN-EXECUTE WORKFLOW section survives the stripping."""
        result = Planner._build_execution_prompt(MOCK_SYSTEM_PROMPT)
        assert "PLAN-THEN-EXECUTE WORKFLOW" in result

    def test_preserves_function_definitions(self) -> None:
        """FUNCTION DEFINITIONS section survives the stripping."""
        result = Planner._build_execution_prompt(MOCK_SYSTEM_PROMPT)
        assert "FUNCTION DEFINITIONS" in result
        assert "search_modules" in result

    def test_preserves_role_section(self) -> None:
        """ROLE AND IDENTITY section survives the stripping."""
        result = Planner._build_execution_prompt(MOCK_SYSTEM_PROMPT)
        assert "ROLE AND IDENTITY" in result
        assert "expert solar energy analyst" in result

    def test_no_excessive_blank_lines(self) -> None:
        """Stripping does not leave triple+ blank lines."""
        result = Planner._build_execution_prompt(MOCK_SYSTEM_PROMPT)
        assert "\n\n\n" not in result

    def test_missing_section_returns_full_prompt(self) -> None:
        """If the constraint section is absent, return the full prompt unchanged."""
        result = Planner._build_execution_prompt(MOCK_PROMPT_NO_CONSTRAINT)
        assert result == MOCK_PROMPT_NO_CONSTRAINT

    def test_real_system_prompt_strips_cleanly(self) -> None:
        """Smoke test against the actual system_prompt.md file."""
        real_prompt = Planner._load_system_prompt(
            "orchestrator/prompts/system_prompt.md"
        )
        result = Planner._build_execution_prompt(real_prompt)

        # Constraint is gone
        assert "CRITICAL ARCHITECTURAL CONSTRAINT" not in result
        assert "You CANNOT call tools directly" not in result

        # Key sections remain
        assert "DOMAIN KNOWLEDGE" in result
        assert "PLAN-THEN-EXECUTE WORKFLOW" in result
        assert "FUNCTION DEFINITIONS" in result
        assert "search_modules" in result
        assert "run_production" in result
