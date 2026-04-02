"""Tests for Planner._clean_plan_response — duplicate plan block removal."""

import pytest

from orchestrator.planning.planner import Planner


# ---------------------------------------------------------------------------
# Fixtures: realistic GPT-5 response fragments
# ---------------------------------------------------------------------------

SINGLE_PLAN = """\
I'll run a production analysis for this site.

ANALYSIS PLAN
═══════════════════════════════════════════════

Site: Phoenix Solar Farm (33.45, -111.98)
Analyses requested: Production

STEP 1 — Equipment Search (~2-3 seconds)
  → GET /analyses/equipment/modules?search=CSI Solar 550
  → GET /analyses/equipment/inverters?search=Sungrow Power Supply SG250
  Purpose: Find exact CEC names

STEP 2 — Production Modeling (~10-20 seconds)
  → POST /analyses/production
  Key parameters:
    DC: 6.5 MW | AC: 5.0 MW | DC/AC: 1.30
    Racking: tracker | Tilt: 60 | GCR: 0.34

Shall I proceed, or would you like to adjust any parameters?"""

SINGLE_PLAN_NO_CONFIRM = """\
ANALYSIS PLAN
═══════════════════════════════════════════════

Site: Denver (39.74, -104.99)

STEP 1 — LMP Price Lookup (~5 seconds)
→ GET /lmp/prices with iso=pjm, zone=AEP"""

DOUBLE_PLAN = """\
I'll set up a production analysis for this Phoenix site.

ANALYSIS PLAN
═══════════════════════════════════════════════

Site: Phoenix Solar Farm (33.45, -111.98)

STEP 1 — Equipment Search
STEP 2 — Production Modeling

Shall I proceed?

Here's a more detailed breakdown of the plan:

ANALYSIS PLAN
═══════════════════════════════════════════════

Site: Phoenix Solar Farm (33.45, -111.98)
Analyses requested: Production only

STEP 1 — Equipment Search (~2-3 seconds)
  → GET /analyses/equipment/modules?search=CSI Solar 550
  → GET /analyses/equipment/inverters?search=Sungrow Power Supply SG250
  Purpose: Find exact CEC module and inverter names

STEP 2 — Production Modeling (~10-20 seconds)
  → POST /analyses/production
  Key parameters:
    DC: 6.5 MW | AC: 5.0 MW | DC/AC: 1.30
    Racking: tracker | Tilt: 60 | GCR: 0.34
    Losses: API defaults

DEFAULTS APPLIED:
  • GCR: 0.34
  • Module: CSI Solar 550W — will confirm via search
  • Inverter: Sungrow SG250HX — will confirm via search

Shall I proceed, or would you like to adjust any parameters?"""

DOUBLE_PLAN_NO_CONFIRM_BETWEEN = """\
ANALYSIS PLAN
Site: Test (33.45, -111.98)
STEP 1 — Equipment Search
STEP 2 — Production

Now let me provide more detail:

ANALYSIS PLAN
Site: Test (33.45, -111.98)
STEP 1 — Equipment Search (~2 seconds)
  → GET /analyses/equipment/modules
STEP 2 — Production (~15 seconds)
  → POST /analyses/production

Shall I proceed?"""

CONVERSATIONAL_RESPONSE = """\
A capacity factor of 25% is excellent for a tracker site in Phoenix. \
The Desert Southwest typically sees AC capacity factors between 28-32% \
for single-axis trackers, so your result is within the expected range.

Would you like me to run a bill savings analysis next?"""

PLAN_WITH_APPROVE_AND_EXECUTE = """\
ANALYSIS PLAN
═══════════════════════════════════════════════

STEP 1 — LMP Price Lookup (~5 seconds)
→ GET /lmp/prices with iso=pjm, zone=AEP, market=DAY_AHEAD_HOURLY

Ready to execute — approve and execute when you're ready."""

PLAN_WITH_ADJUST_VARIANT = """\
ANALYSIS PLAN

Site: Houston (29.76, -95.37)
STEP 1 — Production Modeling

Do you want to adjust anything before I run this?"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCleanPlanResponsePassthrough:
    """Responses that should pass through unchanged (or nearly so)."""

    def test_no_plan_passes_through(self) -> None:
        """Conversational response without ANALYSIS PLAN is unchanged."""
        result = Planner._clean_plan_response(CONVERSATIONAL_RESPONSE)
        assert result == CONVERSATIONAL_RESPONSE

    def test_empty_string_passes_through(self) -> None:
        """Empty string is returned as-is."""
        assert Planner._clean_plan_response("") == ""

    def test_single_plan_with_confirm_truncates_after_confirm(self) -> None:
        """Single plan with 'Shall I proceed?' keeps everything up to that line."""
        result = Planner._clean_plan_response(SINGLE_PLAN)
        assert "ANALYSIS PLAN" in result
        assert "Shall I proceed" in result
        # Should be identical since confirm is the last line
        assert result == SINGLE_PLAN

    def test_single_plan_no_confirm_passes_through(self) -> None:
        """Single plan without a confirmation prompt passes through unchanged."""
        result = Planner._clean_plan_response(SINGLE_PLAN_NO_CONFIRM)
        assert result == SINGLE_PLAN_NO_CONFIRM


class TestCleanPlanResponseTruncation:
    """Responses with duplicate plan blocks that should be truncated."""

    def test_double_plan_keeps_first_only(self) -> None:
        """Two ANALYSIS PLAN blocks → only the first is kept."""
        result = Planner._clean_plan_response(DOUBLE_PLAN)

        # First plan is present
        assert "ANALYSIS PLAN" in result
        assert "Shall I proceed?" in result

        # Second plan is gone — look for content unique to the second block
        assert "Here's a more detailed breakdown" not in result
        assert "DEFAULTS APPLIED" not in result

        # Only one ANALYSIS PLAN header
        assert result.count("ANALYSIS PLAN") == 1

    def test_double_plan_no_confirm_between(self) -> None:
        """Two plans with no confirm between them — truncates at second header."""
        result = Planner._clean_plan_response(DOUBLE_PLAN_NO_CONFIRM_BETWEEN)

        assert result.count("ANALYSIS PLAN") == 1
        # Second plan's detailed content is gone
        assert "GET /analyses/equipment/modules" not in result
        assert "POST /analyses/production" not in result

    def test_truncation_preserves_leading_text(self) -> None:
        """Text before the first ANALYSIS PLAN header is preserved."""
        result = Planner._clean_plan_response(DOUBLE_PLAN)
        assert result.startswith("I'll set up a production analysis")


class TestConfirmationPatterns:
    """Various confirmation prompt patterns are recognized."""

    def test_shall_i_proceed(self) -> None:
        """'Shall I proceed?' is recognized."""
        result = Planner._clean_plan_response(SINGLE_PLAN)
        assert result.rstrip().endswith("parameters?")

    def test_ready_to_execute(self) -> None:
        """'Ready to execute' is recognized."""
        result = Planner._clean_plan_response(PLAN_WITH_APPROVE_AND_EXECUTE)
        assert "Ready to execute" in result
        # Nothing after the confirmation line
        last_line = result.rstrip().split("\n")[-1]
        assert "Ready to execute" in last_line

    def test_want_to_adjust(self) -> None:
        """'want to adjust' is recognized."""
        result = Planner._clean_plan_response(PLAN_WITH_ADJUST_VARIANT)
        assert "want to adjust" in result
        last_line = result.rstrip().split("\n")[-1]
        assert "want to adjust" in last_line

    def test_trailing_text_after_confirm_removed(self) -> None:
        """Any text after a confirmation prompt line is discarded."""
        content = SINGLE_PLAN + "\n\nHere is some extra trailing text.\nMore stuff."
        result = Planner._clean_plan_response(content)
        assert "extra trailing text" not in result
        assert "More stuff" not in result
        assert "Shall I proceed" in result
