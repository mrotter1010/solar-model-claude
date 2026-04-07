"""Orchestrator configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class OrchestratorConfig(BaseSettings):
    """Configuration for the solar orchestrator service.

    All fields map to environment variables by name (uppercased).
    Example: ``openai_api_key`` reads from ``OPENAI_API_KEY``.
    """

    openai_api_key: str = ""
    openai_model: str = "gpt-5"
    analysis_api_url: str = "http://localhost:8000"
    analysis_api_key: str | None = None
    system_prompt_path: str = "orchestrator/prompts/system_prompt.md"
    max_plan_steps: int = 10
    session_ttl_minutes: int = 60
    request_timeout: int = 120
    database_url: str = "postgresql://solar:solar@localhost:5432/solar_model"

    model_config = {"env_prefix": ""}
