from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    service_name: str
    rabbitmq_url: str = "amqp://saga:saga@localhost:5672/"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings(service_name: str) -> CommonSettings:
    # service_name is supplied by each deployable, while environment variables
    # provide deployment-specific values (12-factor configuration).
    return CommonSettings(service_name=service_name)
