from pydantic_settings import BaseSettings as PydanticBaseSettings


class BaseSettings(PydanticBaseSettings):
    """
    Configuración base común. Los paquetes que la usen deben heredar de
    esta clase y añadir su propia configuración específica.

    Las variables se leen del entorno del proceso.
    """

    log_level: str = "INFO"
