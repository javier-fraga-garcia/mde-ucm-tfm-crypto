from abc import ABC, abstractmethod
from shared.schemas import KafkaEnvelope


class Producer(ABC):
    """Interfaz para productores de eventos.

    Define el contrato que deben implementar los productores encargados
    de publicar mensajes en un sistema de mensajería, como Kafka.
    """

    @abstractmethod
    async def publish(self, envelope: KafkaEnvelope) -> None:
        """Publica un evento en el sistema de mensajería.

        Args:
            envelope: Evento encapsulado que contiene los metadatos y
                el payload que se enviará al broker.

        Raises:
            NotImplementedError: Si la implementación concreta no define
                este método.
        """
        ...

    @abstractmethod
    async def connect(self) -> None:
        """Inicializa la conexión con el sistema de mensajería.

        Este método debe preparar todos los recursos necesarios para
        permitir la publicación de eventos.

        Raises:
            NotImplementedError: Si la implementación concreta no define
                este método.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Libera los recursos asociados al productor.

        Este método debe cerrar de forma ordenada la conexión con el
        sistema de mensajería y liberar cualquier recurso utilizado.

        Raises:
            NotImplementedError: Si la implementación concreta no define
                este método.
        """
        ...
