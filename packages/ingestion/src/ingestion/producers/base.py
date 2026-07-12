from abc import ABC, abstractmethod
from shared.schemas import KafkaEnvelope


class Producer(ABC):
    """Interfaz para productores de eventos.

    Define el contrato que deben implementar los productores encargados
    de publicar mensajes en un sistema de mensajería, como Kafka.
    """

    @abstractmethod
    async def publish(self, envelop: KafkaEnvelope) -> None:
        """Publica un evento en el sistema de mensajería.

        Args:
            envelope: Evento encapsulado que contiene los metadatos y
                el payload que se enviará al broker.

        Raises:
            NotImplementedError: Si la implementación concreta no define
                este método.
        """
        pass
