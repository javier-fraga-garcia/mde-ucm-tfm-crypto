from aiokafka import AIOKafkaProducer
from ingestion.producers import Producer
from shared.schemas import KafkaEnvelope


class KafkaProducer(Producer):
    """Productor de eventos basado en Kafka.

    Implementa la interfaz ``Producer`` utilizando ``AIOKafkaProducer``
    para publicar eventos de forma asíncrona en un tópico de Kafka.

    Attributes:
        bootstrap_servers: Dirección de los brokers de Kafka.
        topic: Nombre del tópico donde se publicarán los eventos.
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        """Inicializa el productor de Kafka.

        Args:
            bootstrap_servers: Dirección de los brokers de Kafka.
            topic: Nombre del tópico de destino.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer = None

    async def connect(self) -> None:
        """Establece la conexión con el clúster de Kafka.

        Inicializa la instancia de ``AIOKafkaProducer`` y la deja lista
        para publicar mensajes.
        """
        self._producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self._producer.start()

    async def close(self) -> None:
        """Cierra la conexión con Kafka.

        Libera los recursos asociados al productor.
        """
        await self._producer.stop()

    async def publish(self, envelope: KafkaEnvelope) -> None:
        """Publica un evento en el tópico configurado.

        El símbolo del activo se utiliza como clave del mensaje para
        preservar el orden de los eventos de un mismo activo dentro de
        una partición.

        Args:
            envelope: Evento que se enviará a Kafka.
        """
        await self._producer.send_and_wait(
            topic=self.topic,
            key=envelope.symbol.encode("utf-8"),
            value=envelope.model_dump_json().encode("utf-8"),
        )
