# Paquete shared

## 1. Propósito del módulo

El paquete shared constituye la capa de soporte transversal del proyecto. Su función no es aportar lógica de negocio ni ejecutar procesos de ingestión o procesamiento, sino proporcionar un conjunto de componentes reutilizables que garantizan coherencia técnica, consistencia de datos y facilidad de mantenimiento en todo el monorepo.

Desde una perspectiva de arquitectura de datos, este paquete cumple un papel de infraestructura compartida. Define elementos que deben ser homogéneos entre los distintos módulos del sistema para evitar duplicaciones, reducir errores y favorecer la interoperabilidad entre componentes.

En el contexto del TFM, este paquete resulta especialmente relevante porque representa una decisión de diseño orientada a la modularidad: en lugar de repetir definiciones de configuración, logging o contratos de mensajes en cada servicio, dichas capacidades se centralizan en un único componente reutilizable.

## 2. Contexto dentro del proyecto

El repositorio se organiza como un monorepo con módulos independientes que colaboran para construir un pipeline de datos orientado a streaming. En ese entorno, shared actúa como capa transversal que ofrece mecanismos comunes a todos los servicios.

Su presencia es especialmente útil por tres motivos:

1. reduce la duplicación de código entre módulos
2. evita divergencias en la forma de representar datos o de registrar eventos
3. facilita la evolución del sistema cuando se incorporen nuevos componentes o nuevas fases del pipeline

En términos prácticos, shared permite que el proyecto no dependa de implementaciones ad hoc o de definiciones dispersas, sino de una base común y explícita.

## 3. Diseño arquitectónico

El diseño de este paquete se apoya en un principio claro: la reutilización de infraestructura mediante abstracciones simples, estables y fáciles de integrar.

### 3.1 Separación de responsabilidades

El paquete no intenta resolver problemas de negocio ni de dominio. Su responsabilidad es más técnica y transversal. Por ello, se organiza en submódulos que atienden a necesidades específicas:

- configuración común
- logging estandarizado
- esquemas compartidos para mensajes del sistema

Esta separación permite que cada módulo del proyecto pueda reutilizar estas capacidades sin necesidad de implementar soluciones propias o inconsistentes.

### 3.2 Diseño orientado a la interoperabilidad

Una de las decisiones técnicas más importantes del paquete es que los distintos componentes del sistema comparten los mismos modelos de datos y la misma infraestructura de observabilidad. Esto es esencial en pipelines de datos, donde la interoperabilidad entre servicios y la trazabilidad de los eventos son factores críticos.

La existencia de un envelope común, por ejemplo, garantiza que cualquier componente que consuma mensajes del flujo pueda trabajar con una estructura estable y predecible.

### 3.3 Simplicidad frente a complejidad innecesaria

El paquete no incorpora mecanismos excesivamente sofisticados. Su diseño es deliberadamente simple, con el objetivo de mantener el sistema comprensible y alineado con el alcance del TFM. En particular, se ha priorizado la claridad y la facilidad de mantenimiento sobre la incorporación de capas adicionales de infraestructura que, en este contexto, no aportan suficiente valor.

Este criterio se aplica de forma explícita al propio contenido del paquete: solo se incorpora a shared aquello que dos o más servicios necesitan con el mismo significado. La configuración base, por ejemplo, se ha mantenido deliberadamente mínima, y solo crecerá cuando surja una necesidad real y compartida por varios servicios, en lugar de anticipar campos especulativos.

## 4. Componentes del paquete

### 4.1 Configuración base: config/settings.py

El módulo de configuración define una clase base, `BaseSettings`, para cargar variables de entorno de forma estandarizada. Esta abstracción sirve de punto de partida para todos los servicios que necesiten leer configuraciones desde el entorno.

En su estado actual, `BaseSettings` define un único campo común a todos los servicios: el nivel de logging (`log_level`). Esta minimalidad es intencionada: la decisión de utilizar una clase base común responde a varios objetivos:

- evitar que cada paquete implemente su propia lógica de lectura de entorno
- garantizar que los valores de configuración se traten de forma uniforme
- facilitar el despliegue en entornos distintos, como desarrollo, pruebas o producción

Aun así, solo se añaden a esta clase campos que sean genuinamente compartidos por varios servicios con el mismo significado. Configuraciones específicas de un único paquete, como las credenciales de Kafka o la URL del WebSocket de Binance, se definen en la clase de settings propia de cada servicio, que hereda de esta base.

### 4.2 Logging compartido: logging/config.py

El módulo de logging proporciona una configuración homogénea para todos los servicios. La implementación genera logs estructurados en formato JSON y los envía a stdout, lo que facilita su consumo en entornos de contenedores y sistemas de observabilidad.

Este diseño tiene varias ventajas:

- homogeneiza el formato de los registros entre módulos
- facilita la integración con herramientas de observabilidad
- mejora la trazabilidad de eventos dentro del sistema

La configuración se realiza de forma centralizada mediante la función configure_logging, que permite inicializar el sistema de logging con un nombre de servicio y un nivel de severidad concretos. Esta función está pensada para invocarse una única vez, en el entry point de cada proceso; el resto de módulos obtienen su logger mediante el patrón estándar `logging.getLogger(__name__)`, heredando la configuración a través del root logger.

### 4.3 Esquemas compartidos: schemas/kafka_envelope.py

El módulo de esquemas define el contrato de datos utilizado para los eventos que se publican en Kafka. El componente central es KafkaEnvelope, que encapsula el payload recibido desde Binance junto con metadatos esenciales para el procesamiento posterior.

La estructura incorpora:

- un timestamp de ingestión
- el símbolo del activo financiero
- el tipo de stream del que procede el evento
- el payload crudo original, preservado tal como llegó del proveedor

Este diseño tiene un propósito importante: permitir que el sistema preserve el contexto del evento sin depender de la estructura original del proveedor externo. En otras palabras, el envelope actúa como una capa de normalización entre la fuente de datos y las capas posteriores del pipeline.

## 5. Justificación técnica de las decisiones de diseño

### 5.1 Por qué centralizar la infraestructura compartida

La decisión de centralizar configuraciones, logging y modelos de mensajes en un paquete shared responde a un principio de ingeniería de software muy extendido: evitar la duplicación y favorecer la consistencia. En un sistema distribuido o en un pipeline de datos, la uniformidad de estas capacidades es una ventaja operativa clara.

Esto es especialmente relevante en un proyecto académico como este, donde la prioridad no es únicamente implementar una solución funcional, sino demostrar que se aplican buenos principios de diseño.

### 5.2 Por qué usar Pydantic para los contratos de datos

El paquete hace uso de Pydantic para definir y validar la estructura de los mensajes. Esta opción es adecuada para el alcance del proyecto porque permite:

- validar automáticamente la forma de los datos
- detectar errores de entrada de forma temprana
- mantener una definición clara y expresiva del contrato interno

En este proyecto, Pydantic se ha elegido como mecanismo de validación porque el sistema no necesita niveles adicionales de complejidad, como un Schema Registry, para cubrir sus requisitos iniciales. Para un alcance de TFM, esta solución ofrece un equilibrio adecuado entre rigor técnico y simplicidad.

### 5.3 Por qué el logging está pensado para ser uniforme

El logging homogéneo es una decisión importante porque mejora la observabilidad del sistema. Cuando todos los módulos generan logs con el mismo formato y los mismos campos, resulta mucho más sencillo diagnosticar problemas, correlacionar eventos y entender el comportamiento del pipeline en entornos reales.

## 6. Flujo de uso dentro del sistema

El paquete shared no suele actuar como un componente activo de procesamiento, sino como una capa de soporte que se reutiliza en los demás módulos. Su uso se puede describir de la siguiente manera:

1. un servicio del proyecto importa la configuración base o el sistema de logging compartido
2. se inicializa la infraestructura del módulo correspondiente
3. los datos que circulan entre componentes se estructuran mediante el envelope común
4. los logs y los eventos se registran con un formato uniforme

En este sentido, shared no es un elemento visible para el usuario final, sino una infraestructura que sostiene la calidad y la consistencia del sistema.

## 7. Ejemplos de uso

### 7.1 Configuración

Los módulos pueden heredar de la clase base de configuración para incorporar variables de entorno de forma estandarizada, añadiendo únicamente los campos propios de cada servicio:

```python
from shared.config import BaseSettings

class IngestionSettings(BaseSettings):
    ws_base_url: str
    kafka_bootstrap_servers: str
    kafka_topic: str
```

### 7.2 Logging

El siguiente patrón es el que se utiliza para inicializar el logging en los servicios del proyecto:

```python
from shared.logging import configure_logging

configure_logging("ingestion", level="INFO")
```

### 7.3 Esquema de mensaje

Un ejemplo típico de uso del envelope compartido es el siguiente:

```python
from shared.schemas import KafkaEnvelope, StreamType

envelope = KafkaEnvelope(
    symbol="BTCUSDT",
    stream_type=StreamType.AGG_TRADE,
    raw_payload='{"event": "trade"}',
)
```

## 8. Pruebas

El paquete shared incluye pruebas que verifican el comportamiento de sus componentes principales. Estas pruebas cubren:

- la configuración del logging, incluyendo el rechazo de niveles inválidos y la ausencia de handlers duplicados ante llamadas repetidas
- la generación de logs en formato JSON con los campos esperados
- la validación del esquema KafkaEnvelope, incluyendo el rechazo de tipos de stream no válidos
- la correcta gestión del timestamp de ingestión, verificando que se genera un valor nuevo en cada instancia

La cobertura de pruebas alcanza el 100% en los módulos `logging` y `schemas`. El módulo `config/settings.py` queda fuera del alcance de las pruebas de forma deliberada: en su estado actual, `BaseSettings` no contiene lógica propia más allá de heredar el comportamiento de `pydantic-settings`, por lo que un test ahí verificaría el funcionamiento de la librería subyacente y no código propio del proyecto.

Las pruebas pueden ejecutarse con:

```bash
uv run --package shared pytest packages/shared/tests --cov=shared --cov-report=term-missing
```

## 9. Limitaciones actuales y extensiones naturales

El diseño actual es simple y suficiente para cubrir el alcance del TFM, pero presenta algunas limitaciones naturales:

- la capa de configuración es todavía bastante básica
- el sistema de logging está orientado a un uso generalista y no incorpora métricas de negocio ni trazabilidad avanzada
- el esquema de mensajes es sencillo y está pensado para un entorno con un número limitado de productores

Como extensiones futuras, podrían incorporarse:

- soporte para perfiles de configuración más complejos
- integración con sistemas de observabilidad externos
- evolución del modelo de mensajes hacia un esquema más formal si el proyecto crece

## 10. Conclusión

El paquete shared desempeña un papel esencial en el proyecto porque convierte la infraestructura común en un activo reutilizable y bien definido. A través de la configuración compartida, el logging uniforme y los esquemas de mensajes estructurados, este módulo aporta coherencia técnica al sistema y permite que los demás componentes se centren en sus responsabilidades específicas.

Desde la perspectiva del TFM, este paquete ilustra una decisión arquitectónica importante: cuando un sistema está creciendo, la centralización de componentes transversales no solo mejora la mantenibilidad, sino que también refuerza la calidad del diseño global.
