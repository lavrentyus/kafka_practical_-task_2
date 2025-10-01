# Kafka Producer-Consumer Application

Приложение демонстрирует работу с Apache Kafka, используется библиотека `confluent-kafka`.

## Состав приложения

- **1 продюсер (Producer)** - отправляет сообщения с гарантией доставки
- **2 типа консюмеров:**
  - SingleMessageConsumer - обрабатывает по одному сообщению
  - BatchMessageConsumer - обрабатывает пакеты сообщений

## Архитектура системы

### Kafka Cluster
- 3 брокера Kafka (kafka-0, kafka-1, kafka-2)
- KRaft режим (без ZooKeeper)
- Репликация: 2
- Партиции: 3

## Описание классов

### 1. KafkaMessage (kafka_message.py)

Основной класс для структурированного представления сообщений Kafka.

**Назначение:**
- Унифицированное представление сообщений в системе
- Автоматическая сериализация/десериализация в JSON
- Встроенная поддержка метаданных и временных меток

**Ключевые атрибуты:**
- `message_id` (int) - уникальный идентификатор сообщения
- `content` (str) - основное содержимое сообщения  
- `producer_id` (str) - идентификатор отправителя
- `timestamp` (str) - временная метка в ISO формате
- `metadata` (dict) - дополнительные метаданные

**Основные методы:**
- `serialize()` - преобразование в JSON строку для отправки
- `deserialize()` - восстановление объекта из JSON строки
- `to_dict()` - преобразование в словарь Python

### 2. Producer (producer.py)

Надежный отправитель сообщений с гарантией "At Least Once".

**Ключевые параметры конфигурации:**
- `acks='all'` - ждет подтверждения от всех ISR реплик (максимальная надежность)
- `retries=10` - до 10 повторных попыток при сбоях
- `enable.idempotence=True` - предотвращение дубликатов при повторах
- `delivery.timeout.ms=120000` - общий таймаут доставки (2 минуты)
- `max.in.flight.requests.per.connection=1` - гарантия порядка сообщений

**Принцип работы:**
1. Создает структурированное сообщение через KafkaMessage
2. Сериализует в JSON и отправляет асинхронно
3. Получает подтверждения через delivery callback
4. Выполняет flush() для гарантии доставки всех сообщений

### 3. SingleMessageConsumer (single_message_consumer.py)

Консюмер для поштучной обработки сообщений.

**Ключевые параметры конфигурации:**
- `group.id='single-message-group'` - уникальная группа потребителей
- `enable.auto.commit=True` - автоматическое подтверждение обработки
- `fetch.min.bytes=1` - возврат данных при любом объеме (минимальная задержка)

**Принцип работы:**
1. Получает одно сообщение через `consumer.poll()`
2. Десериализует JSON в объект KafkaMessage
3. Обрабатывает сообщение и выводит информацию
4. Автоматически подтверждает обработку

### 4. BatchMessageConsumer (batch_message_consumer.py)

Консюмер для пакетной обработки сообщений.

**Ключевые параметры конфигурации:**
- `group.id='batch-message-group'` - отдельная группа от одиночного консюмера
- `enable.auto.commit=False` - ручное подтверждение после обработки пакета
- `fetch.min.bytes=1024` - ожидание накопления данных (1KB)

**Принцип работы:**
1. Накапливает сообщения в буфере до достижения MIN_BATCH_SIZE (10 сообщений)
2. Обрабатывает весь пакет как единое целое
3. Вручную подтверждает обработку всего пакета через `consumer.commit()`
4. Обеспечивает "exactly-once" обработку пакетов

### Приложения

#### Producer (confluent-kafka)
- Отправляет сообщения асинхронно в топик `andrew-topic`
- Использует модель push с delivery callbacks
- **ГАРАНТИИ ДОСТАВКИ "AT LEAST ONCE":**
  - `acks='all'` - ждет подтверждения от всех ISR реплик
  - `retries=10` - до 10 повторов при ошибках
  - `enable.idempotence=True` - предотвращение дубликатов
  - `delivery.timeout.ms=120000` - общий таймаут доставки
- Сериализация/десериализация через класс KafkaMessage (JSON)
- Поддержка сжатия lz4 и буферизации
- Запускается в 2 экземплярах

#### SingleMessageConsumer (confluent-kafka)
- Читает и обрабатывает по одному сообщению за раз
- Автоматический коммит оффсетов (enable.auto.commit=True)
- Уникальный group_id: `single-message-group`
- Настройки: fetch.min.bytes=1, fetch.max.wait.ms=500
- Запускается в 2 экземплярах

#### BatchMessageConsumer (confluent-kafka)
- Читает минимум 10 сообщений за один poll()
- Ручной коммит оффсетов после обработки всей пачки
- Уникальный group_id: `batch-message-group`
- Настройки: fetch.min.bytes=1024, fetch.max.wait.ms=5000
- Буферизация сообщений для накопления пачек
- Запускается в 2 экземплярах

## Инструкция по запуску

### Шаг 1: Запуск Kafka кластера
```bash
# Запуск 3-х брокеров Kafka в фоновом режиме
docker compose up -d kafka-0 kafka-1 kafka-2 ui

# Проверка состояния брокеров
docker compose ps
```

### Шаг 2: Создание топика
```bash
docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic andrew-topic --partitions 3 --replication-factor 2 --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092


### Шаг 3: Запуск приложений
```bash
# Запуск всех компонентов системы
docker compose up -d

# Проверка запущенных контейнеров
docker compose ps
```


## Проверка работы приложения

### 1. Проверка создания топика
```bash
# Список всех топиков
docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka-0:9092

# Подробная информация о топике andrew-topic
docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --describe --topic andrew-topic --bootstrap-server kafka-0:9092
```
**Ожидаемый результат:** Топик `andrew-topic` с 3 партициями и фактором репликации 2.

### 2. Проверка отправки сообщений продюсером
```bash
docker compose logs producer
```
