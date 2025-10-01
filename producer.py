import time
from confluent_kafka import Producer
from kafka_message import KafkaMessage

# Настройки для подключения к Kafka
kafka_config = {
    'bootstrap.servers': 'kafka-0:9092,kafka-1:9092,kafka-2:9092',  # Список брокеров
     'acks': 'all',  # подтверждения от всех реплик
    'retries': 10,  # Количество повторных попыток
    'retry.backoff.ms': 500,  # Интервал между повторными попытками в миллисекундах
    'enable.idempotence': True,  # идемпотентность
    'max.in.flight.requests.per.connection': 1,  # максимум 1 запрос
    'delivery.timeout.ms': 120000,  # Общий таймаут
    'request.timeout.ms': 30000,    # таймаут одного запроса к брокеру
    'batch.size': 16384,     # размер сообщений
    'linger.ms': 10         # Время ожидания перед отправкой
}

# Создаем продюсер
producer = Producer(kafka_config)

# Название топика
topic_name = 'andrew-topic'

#функция для обработки подтверждений доставки сообщений
def delivery_callback(err, msg):

    if err is not None:
        print(f"ОШИБКА ДОСТАВКИ: {err}")
        print(f"Сообщение НЕ доставлено в топик {msg.topic()}")
    else:
        print(f"ПОДТВЕРЖДЕНИЕ ДОСТАВКИ:")
        print(f"Топик: {msg.topic()}")
        print(f"Партиция: {msg.partition()}")
        print(f"Оффсет: {msg.offset()}")
        print(f"Ключ: {msg.key().decode('utf-8') if msg.key() else None}")

#отправка сообщений
def send_message_to_kafka(message_number):
    
    # создаем ключ
    key = f"key-{message_number}"
    
    try:
        # Создаем объект сообщения
        message = KafkaMessage(
            message_id=message_number,
            content=f'Сообщение номер {message_number}',
            producer_id='reliable-producer',
            metadata={
                'version': '1.0',
                'format': 'json',
                'sequence': message_number,
                'delivery_guarantee': 'at_least_once'
            }
        )
        
        # Сериализуем сообщение в JSON
        serialized_message = message.serialize()
        
        # Выводим отправляемое сообщение на консоль
        print(f"отправка : {message}")
        
        # отправляем сериализованное сообщение в Kafka
        producer.produce(
            topic=topic_name,
            key=key,
            value=serialized_message,
            callback=delivery_callback 
        )

        # ожидание обработки
        producer.poll(0.1)
        
        print(f"Сообщение {message_number} отправлено (ждем подтверждения...)")
        return True
        
    except Exception as error:
        print(f"Ошибка сериализации или отправки сообщения {message_number}: {error}")
        return False

if __name__ == "__main__":
    print("Запускаю продюсер...")
    print()
    
    total_messages = 60
    failed_messages = []

    for i in range(total_messages):
        print(f"\nОтправляю сообщение {i+1}/{total_messages}")
        
        success = send_message_to_kafka(i)
        
        if success:
            print(f"Сообщение {i} успешно поставлено в очередь")
        else:
            print(f"Не удалось отправить сообщение {i}")
            failed_messages.append(i)
        
        # Пауза между сообщениями
        time.sleep(1)

    print(f"\nОжидание подтверждения доставки всех сообщений.")
    
    remaining_messages = producer.flush(timeout=130.0)  # Ждем до 130 секунд
    
    if remaining_messages == 0:
        print("СООБЩЕНИЯ ДОСТАВЛЕНЫ!")
    else:
        print(f"{remaining_messages} сообщений не были доставлены в срок")
    
    if failed_messages:
        print(f"Сообщения с ошибками отправки: {failed_messages}")
    else:
        print("Все сообщения были успешно отправлены")
    
    print("\nПродюсер завершен.")