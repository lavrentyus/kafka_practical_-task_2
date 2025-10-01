import time
from confluent_kafka import Consumer
from kafka_message import KafkaMessage

# Конфигурация для одиночного обработчика сообщений
kafka_config = {
    'bootstrap.servers': 'kafka-0:9092,kafka-1:9092,kafka-2:9092',  # Список брокеров
    'group.id': 'single-message-group',  # уидентификатор группы
    'enable.auto.commit': True,          # автоматическое подтверждение
    'auto.offset.reset': 'earliest',     # чтение с начала топика
    'fetch.min.bytes': 1                # минимальный объем данных
}

# Создаем консюмер
consumer = Consumer(kafka_config)

# Подписываемся на топик
topic_name = 'andrew-topic'
consumer.subscribe([topic_name])

#Обработка сообщений
def process_one_message(message):
    
    try:
        # Получаем ключ сообщения
        if message.key():
            key = message.key().decode('utf-8')
        else:
            key = None
        
        # Получаем сырые данные сообщения
        raw_message_content = message.value().decode('utf-8')
        
        # Десериализуем сообщение
        try:
            kafka_msg = KafkaMessage.deserialize(raw_message_content)
            
            # Выводим полученное сообщение на консоль
            print(f"ПОЛУЧИЛ: {kafka_msg}")
            print(f"Сырые данные: {raw_message_content}")
            
        except Exception as deserialize_error:
            print(f"Ошибка десериализации сообщения: {deserialize_error}")
            print(f"Сырые данные: {raw_message_content}")
            return False
        
        # Получаем информацию о партиции и оффсете
        partition = message.partition()
        offset = message.offset()
        
        # Выводим доп. информацию
        print(f"Доп. информация: key={key}, partition={partition}, offset={offset}")
        
        time.sleep(0.5)
        
        print(f"Сообщение {key} (ID: {kafka_msg.message_id}) обработано успешно!")
        return True
        
    except Exception as error:
        print(f"Ошибка при обработке сообщения: {error}")
        return False

if __name__ == "__main__":
    print("Запускаю консюмер для одиночных сообщений...")
    
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            
            if message is None:
                print("Нет новых сообщений, жду...")
                continue
            
            if message.error():
                print(f"Ошибка консюмера: {message.error()}")
                continue
            
            success = process_one_message(message)
            
            if success:
                print("Сообщение обработано, оффсет автоматически закоммичен")
            else:
                print("Ошибка обработки, но продолжаем работать")
            
            print("---")
    
    except KeyboardInterrupt:
        print("Получил сигнал остановки...")
    
    except Exception as error:
        print(f"Произошла ошибка: {error}")
    
    finally:
        print("Закрываю консюмер...")
        consumer.close()
        print("Консюмер для одиночных сообщений остановлен")