import time
from confluent_kafka import Consumer
from kafka_message import KafkaMessage

# Конфигурация для пакетного обработчика сообщений
kafka_config = {
    'bootstrap.servers': 'kafka-0:9092,kafka-1:9092,kafka-2:9092',  # список брокеров
    'group.id': 'batch-message-group',  # уникальный ID группы
    'enable.auto.commit': False,        # отключаем автокоммит
    'auto.offset.reset': 'earliest',    # начинать чтение с начала
    'fetch.min.bytes': 1024           # минимальный объем данных для возврата
    
}

# Создаем консюмер
consumer = Consumer(kafka_config)

# Подписываемся на топик
topic_name = 'andrew-topic'
consumer.subscribe([topic_name])

# Количество в пачке
MIN_BATCH_SIZE = 10

#Обработка пакета сообщений
def process_batch_of_messages(messages):
    
    print(f"Начало обработки пакета из {len(messages)} сообщений")
    
    processed_count = 0
    deserialization_errors = 0
    
    # Обрабатываем каждое сообщение в пакете
    for i, message in enumerate(messages, 1):
        try:
            if message.key():
                key = message.key().decode('utf-8')
            else:
                key = None
            
            # Получаем сообщения
            raw_message_content = message.value().decode('utf-8')
            
            # Десериализуем сообщение
            try:
                kafka_msg = KafkaMessage.deserialize(raw_message_content)
                
                print(f"[{i}/{len(messages)}] ПОЛУЧИЛ: {kafka_msg}")
                
            except Exception as deserialize_error:
                print(f"[{i}/{len(messages)}] Ошибка десериализации: {deserialize_error}")
                print(f"Сырые данные: {raw_message_content}")
                deserialization_errors += 1
                continue
            
            # Получаем доп. информацию о партиции и оффсете
            partition = message.partition()
            offset = message.offset()
            
            print(f"доп. информация: key={key}, partition={partition}, offset={offset}")
            
            time.sleep(0.1)
            
            processed_count += 1
            
        except Exception as error:
            print(f"[{i}/{len(messages)}] Общая ошибка при обработке сообщения: {error}")
            # Продолжаем обработку остальных сообщений в пакете
            continue

    print(f"Успешно обработано {processed_count} из {len(messages)} сообщений в пакете")
    if deserialization_errors > 0:
        print(f"Ошибок десериализации: {deserialization_errors}")
    
    return processed_count > 0  # Возвращаем True если хотя бы одно сообщение обработано

#Коммит оффсетов
def commit_offsets_manually(messages):
    
    try:
        consumer.commit(asynchronous=False)
        print("Оффсеты успешно закоммичены вручную!")
        return True
        
    except Exception as error:
        print(f"Ошибка при коммите оффсетов: {error}")
        return False

if __name__ == "__main__":
    print("Запускаю консюмер для пакетов сообщений...")
    print(f"Минимум {MIN_BATCH_SIZE} сообщений за раз...")
    
    message_batch = []
    
    try:
        while True:
            message = consumer.poll(timeout=2.0)
            
            if message is None:
                if message_batch:
                    print(f"Таймаут ожидания. Обрабатываем {len(message_batch)} накопленных сообщений")
                    
                    success = process_batch_of_messages(message_batch)
                    
                    if success:
                        commit_success = commit_offsets_manually(message_batch)
                        if commit_success:
                            print("Пакет обработан и закоммичена!")
                        else:
                            print("Пакет обработан, но ошибка коммита!")
                    else:
                        print("Ошибки при обработке пакета, оффсеты НЕ коммичены")
                    
                    message_batch = []
                    print("---")
                
                else:
                    print("Нет новых сообщений, жду...")
                continue
            
            if message.error():
                print(f"Ошибка консюмера: {message.error()}")
                continue
            
            message_batch.append(message)
            print(f"Добавил сообщение в пакет. Сейчас в пакете: {len(message_batch)}")
            
            if len(message_batch) >= MIN_BATCH_SIZE:
                print(f"Набран пакет из {len(message_batch)} сообщений!")
                
                success = process_batch_of_messages(message_batch)
                
                if success:
                    commit_success = commit_offsets_manually(message_batch)
                    if commit_success:
                        print("Пакет обработан и закоммичен!")
                    else:
                        print("Пакет обработан, но ошибка коммита!")
                else:
                    print("Ошибки при обработке пакета, оффсеты НЕ коммичены")
                
                message_batch = []
                print("---")
            
            else:
                print(f"Жду еще сообщений... (нужно минимум {MIN_BATCH_SIZE}, есть {len(message_batch)})")
    
    except KeyboardInterrupt:
        print("Получил сигнал остановки...")
        
        if message_batch:
            print(f"Обрабатываю оставшиеся {len(message_batch)} сообщений перед остановкой")
            success = process_batch_of_messages(message_batch)
            if success:
                commit_offsets_manually(message_batch)
    
    except Exception as error:
        print(f"Произошла ошибка: {error}")
    
    finally:
        print("Закрываю консюмер...")
        consumer.close()
        print("Консюмер для пакетов сообщений остановлен")