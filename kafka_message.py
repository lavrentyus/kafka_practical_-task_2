import json
from datetime import datetime
from typing import Optional, Dict, Any

#Класс для представления сообщения в Kafka.
class KafkaMessage:
    
    #Инициализация сообщения Kafka.
    def __init__(self, message_id: int, content: str, producer_id: str = "unknown", 
                 metadata: Optional[Dict[str, Any]] = None):

        self.message_id = message_id        # уникальный идентификатор для отслеживания
        self.content = content              # полезная нагрузка сообщения
        self.producer_id = producer_id      # источник сообщения для диагностики
        self.timestamp = datetime.now().isoformat()  # автоматическая временная метка
        self.metadata = metadata or {}       # расширяемые метаданные

    def to_dict(self) -> Dict[str, Any]:
        return {
            'message_id': self.message_id,
            'content': self.content,
            'producer_id': self.producer_id,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }
    
    def serialize(self) -> str:

        try:
            return json.dumps(self.to_dict(), ensure_ascii=False)
        except Exception as e:
            print(f"Ошибка сериализации сообщения: {e}")
            raise
    
    @classmethod
    def deserialize(cls, json_string: str) -> 'KafkaMessage':

        try:
            data = json.loads(json_string)
            message = cls(
                message_id=data['message_id'],
                content=data['content'],
                producer_id=data.get('producer_id', 'unknown'),
                metadata=data.get('metadata', {})
            )
            # Сохраняем оригинальный timestamp из сообщения
            message.timestamp = data.get('timestamp', message.timestamp)
            return message
        except Exception as e:
            print(f"Ошибка десериализации сообщения: {e}")
            raise
    
    def __str__(self) -> str:
       
        return (f"KafkaMessage(id={self.message_id}, "
                f"content='{self.content}', "
                f"producer='{self.producer_id}', "
                f"timestamp='{self.timestamp}')")
    
    def __repr__(self) -> str:
        return self.__str__()