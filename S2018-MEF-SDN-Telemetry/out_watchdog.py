import time, os
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from kafka import KafkaProducer

kafka_server = "192.168.149.128:9092"
topic = "raw.log.localtest"
path_to_watch = "controller/out"



class Handler(PatternMatchingEventHandler):
    def __init__(self, producer, topic):
        super(Handler, self).__init__()
        
        self.producer = producer
        self.topic = topic
        
    def publish_metrics(self, path):
        with open(path, 'r+') as file:
            metrics = file.read().strip()
            file.seek(0)
            file.truncate()
            
        if metrics:
            self.producer.send(self.topic, metrics)

    def on_created(self, event):
        if not event.is_directory:
            self.publish_metrics(event.src_path)
        
    def on_modified(self, event):
        if not event.is_directory:
            self.publish_metrics(event.src_path)
        
if __name__ == '__main__':            
    try: # to create the output directory if it does not exist
        os.mkdir(path_to_watch)
    except OSError:
        pass

    producer = producer = KafkaProducer(bootstrap_servers=kafka_server)

    observer = Observer()
    observer.schedule(Handler(producer, topic), path=path_to_watch)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        
    observer.join()
    
    time.sleep(2000)
    producer.close()

