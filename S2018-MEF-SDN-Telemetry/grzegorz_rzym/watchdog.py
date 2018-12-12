import time, os, glob
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from kafka import KafkaProducer

kafka_server = "149.156.114.150:9092"
topic = "raw.log.localtest"
path_to_watch = "controller/out"



class Handler(PatternMatchingEventHandler):
    def __init__(self, producer, topic):
        super(Handler, self).__init__()
        
        self.producer = producer
        self.topic = topic
        try:
            os.mkdir('watchdog_stats')
        except OSError:
            pass
        self.stats_path = 'watchdog_stats/watchdog-stats.txt'

    def rename(dir, pattern, titlePattern):
        for pathAndFilename in glob.iglob(os.path.join(dir, pattern)):
            title, ext = os.path.splitext(os.path.basename(pathAndFilename))
            os.rename(pathAndFilename,
                      os.path.join(dir, titlePattern % title + ext))

    def publish_metrics(self, path):
        new_path = path+".obsolete"
        try:
            os.rename(path,new_path)
            with open(new_path, 'r+') as file:
                metrics = file.read().strip()

            if metrics:
                self.producer.send(self.topic, metrics)
                with open(self.stats_path, 'a') as file:
                    file.write(str((time.time())) + ' ' + str(len(metrics)) + ' ' + str(os.path.getsize(new_path)) + '\n')
                try:
                    os.remove(new_path)
                except OSError as e:  ## if failed, report it back to the user ##
                    print("Error: %s - %s." % (e.filename, e.strerror))
        except:
            pass

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

    producer = KafkaProducer(bootstrap_servers=kafka_server)

    observer = Observer()
    observer.schedule(Handler(producer, topic), path=path_to_watch)
    observer.start()
    
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        observer.stop()
        
    observer.join()
    
    time.sleep(2000)
    producer.close()

