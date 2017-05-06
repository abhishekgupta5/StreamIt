# producer.py

import time
import cv2
from kafka import SimpleProducer, KafkaClient

#Connecting to kafka
kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

#Assign a topic
topic = 'my-project'

def video_emitter(video):
    #Open the video
    video = cv2.VideoCapture(video)
    print(' Emitting...')

    #Read the file
    while (video.isOpened):
        #Read the image in each frame
        success, image = video.read()
        #Check if the file has read to the end
        if not success:
            print('failed to read video')
            break
        #Convert the image png
        ret, jpeg = cv2.imencode('.png', image)
        #Convert the image to bytes and send to kafka
        producer.send_messages(topic, jpeg.tobytes())
        #To reduce CPU usage create sleep time of 0.2sec
        time.sleep(0.2)
    #Clear the capture
    video.release()
    print('Done emitting')

if __name__ == '__main__':
    video_emitter('video.mp4')
