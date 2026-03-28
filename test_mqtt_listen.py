import paho.mqtt.client as mqtt
import time

msg_count = 0

def on_message(client, userdata, msg):
    global msg_count
    msg_count += 1
    print(f"RAW MQTT: {msg.topic} | size: {len(msg.payload)}")

client = mqtt.Client()
client.on_message = on_message
client.connect("127.0.0.1", 1883, 60)
client.loop_start()

print("Listening for 5 seconds...")
time.sleep(5)
client.loop_stop()
print(f"Total messages received: {msg_count}")
