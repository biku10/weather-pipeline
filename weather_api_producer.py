import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITIES = ['New York', 'London', 'Tokyo', 'Mumbai', 'São Paulo']
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'weather_raw'
INTERVAL = 300

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather(city):
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {'q': city, 'appid': API_KEY, 'units': 'metric'}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'city': city,
        'temp_c': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'weather': data['weather'][0]['main'],
        'description': data['weather'][0]['description']
    }

def main():
    while True:
        for city in CITIES:
            try:
                weather_data = fetch_weather(city)
                producer.send(TOPIC_NAME, weather_data)
                print(f"Sent data for {city}")
            except Exception as e:
                print(f"Error for {city}: {e}")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()