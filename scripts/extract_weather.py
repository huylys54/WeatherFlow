import requests
from dotenv import load_dotenv
import os
import json
import datetime
import time
import argparse


load_dotenv()

def extract_weather(cities):
    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise ValueError("API_KEY not found in .env")
    weather_data = []
    for city in cities:
        url =  f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            weather_data.append({
                "city": data["name"],
                "temperature_k": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather_desc": data["weather"][0]["description"],
                "timestamp": data["dt"],
                "pressure": data["main"]["pressure"],
                "wind_speed": data["wind"]["speed"]
            })
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city}: {e}")
        time.sleep(1)
    return weather_data

def save_to_json(data, file_name):
    with open(file_name, 'w') as f:
        json.dump(data, f, indent=2)


def main(params):
    weather_data = extract_weather(params.cities)
    if weather_data:
        filename = f"raw_weather_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        save_to_json(weather_data, filename)
        print(f"Weather data saved to {filename}")
    else:
        print("No data extracted.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract weather data from OpenWeatherMap API.')
    parser.add_argument('--cities', nargs='+', default=["London", "New York", "Tokyo", "Hanoi"],
                       help='List of cities to fetch weather data')

    args = parser.parse_args()
    
    main(args)