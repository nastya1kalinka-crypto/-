import psycopg2
import time
import random
from datetime import datetime
import os

# Конфигурация подключения к базе данных PostgreSQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "weather_db"),
    "user": os.getenv("DB_USER", "weather_user"),
    "password": os.getenv("DB_PASSWORD", "weather_pass")
}

# Список городов России с координатами
RUSSIAN_CITIES = [
    {"name": "Москва", "lat": 55.7558, "lon": 37.6173, "alt": 156},
    {"name": "Санкт-Петербург", "lat": 59.9343, "lon": 30.3351, "alt": 3},
    {"name": "Новосибирск", "lat": 55.0084, "lon": 82.9357, "alt": 150},
    {"name": "Екатеринбург", "lat": 56.8389, "lon": 60.6057, "alt": 270},
    {"name": "Нижний Новгород", "lat": 56.3269, "lon": 44.0065, "alt": 200},
    {"name": "Казань", "lat": 55.7961, "lon": 49.1064, "alt": 116},
    {"name": "Челябинск", "lat": 55.1644, "lon": 61.4368, "alt": 220},
    {"name": "Омск", "lat": 54.9914, "lon": 73.3715, "alt": 90},
    {"name": "Самара", "lat": 53.1959, "lon": 50.1002, "alt": 135},
    {"name": "Ростов-на-Дону", "lat": 47.2224, "lon": 39.7189, "alt": 70},
    {"name": "Уфа", "lat": 54.7355, "lon": 55.9917, "alt": 160},
    {"name": "Красноярск", "lat": 56.0153, "lon": 92.8932, "alt": 140},
    {"name": "Воронеж", "lat": 51.6720, "lon": 39.1843, "alt": 154},
    {"name": "Пермь", "lat": 58.0105, "lon": 56.2294, "alt": 171},
    {"name": "Волгоград", "lat": 48.7194, "lon": 44.5018, "alt": 80},
    {"name": "Краснодар", "lat": 45.0355, "lon": 38.9753, "alt": 25},
    {"name": "Саратов", "lat": 51.5336, "lon": 46.0086, "alt": 50},
    {"name": "Тюмень", "lat": 57.1530, "lon": 65.5343, "alt": 102},
    {"name": "Тольятти", "lat": 53.5078, "lon": 49.4204, "alt": 90},
    {"name": "Ижевск", "lat": 56.8528, "lon": 53.2115, "alt": 158},
    {"name": "Барнаул", "lat": 53.3478, "lon": 83.7756, "alt": 180},
    {"name": "Ульяновск", "lat": 54.3167, "lon": 48.3667, "alt": 150},
    {"name": "Иркутск", "lat": 52.2864, "lon": 104.281, "alt": 440},
    {"name": "Хабаровск", "lat": 48.4802, "lon": 135.0719, "alt": 72},
    {"name": "Ярославль", "lat": 57.6266, "lon": 39.8938, "alt": 100},
    {"name": "Владивосток", "lat": 43.1155, "lon": 131.8855, "alt": 40},
    {"name": "Махачкала", "lat": 42.9831, "lon": 47.5047, "alt": 10},
    {"name": "Томск", "lat": 56.4977, "lon": 84.9744, "alt": 110},
    {"name": "Кемерово", "lat": 55.3547, "lon": 86.0878, "alt": 140},
    {"name": "Новокузнецк", "lat": 53.7600, "lon": 87.1214, "alt": 190}
]

# Настройки генерации данных
STATIONS_COUNT = 15  # Количество станций для создания
UPDATE_INTERVAL = 60  # Интервал обновления данных в секундах


def generate_station_name(city_name):
    prefixes = ["Метеостанция", "Станция наблюдения", "Пост", "Центр", "АМС"]
    suffixes = ["", " №1", " №2", " центральная"]
    return f"{random.choice(prefixes)} {city_name}{random.choice(suffixes)}"


def initialize_random_stations(conn):
    station_ids = []
    
    with conn.cursor() as cur:
        # Выбор случайного города из списка
        selected_cities = random.sample(RUSSIAN_CITIES, min(STATIONS_COUNT, len(RUSSIAN_CITIES)))
        
        for city in selected_cities:
            # Генерация имени станции
            station_name = generate_station_name(city["name"])
            
            # Проверка, существования станции с таким именем
            cur.execute(
                "SELECT station_id FROM weather_stations WHERE station_name = %s",
                (station_name,)
            )
            existing = cur.fetchone()
            
            if existing:
                # Если станция уже существует, используем её ID
                station_id = existing[0]
            else:
                # Создание новой станции
                cur.execute("""
                    INSERT INTO weather_stations 
                    (station_name, location, latitude, longitude, altitude)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING station_id
                """, (
                    station_name,
                    f"г. {city['name']}",
                    city["lat"],
                    city["lon"],
                    city["alt"]
                ))
                station_id = cur.fetchone()[0]
            
            station_ids.append(station_id)
    
    conn.commit()
    return station_ids


def generate_weather_data(station_id):
    current_hour = datetime.now().hour
    
    # Базовая температура для зимы
    base_temp = random.uniform(-25, 3)
    
    # Корректировка температуры в зависимости от времени суток
    if 0 <= current_hour < 6:     # Ночь 
        temp_adjust = -8
    elif 6 <= current_hour < 12:  # Утро
        temp_adjust = -4
    elif 12 <= current_hour < 18: # День
        temp_adjust = 2
    else:                         # Вечер
        temp_adjust = -6
    
    temperature = round(base_temp + temp_adjust, 1)
    
    # Влажность зависит от температуры
    if temperature < -20:
        humidity = round(random.uniform(50, 75), 1)
    elif temperature < -10:
        humidity = round(random.uniform(65, 80), 1)
    else:
        humidity = round(random.uniform(70, 85), 1)
    
    # Атмосферное давление
    pressure = round(random.uniform(730, 780), 1)
    
    # Скорость ветра зависит от давления
    if pressure < 740:
        wind_speed = round(random.uniform(3, 12), 1)  
    elif pressure > 770:
        wind_speed = round(random.uniform(1, 5), 1)   
    else:
        wind_speed = round(random.uniform(2, 8), 1)
    
    # Направление ветра
    directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    wind_direction = random.choice(directions)
    
    # Осадки (вероятность 30%)
    if random.random() < 0.3:
        if temperature < 0:
            precipitation = round(random.uniform(0.5, 3), 1)  # Снег
        elif temperature == 0:
            precipitation = round(random.uniform(0.1, 2), 1)  # Мокрый снег
        else:
            precipitation = round(random.uniform(0.1, 5), 1)  # Дождь
    else:
        precipitation = 0.0
    
    return {
        "station_id": station_id,
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
        "wind_speed": wind_speed,
        "wind_direction": wind_direction,
        "precipitation": precipitation
    }


def generate_air_quality(station_id):
    # Случайный выбор уровня загрязнения для станции
    pollution_levels = ["низкое", "среднее", "высокое"]
    level = random.choice(pollution_levels)
    
    # Генерация значений загрязнителей в зависимости от уровня
    if level == "низкое":
        pm25 = round(random.uniform(5, 20), 1)      # Мелкие частицы
        pm10 = round(random.uniform(10, 30), 1)     # Крупные частицы
        no2 = round(random.uniform(5, 15), 1)       # Диоксид азота
        so2 = round(random.uniform(1, 8), 1)        # Диоксид серы
        co = round(random.uniform(0.1, 0.5), 1)     # Угарный газ
    elif level == "среднее":
        pm25 = round(random.uniform(15, 35), 1)
        pm10 = round(random.uniform(25, 50), 1)
        no2 = round(random.uniform(15, 30), 1)
        so2 = round(random.uniform(5, 12), 1)
        co = round(random.uniform(0.3, 1.0), 1)
    else:  # высокое
        pm25 = round(random.uniform(30, 60), 1)
        pm10 = round(random.uniform(40, 80), 1)
        no2 = round(random.uniform(25, 45), 1)
        so2 = round(random.uniform(10, 20), 1)
        co = round(random.uniform(0.8, 2.0), 1)
    o3 = round(random.uniform(8, 25), 1)
    
    # Расчет индекса качества воздуха (AQI) на основе PM2.5
    if pm25 <= 12:
        aqi = int((pm25 / 12) * 50)
        health_impact = "Хорошее"
    elif pm25 <= 35.4:
        aqi = int(50 + ((pm25 - 12.1) / 23.3) * 50)
        health_impact = "Умеренное"
    elif pm25 <= 55.4:
        aqi = int(100 + ((pm25 - 35.5) / 19.9) * 50)
        health_impact = "Нездоровое"
    else:
        aqi = int(150 + ((pm25 - 55.5) / 94.9) * 100)
        health_impact = "Очень нездоровое"
    
    # Ограничение максимального значение AQI
    aqi = min(200, aqi)
    
    # Выбор источников загрязнения
    sources = ["транспорт", "промышленность", "отопление", "строительство"]
    selected_sources = random.sample(sources, random.randint(1, 3))
    source_type = ", ".join(selected_sources)
    
    return {
        "station_id": station_id,
        "pm25": pm25,
        "pm10": pm10,
        "no2": no2,
        "so2": so2,
        "o3": o3,
        "co": co,
        "aqi": aqi,
        "health_impact": health_impact,
        "source_type": source_type
    }


def main():
    # Ожидание инициализации базы данных
    time.sleep(10)
    
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Создание случайных метеостанций
        station_ids = initialize_random_stations(conn)
        
        # Бесконечный цикл генерации данных
        while True:
            current_time = datetime.now()
            print(f"\n[{current_time.strftime('%H:%M:%S')}]")
            
            # Генерация данных для каждой станции
            for station_id in station_ids:
                try:
                    # Генерация данных о погоде
                    weather = generate_weather_data(station_id)
                    
                    # Запись данных о погоде в таблицу weather_data
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO weather_data 
                            (station_id, temperature, humidity, pressure, 
                             wind_speed, wind_direction, precipitation)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            weather["station_id"],
                            weather["temperature"],
                            weather["humidity"],
                            weather["pressure"],
                            weather["wind_speed"],
                            weather["wind_direction"],
                            weather["precipitation"]
                        ))
                    
                    # Генерация данных о качестве воздуха
                    air = generate_air_quality(station_id)
                    
                    # Запись данных о качестве воздуха в таблицу air_quality
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO air_quality 
                            (station_id, pm25, pm10, no2, so2, o3, co, 
                             aqi, health_impact, source_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            air["station_id"],
                            air["pm25"],
                            air["pm10"],
                            air["no2"],
                            air["so2"],
                            air["o3"],
                            air["co"],
                            air["aqi"],
                            air["health_impact"],
                            air["source_type"]
                        ))
                    
                    # Вывод информации о сгенерированных данных
                    print(f"Ст.{station_id:2d} | "
                          f"Т:{weather['temperature']:5.1f}°C | "
                          f"В:{weather['humidity']:3.0f}% | "
                          f"Д:{weather['pressure']:6.1f}мм | "
                          f"В:{weather['wind_speed']:4.1f}м/с {weather['wind_direction']} | "
                          f"AQI:{air['aqi']:3d}")
                    
                except Exception as e:
                    conn.rollback()
                    continue
            
            # Фиксация всех изменений в базе данных
            conn.commit()
            
            # Пауза перед следующим циклом генерации
            time.sleep(UPDATE_INTERVAL)
            
    except Exception:
        # Минимальная обработка исключений без вывода сообщений
        pass
    finally:
        # Закрытие соединения с базой данных
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()