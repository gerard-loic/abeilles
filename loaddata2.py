import requests
import pandas as pd

def get_gdd_from_open_meteo(lat, lon, date, t_base=5):
    """
    Récupère les températures et calcule GDD cumulé depuis le 1er janvier
    """
    # Extraire l'année
    year = pd.to_datetime(date).year
    start_date = f"{year}-01-01"
    
    # Requête API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date,
        'end_date': date,
        'daily': 'temperature_2m_max,temperature_2m_min',
        'timezone': 'auto'
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    # Calculer GDD
    temps = pd.DataFrame({
        'date': pd.to_datetime(data['daily']['time']),
        't_max': data['daily']['temperature_2m_max'],
        't_min': data['daily']['temperature_2m_min']
    })
    
    temps['t_mean'] = (temps['t_max'] + temps['t_min']) / 2
    temps['gdd_daily'] = (temps['t_mean'] - t_base).clip(lower=0)
    gdd_cumul = temps['gdd_daily'].sum()
    
    return gdd_cumul

# Exemple d'utilisation
lat, lon = 48.8, 2.49
date = "2023-02-01"
gdd = get_gdd_from_open_meteo(lat, lon, date, t_base=5)
print(f"GDD cumulé au {date} : {gdd:.1f}°C·jours")