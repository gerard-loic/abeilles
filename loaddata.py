import xarray as xr
import pandas as pd

# Accès direct à ERA5 via Pangeo (sans téléchargement)
url = 'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3'

# Ouvrir le dataset (lecture directe cloud)
ds = xr.open_zarr(url, chunks='auto')

# Filtrer pour votre zone et période
lat_slice = slice(43, 51)  # France
lon_slice = slice(-5, 9)

ds_france = ds.sel(
    latitude=lat_slice,
    longitude=lon_slice,
    time=slice('1900', '2025')
)

# Extraire température 2m
temp_data = ds_france['2m_temperature']

print(temp_data)