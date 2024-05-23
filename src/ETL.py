"""Filtros

Este script aplica los filtros recomendados por Ookla para las pruebas 
en segundo plano. Adicionalmente, transforma los datos recolectados del
último mes calendario ubicados en la carpeta Datos a formato parquet y 
los almacena en la carpeta DatosLimpios. Los archivos generados son un 
insumo para la actualización del mapa interactivo de cobertura 4G del IFT.

Esta herramienta solo acepta archivos separados por coma (.csv) de la 
base de datos bgv2 de Ookla.

Este script requiere que `dask` y 'numpy' estén instalados dentro del 
ambiente de Python en donde se vaya a ejecutar el script.
"""

import dask.dataframe as dd
import numpy as np
from datetime import date

# Creamos un diccionario con el nombre de los campos de la base bgv2
formato = {
    'report_id': 'int64',
    'trigger_name': 'object',
    'result_date': 'object',
    'received_date': 'object',
    'received_date_local': 'object',
    'time_zone_name': 'object',
    'device_id': 'float64',
    'device_model':	'object',
    'device_manufacturer': 'object',
    'device_model_raw': 'object',
    'device_manufacturer_raw': 'object',
    'device_brand_raw': 'object',
    'os_version': 'object',
    'app_version': 'object',
    'connection_type': 'float64',
    'is_airplane_mode': 'object',
    'is_network_roaming': 'object',
    'is_international_roaming': 'object',
    'number_registered_networks': 'float64',
    'number_unregistered_networks': 'float64',
    'sim_operator_name': 'object',
    'raw_sim_operator_name': 'object',
    'sim_operator_mcc_code': 'float64',
    'sim_operator_mnc_code': 'float64',
    'network_operator_name': 'object',
    'network_operator_mcc_code': 'float64',
    'network_operator_mnc_code': 'float64',
    'client_latitude': 'float64',
    'client_longitude': 'float64',
    'altitude': 'float64',
    'location_accuracy': 'float64',
    'location_age': 'float64',
    'location_type': 'float64',
    'country': 'object',
    'region': 'object',
    'subregion': 'object',
    'locality': 'object',
    'place_type': 'object',
    'postal_code': 'object',
    'computed_cellular_generation': 'object',
    'tac': 'float64',
    'pci': 'float64',
    'cell_identifier': 'float64',
    'lte_enodeb': 'float64',
    'rnc_id': 'float64',
    'cell_id': 'float64',
    'arfcn': 'float64',
    'uarfcn': 'float64',
    'earfcn': 'float64',
    'is_primary_cell': 'object',
    'rsrp': 'float64',
    'rsrq': 'float64',
    'rssi': 'float64',
    'rssnr': 'float64',
    'timing_advance': 'float64',
    'cqi': 'float64',
    'wifi_enabled': 'object',
    'wifi_state': 'float64',
    'wifi_rssi': 'float64',
    'wifi_frequency': 'float64',
    'wifi_channel': 'float64',
    'app_foreground': 'object',
    'azimuth': 'float64',
    'battery_level': 'object',
    'battery_level_max': 'float64',
    'battery_plugged': 'object',
    'battery_present': 'object',
    'battery_status': 'float64',
    'battery_technology': 'object',
    'battery_temperature': 'float64',
    'battery_voltage': 'float64',
    'device_idle_mode': 'object',
    'humidity': 'float64',
    'humidity_accuracy': 'float64',
    'light_lx': 'float64',
    'light_accuracy': 'float64',
    'pitch': 'float64',
    'power_interactive': 'object',
    'power_save_mode': 'object',
    'pressure_accuracy': 'object',
    'pressure_mbar': 'float64',
    'temp_celsius': 'float64',
    'temp_accuracy': 'object',
    'grant_billing': 'object',
    'grant_internet': 'object',
    'grant_network_state': 'object',
    'grant_phone_state': 'object',
    'grant_fine_location': 'object',
    'grant_coarse_location': 'object',
    'grant_background_location': 'object',
    'grant_wifi_state': 'object',
    'grant_boot_completed': 'object',
    'valid_device_check': 'object',
    'location_check': 'object',
    'radio': 'object',
    'service_state': 'object',
    'sim_state': 'object',
    'sim_count': 'object',
    'guid': 'object',
    'cell_bandwidth': 'float64',
    'vertical_accuracy': 'float64',
    'nr_ss_rsrp': 'float64',
    'nr_ss_rsrq': 'float64',
    'nr_ss_sinr': 'float64',
    'nr_csi_rsrp': 'float64',
    'nr_csi_rsrq': 'float64',
    'nr_csi_sinr': 'float64',
    'nr_level': 'float64',
    'nr_asu': 'float64',
    'nr_arfcn': 'float64',
    'nr_nci': 'float64',
    'nr_pci': 'float64',
    'nr_tac': 'float64',
    'nr_mcc': 'object',
    'nr_mnc': 'object',
    'nr_state': 'float64',
    'nr_frequency_range': 'float64',
    "is_nr_available": 'object',
    'is_nr_telephony_sourced': 'object',
    'is_using_carrier_aggregation': 'object',
    'chipset_name': 'object',
    'chipset_manufacturer': 'object',
    'cell_bandwidths': 'object',
    'is_access_network_technology_nr': 'object',
    'device_tac': 'float64',
    'downstream_bandwidth_kbps': 'float64',
    'wifi_channel_width': 'float64',
    'has_bg_location_permission': 'object',
    'has_cellular_service': 'object',
    'upstream_bandwidth_kbps': 'float64',
    'gsm_additional_plmns': 'object',
    'tdscdma_additional_plmns': 'object',
    'wcdma_additional_plmns': 'object',
    'lte_additional_plmns': 'object',
    'lte_bands': 'object',
    'nr_additional_plmns': 'object',
    'nr_bands': 'object',
    'gsm_rssi': 'float64',
    'wcdma_ecno': 'float64',
    'wifi_rx_link_speed': 'float64',
    'wifi_max_supported_rx_link_speed': 'float64',
    'wifi_max_supported_tx_link_speed': 'float64',
    'wifi_passpoint_fqdn': 'object',
    'wifi_passpoint_provider_name': 'object',
    'wifi_carrier_name': 'object',
    'wifi_standard': 'float64',
    'wifi_is_2_4GHz_band_supported': 'object',
    'wifi_is_6GHz_band_supported': 'object',
    'wifi_is_60GHz_band_supported': 'object',
    'current_thermal_status': 'float64',
    'thermal_headroom': 'float64',
    'alt_sim_operator_name': 'object',
    'alt_raw_sim_operator_name': 'object',
    'alt_sim_operator_mcc_code': 'float64',
    'alt_sim_operator_mnc_code': 'float64',
    'data_activity': 'float64',
    'data_state': 'float64',
    'display_state': 'float64',
    'is_concurrent_voice_data_supported': 'object',
    'is_data_capable': 'object',
    'is_data_enabled': 'object',
    'is_data_connection_allowed': 'object',
    'is_data_roaming_enabled': 'object',
    'has_icc_card': 'object',
    'is_world_phone': 'object',
    'is_multi_sim_supported': 'float64',
    'active_modem_count': 'float64',
    'supported_modem_count': 'float64',
    'override_network_type': 'float64',
    'lac': 'float64',
    'psc': 'float64',
    'is_device_5g_capable': 'object'
}

# Cargamos los datos reportados por el background
df= dd.read_csv("Y:\\MapaCobertura\\Datos\\*.csv", \
                dtype=formato, parse_dates=['result_date', 'received_date'])

# Eliminamos los registros con valores nulos en nuestras variables de interés
df = df.dropna(subset=['network_operator_name']) 

# Nos quedamos con las observaciones que sean precisas en la ubicación
df = df[(df["location_accuracy"]<500) & (df["location_accuracy"]>1)]

# Eliminamos los registros de celulares en modo avión
df = df[(df["is_airplane_mode"]=="f")]

# Quitamos las mediciones fuera de la red nacional
df = df[(df["is_international_roaming"]=="f")]

# Muestras con ubicaciones determinadas por GPS
df = df[(df["location_type"]==1)]

# Filtramos las muestras cuya última medición de la ubicación haya sido hace más de 30 minutos
df = df[(df["location_age"]<=1800000) & (df["location_age"]>=1)]

# Filtro de calidad de la ubicación
df = df[(df['location_check']=="t")]

# Filtro de validez del celular
df = df[(df['valid_device_check']=="t")]

# Calculamos la diferencia de días entre el día del escaneo y el día que se cargó a la base de datos
df['dif'] = (df['received_date']-df['result_date'])/ np.timedelta64(1, 'D')

# Filtramos las muestras con una antigüedad mayor a dos días
df=df[df['dif']<=2]

# Filtramos rangos de potencias
df = df[(df['rsrp']>=-144) & (df['rsrp']<=-44)]

# Filtramos las observaciones de operadores extranjeros
df = df[df["network_operator_mcc_code"] == 334]

# Guardamos la base filtrada en archivos parquet
name_function = lambda x: f"tbl_RSRP_0{date.today().month}_{date.today().year}-{x}.parquet"
df.to_parquet('Y:\\Operadores\\DatosLimpios\\', name_function=name_function,
              partition_on = "network_operator_name")