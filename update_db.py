# CODE FOR UNIQUE SCRIPT TO UPDATE DATABASES

#TRANSETOS MAMIFEROS
import psycopg2
import datetime
from datetime import timezone
import requests
from shapely.geometry import Point
# Connect to the PostgreSQL database (replace with your own connection details)
conn = psycopg2.connect(
    dbname="biota",
    user="postgres",
    password="datamiguel1M",
    host="localhost"
)

# Create a cursor object to execute SQL commands
cur = conn.cursor()

#import datetime
# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='TMAM';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aD9sgAGeEJPvanixawgTvT/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aD9sgAGeEJPvanixawgTvT/data/?format=json&query={"_submission_time":{"$gt":"2023-05-28T22:37:06"}}'

# Request headers (including the access token)
headers = {
    'Authorization': 'Token 8cd83af6fdc8e74c1dfa40430e7a88b5acc04b01'
}

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
   # Now 'data' contains the response data from the API
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result['group_xn9dx42/eventid']
        project_code = result['group_xn9dx42/Codigo_projeto']
        campanha = result['group_xn9dx42/Campanha_de_amostragem']
        protocolo = result['group_xn9dx42/Protocolo']
        valor = result['group_xn9dx42/Valor_da_amostragem']
        unidade = result['group_xn9dx42/Unidade_de_amostragem']
        esforço = result['group_xn9dx42/Esfor_o_de_amostragem']
        #nr_campanha = result['group_xn9dx42/N_mero_da_campanha_de_amostragem']
        date = result['group_xn9dx42/Data']
        hour = result['group_xn9dx42/Hora']
        location_code = result['group_xn9dx42/Codigo_local']
        recordedby = result['group_xn9dx42/Tecnico']
        submission_time = result['_submission_time']
        dados_data = result.get('dados', [])
    # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

            # Check if most_recent_date is None or the result's date is more recent than the most recent in the database
        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
    INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, eventdate, eventtime, locationid, recordedby, submission_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)                    
""", (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, date, hour, location_code, recordedby, end_time))

        for dados in dados_data:
            especie = dados.get('dados/especie')
            tipo_obs = dados.get('dados/Tipo_obsercacao')
            numero_indicios = dados.get('dados/Numero_de_indicios')
            codigo_gps = dados.get('dados/Codigo_gps')
            localizacao = dados.get('dados/Localizacao')
            photo_code = dados.get('dados/Codigo_fotos')

            try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
            except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None
            
            if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
            else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
            if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
            else:
                point_text = None

            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification, organismquantitytype, individualcount, gps_id, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, locationid, recordedby, geometry, submission_time, occ_photosID) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s)                   
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, tipo_obs, numero_indicios, codigo_gps, latitude, longitude, altitude, precisao, location_code, recordedby, point_text, end_time, photo_code))
            
#Confirm the changes and close the connection
conn.commit()

# TRANSETOS AQUÁTICOS

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol ='TMAQ';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aFnwPJHMYaCfAkRbBWAcHx/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aFnwPJHMYaCfAkRbBWAcHx/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result['group_ep8mf94/eventid']
        project_code = result['group_ep8mf94/Codigo_projeto']
        campanha = result['group_ep8mf94/campanha']
        protocolo = result['group_ep8mf94/Protocolo']
        valor = result['group_ep8mf94/Valor_da_amostragem']
        unidade = result['group_ep8mf94/Unidade_de_amostragem']
        esforço = result['group_ep8mf94/Esfor_o_de_amostragem']
        date = result['group_ep8mf94/Data']
        start_hour = result['group_ep8mf94/Hora_inicial']
        end_hour = result['group_ep8mf94/Hora_final']
        location_code = result['group_ep8mf94/Codigo_local']
        recordedby = result['group_ep8mf94/Tecnicos']
        submission_time = result['_submission_time']
        ev_photoid = result['group_ep8mf94/Codigo_fotos']
        dados_data = result.get('group_yw1fc15', [])

            # Parse the end time string into a datetime object
        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
    INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, eventdate, eventtime_start, eventtime_end, locationid, recordedby, submission_time, photosid)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)                    
""", (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, date, start_hour, end_hour, location_code, recordedby, end_time, ev_photoid))

        for dados_item in dados_data:
            especie = dados_item.get('group_yw1fc15/Especie')
            tipo_indicio = dados_item.get('group_yw1fc15/Tipo_de_indicio')
            numero_indicios = dados_item.get('group_yw1fc15/Numero_de_indicios')
            Local_de_deposicao_do_dejeto = dados_item.get('group_yw1fc15/Local_de_deposicao_do_dejeto')
            Material_de_deposicao_do_dejeto = dados_item.get('group_yw1fc15/Material_de_deposicao_do_dejet')
            Local_de_latrina = dados_item.get('group_yw1fc15/Local_de_latrina')
            codigo_gps = dados_item.get('group_yw1fc15/Codigo_GPS')
            localizacao = dados_item.get('group_yw1fc15/Localizacao')
            recolha_de_amostra = dados_item.get('group_yw1fc15/recolha_de_amostra')
            Codigo_amostras = dados_item.get('group_yw1fc15/Codigo_amostras')
            occ_photosid = dados_item.get('group_yw1fc15/Codigo_fotos_001')

            try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
            except:
                latitude = 0
                longitude = 0
                altitude = 0
                precisao = 0

            if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
            else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
            if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
            else:
                point_text = None

            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification, organismquantitytype, individualcount, wastedepositionlocation, wastedepositionmaterial, latrinelocation, gps_id, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, locationid, samplecollection, samplecode, recordedby, geometry, submission_time, occ_photosID) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s)                   
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, tipo_indicio, numero_indicios, Local_de_deposicao_do_dejeto, Material_de_deposicao_do_dejeto, Local_de_latrina, codigo_gps, latitude, longitude, altitude, precisao, location_code, recolha_de_amostra, Codigo_amostras, recordedby, point_text, end_time, occ_photosid))

# Confirm the changes and close the connection
conn.commit()

# #ABRIGOS QUIROPTEROS

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='ABMO';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aQaBth87BDABGUm8gvSTsY/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aQaBth87BDABGUm8gvSTsY/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
   # Now 'data' contains the response data from the API
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result.get('group_ib5ql93/eventid')
        project_code = result['group_ib5ql93/Codigo_projeto'] 
        campanha = result['group_ib5ql93/campanha']
        protocolo = result['group_ib5ql93/Protocolo']
        valor = result.get('group_ib5ql93/Valor_da_amostragem')
        unidade = result.get('group_ib5ql93/Unidade_de_amostragem')
        esforço = result.get('group_ib5ql93/Esfor_o_de_amostragem')
        #nr_campanha = result.get('group_ib5ql93/N_mero_campanha_amostragem')
        date = result['group_ib5ql93/Data']
        start_hour = result['group_ib5ql93/Hora_inicial']
        end_hour = result['group_ib5ql93/Hora_final']
        location_code = result['group_ib5ql93/Codigo_Local']
        cover = result['group_ib5ql93/Tipo_de_abrigo']
        recordedby = result.get('group_ib5ql93/Tecnicos_de_amostragem')
        submission_time = result['_submission_time']
        codigo_gps = result.get('group_ib5ql93/codigo_gps')
        localizacao = result.get('group_ib5ql93/coordenadas_geograficas')
        ev_photos = result.get('group_ib5ql93/codigos_fotos')
        notes = result.get('group_ib5ql93/Notas')
        dados_data = result.get('group_cy1au63', [])
        # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        try:
            x = localizacao.split()
            latitude = float(x[0])
            longitude = float(x[1])
            altitude = x[2]
            precisao = x[3]
        except:
            latitude = 0
            longitude = 0
            altitude = 0
            precisao = 0

        if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
            point = Point(longitude, latitude)
        else:
            point = None

        # Convert the Shapely object to a PostGIS text representation
        if latitude is not None and longitude is not None:
            point_text = f'POINT({point.x} {point.y})'
        else:
            point_text = None

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
    INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, eventdate, eventtime_start, eventtime_end, type_cover, recordedby, submission_time, photosid, fieldnotes, gps_id, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, locationid, geometry)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,ST_GeomFromText(%s, 4326))                    
""", (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, date, start_hour, end_hour, cover, recordedby, end_time, ev_photos, notes, codigo_gps, latitude, longitude, altitude, precisao, location_code, point_text))
 
        for dados_item in dados_data:
            especie = dados_item.get('group_cy1au63/Especie')
            numero_indicios = dados_item.get('group_cy1au63/N_mero_de_indiv_duos')
            occ_photosid = dados_item.get('group_cy1au63/Codigo_Fotos')
            notes = dados_item.get('group_cy1au63/Notas_001')
            
            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification, individualcount, recordedby, geometry, submission_time, occ_photosID, fieldNotes) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s)                   
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, numero_indicios, recordedby, point_text, end_time, occ_photosid, notes))
    
#Confirm the changes and close the connection
conn.commit()

#ESCUTAS QUIROPTEROS

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM events where samplingprotocol='EQUI';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/a9ppHHgCPUTvXLjTTW2ipE/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/a9ppHHgCPUTvXLjTTW2ipE/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
   # Now 'data' contains the response data from the API
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result['group_si8dz50/eventid']
        project_code = result['group_si8dz50/Codigo_projeto'] 
        campanha = result['group_si8dz50/campanha']
        protocolo = result['group_si8dz50/Protocolo']
        valor = result.get('group_si8dz50/Valor_da_amostragem')
        unidade = result.get('group_si8dz50/Unidade_de_amostragem')
        esforço = result.get('group_si8dz50/Esfor_o_de_amostragem')
        #nr_campanha = result.get('group_si8dz50/N_mero_campanha_amostragem')
        date = result['group_si8dz50/Data']
        location_code = result['group_si8dz50/Codigo_Local']
        recordedby = result['group_si8dz50/Tecnicos_de_amostragem']
        biotype_habitat = result['group_si8dz50/Bi_topo_Habitat']
        time_sunset = result ['group_si8dz50/Por_do_sol']
        time_start = result ['group_nd31f82/Hora_in_cio']
        time_end = result ['group_nd31f82/Hora_fim']
        wind_speed = result ['group_nd31f82/Velocidade_do_vento_m_s']
        type_wind = result ['group_nd31f82/Velocidade_do_vento']
        temp = result ['group_nd31f82/Temperatura_C']
        type_precipitation = result ['group_nd31f82/Precipita_o']
        humidity = result ['group_nd31f82/Humidade_']
        type_humidity = result['group_nd31f82/Humidade']
        cloudiness = result['group_nd31f82/Nebulosidade']
        nr_passages = result.get('group_nd31f82/tempo_utilizacao')
        code_file_start = result.get('group_nd31f82/Codigo_ficheiro_inicial')
        code_file_end = result.get('group_nd31f82/Codigo_ficheiro_final')
        notes = result.get('group_nd31f82/Notas_001')
        submission_time = result['_submission_time']
        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)


        # Insert into the database inside the loop
        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
            INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit,  samplingeffort, eventdate, locationid, recordedby, biotype_habitat, time_sunset, eventtime_start, eventtime_end, wind_speed, type_wind, temperature, type_precipitation, humidity, type_humidity, cloudiness, time_usage, code_file_start, code_file_end, fieldnotes, submission_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)                    
""", (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, date, location_code, recordedby, biotype_habitat, time_sunset, time_start, time_end, wind_speed, type_wind, temp, type_precipitation, humidity, type_humidity, cloudiness, nr_passages, code_file_start, code_file_end, notes, end_time))

#Confirm the changes and close the connection
conn.commit()

#ARMADILHAGEM FOTO

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM events where samplingprotocol='AFOT';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aFzUK7Wa6JPvBHYWD8PoTF/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aFzUK7Wa6JPvBHYWD8PoTF/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
   # Now 'data' contains the response data from the API
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result['group_et4hk59/eventid']
        project_code = result['group_et4hk59/Codigo_projeto']
        amostragem = result['group_et4hk59/campanha']
        protocolo = result['group_et4hk59/Protocolo']
        valor = result.get('group_et4hk59/Valor_da_amostragem')
        unidade = result.get('group_et4hk59/Unidade_de_amostragem')
        esforço = result.get('group_et4hk59/Esfor_o_de_amostragem')
        date = result['group_et4hk59/Data']
        hour = result['group_et4hk59/Hora']
        location_code = result['group_et4hk59/Codigo_local']
        biotype_habitat = result['group_et4hk59/Biotopo_Habitat']
        recordedby = result['group_et4hk59/Tecnico']
        type_of_activity = result['group_um6iw12/tipo_de_atividade']
        camera_code = result.get('group_um6iw12/Codigo_camara')
        locker_code = result.get('group_um6iw12/Codigo_cadeado')
        card_code = result.get('group_um6iw12/Codigo_cartao') or result.get('group_um6iw12/Codigo_cartao_001')
        localizacao = result.get('group_um6iw12/Localizacao')
        codigo_gps = result.get('group_um6iw12/Codigo_GPS')
        description_camera_placement = result.get('group_um6iw12/Descricao')
        submission_time = result['_submission_time']
        submittedby = result.get('_submitted_by')
        battery_status = result.get('group_um6iw12/Estado_da_bateria') or result.get('group_um6iw12/Estado_da_bateria_001')
        nr_photos = result.get('group_um6iw12/Numero_de_fotos') or result.get('group_um6iw12/Numero_de_fotos_001')

        try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
        except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

        if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
            point = Point(longitude, latitude)
        else:
            point = None

        # Convert the Shapely object to a PostGIS text representation
        if latitude is not None and longitude is not None:
            point_text = f'POINT({point.x} {point.y})'
        else:
             point_text = None


    # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

            # Check if most_recent_date is None or the result's date is more recent than the most recent in the database
        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
    INSERT INTO events (id_form, eventid, samplingprotocol, samplesizevalue, samplesizeunit,  samplingeffort, parenteventid, amostragemid, eventdate, eventtime, locationid, biotype_habitat, recordedby,type_of_activity, camera_code, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, geometry, gps_id, description_camera_placement, battery_status, nr_photos, submission_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s)                  
""", (id_form, eventid, protocolo, valor,unidade, esforço, project_code, amostragem, date, hour, location_code, biotype_habitat, recordedby, type_of_activity, camera_code, latitude, longitude, altitude, precisao, point_text, codigo_gps, description_camera_placement, battery_status, nr_photos, end_time))
            
#Confirm the changes and close the connection
conn.commit()

#CARACTERIZACAO BARREIRAS

#import datetime
# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM events where samplingprotocol='CBAR';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aC9Td5iJAVMrVoaQUEpbNW/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aC9Td5iJAVMrVoaQUEpbNW/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
   # Now 'data' contains the response data from the API
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id = result['_id']
        project_code = result['dados_gerais/codigo_projeto']
        campanha = result['dados_gerais/Campanha_de_amostragem']
        locationid = result['dados_gerais/C_digo_local']
        protocolo = result['dados_gerais/Protocolo']
        valor = result.get('dados_gerais/Valor_da_amostragem')
        unidade = result.get('dados_gerais/Unidade_de_amostragem')
        esforço = result.get('dados_gerais/Esfor_o_de_amostragem')
        date = result['dados_gerais/data']
        hour = result['dados_gerais/hora']
        recordedby = result['dados_gerais/tecnico_de_amostragem']
        name_technician = result.get('dados_gerais/nome_tecnico')
        eventid = result.get('dados_gerais/eventid')
        barrier_code = result['descricao_da_barreira/codigo_barreira']
        barrier_name = result['descricao_da_barreira/nome_da_barreira']
        localizacao = result.get('descricao_da_barreira/coordenadas_geograficas')
        codigo_gps = result.get('descricao_da_barreira/codigo_gps')
        barrier_type = result['caracterizacao_da_barreira/tipo_de_barreira']
        other_barrier_type = result.get('caracterizacao_da_barreira/outro')
        barrier_ownership = result['caracterizacao_da_barreira/titularidade_da_barreira']
        owner_identification = result.get('caracterizacao_da_barreira/identificacao_proprietario')
        barrier_function = result.get('caracterizacao_da_barreira/funcao_uso_ou_objetivo_da_barreira')
        barrier_conservation = result['caracterizacao_da_barreira/estado_de_conservacao_da_barreira']
        construction_material = result['caracterizacao_da_barreira/material_de_construcao']
        other_construction_material = result.get('caracterizacao_da_barreira/outro_material_construcao')
        fish_passage = result['caracterizacao_da_barreira/passagem_para_peixes']
        minimum_barrier_height = result['caracterizacao_da_barreira/altura_minima_da_barreira']
        maximum_barrier_height = result['caracterizacao_da_barreira/altura_maxima_da_barreira']
        barrier_width = result['caracterizacao_da_barreira/largura_da_barreira']
        right_land_uses = result['caracterizacao_envolvente/usos_do_solo_direita']
        left_land_uses = result['caracterizacao_envolvente/usos_do_solo_esquerda']
        gallery_ripicola_right = result['caracterizacao_envolvente/galeria_ripicola_direita']
        gallery_ripicola_left = result['caracterizacao_envolvente/galeria_ripicola_esquerda']
        dominance_gallery = result['caracterizacao_envolvente/dominancia_galeria']
        dominant_species = result.get('caracterizacao_envolvente/especies_dominantes')
        other_information = result['caracterizacao_envolvente/outra_informacao']
        other_data = result.get('caracterizacao_envolvente/outros_dados')
        submission_time = result['_submission_time']
        submittedby = result.get('_submitted_by')

        try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
        except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

        if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
            point = Point(longitude, latitude)
        else:
            point = None

        # Convert the Shapely object to a PostGIS text representation
        if latitude is not None and longitude is not None:
            point_text = f'POINT({point.x} {point.y})'
        else:
             point_text = None


    # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

            # Check if most_recent_date is None or the result's date is more recent than the most recent in the database
        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
    INSERT INTO events (id_form, eventid, parenteventid, samplesizevalue, samplesizeunit,  samplingeffort, amostragemid, samplingprotocol, locationid, eventdate, eventtime, recordedby, other_technician, barrier_code, barrier_name, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, gps_id, barrier_type, other_barrier_type, barrier_ownership, owner_identification, barrier_function, barrier_conservation, construction_material, fish_passage, minimum_barrier_height, maximum_barrier_height, barrier_width, right_land_uses, left_land_uses, gallery_ripicola_right, gallery_ripicola_left, dominance_gallery, dominant_species, other_information, other_data, submission_time, geometry)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))                  
""", (id_form, eventid, project_code, valor, unidade, esforço, campanha, protocolo, locationid, date, hour, recordedby, name_technician, barrier_code, barrier_name, latitude, longitude, altitude, precisao, codigo_gps, barrier_type, other_barrier_type, barrier_ownership, owner_identification, barrier_function, barrier_conservation, construction_material, fish_passage, minimum_barrier_height, maximum_barrier_height, barrier_width, right_land_uses, left_land_uses,gallery_ripicola_right, gallery_ripicola_left, dominance_gallery, dominant_species, other_information, other_data, end_time, point_text))
            
#Confirm the changes and close the connection
conn.commit()


#INVENTARIO FLORA

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='IFLO';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aDmDg2hnA2MYb2sswjqfZp/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aDmDg2hnA2MYb2sswjqfZp/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result.get('dados_local/eventid')
        project_code = result.get('dados_local/Codigo_projeto')
        campanha = result.get('dados_local/campanha')
        protocolo = result['dados_local/Protocolo']
        valor = result.get('dados_local/Valor_da_amostragem')
        unidade = result.get('dados_local/Unidade_de_amostragem')
        esforço = result.get('dados_local/Esfor_o_de_amostragem')
        locationid = result.get('dados_local/C_digo_local')
        date = result.get('dados_local/Data')
        hour = result.get('dados_local/Hora')
        recordedby = result.get('dados_local/Tecnico')
        parcel_code = result.get('descricao_da_parcela/Codigo_Parcela')
        gps_point = result.get('descricao_da_parcela/Ponto_GPS')
        especie_alvo = result.get('descricao_da_parcela/Esp_cie_Alvo')
        phenology = result.get('descricao_da_parcela/Fenologia')
        abundance = result.get('descricao_da_parcela/Abundancia')
        bush_cover = result.get('descricao_da_parcela/Cobertura_arbustiva')
        herbaceous_cover = result.get('descricao_da_parcela/Cobertura_herbacea')
        lichen_cover = result.get('descricao_da_parcela/Cobertura_liquenes')
        epiphytic_coverage = result.get('descricao_da_parcela/Cobertura_epifitico')
        bare_ground_cover = result.get('descricao_da_parcela/Cobertura_solo_nu')
        number_of_strata = result.get('descricao_da_parcela/Numero_de_estratos')
        dominant_stratum = result.get('descricao_da_parcela/Estrato_dominante')
        habitat = result.get('descricao_da_parcela/Habitat')
        other_habitat = result.get('descricao_da_parcela/outro_habitat')
        disturbances = result.get('descricao_da_parcela/Perturbacoes')
        other_disturbances = result.get('descricao_da_parcela/outras_pertubacoes')
        submission_time = result.get('_submission_time')
        submittedby = result.get('_submitted_by')
        dados_data = result.get('Inventario', [])
        # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (id_form, eventid, parenteventid, samplingprotocol, samplesizevalue, samplesizeunit,  samplingeffort, locationid, amostragemid, eventdate, eventtime, recordedby, parcel_code, gps_id, phenology, abundance, bush_cover, herbaceous_cover, lichen_cover, epiphytic_coverage, bare_ground_cover, number_of_strata, dominant_stratum, habitat, other_habitat, disturbances, other_disturbances, submission_time, target_species)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_form, eventid, project_code, protocolo, valor, unidade, esforço, locationid, campanha, date, hour, recordedby, parcel_code, gps_point, phenology, abundance, bush_cover, herbaceous_cover, lichen_cover, epiphytic_coverage, bare_ground_cover, number_of_strata, dominant_stratum, habitat, other_habitat, disturbances, other_disturbances,  end_time, especie_alvo))

        for dados_item in dados_data:
            especie = dados_item.get('Inventario/Especie') or dados_item.get('Inventario/Esp_cie')
            other_especie = dados_item.get('Inventario/outra_especie')
            cobertura = dados_item.get('Inventario/Cobertura_Esp_cie_')
            index = dados_item.get('Inventario/Indice')
            occ_photosID = dados_item.get('Inventario/Codigo_fotos_especie')
            comments = dados_item.get('Inventario/Comentarios')

            # Insert into the database inside the loop
            # Check if most_recent_date is None or the result's date is more recent than the most recent in the database
    
            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification, specie_coverage, index, occ_photosid, comments, submission_time, other_verbatimidentification, locationid)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, cobertura, index, occ_photosID, comments, end_time, other_especie, locationid))

# Confirm the changes and close the connection
conn.commit()

#TRANSETOS MORTALIDADE

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='TMOR';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aGnVyGgQ4PCZ4KhmFaKutY/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aGnVyGgQ4PCZ4KhmFaKutY/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
   # Now 'data' contains the response data from the API
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

    # Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result['dados_local/eventid']
        project_code = result['dados_local/Codigo_projeto']
        campanha = result['dados_local/campanha']
        protocolo = result['dados_local/Protocolo']
        valor = result.get('dados_local/Valor_da_amostragem')
        unidade = result.get('dados_local/Unidade_de_amostragem')
        esforço = result.get('dados_local/Esfor_o_de_amostragem')
        date = result['dados_local/Data']
        start_hour = result['dados_local/Hora_inicio']
        end_hour = result['dados_local/Hora_fim']
        location_code = result['dados_local/Codigo_local']
        recordedby = result['dados_local/Tecnico']
        type_wind = result['group_yu7xl31/Vento']
        cloudiness = result['group_yu7xl31/Nebulosidade']
        type_precipitation = result ['group_yu7xl31/Precipitacao']
        visibility = result['group_yu7xl31/Visibilidade']
        temp = result['group_yu7xl31/Temperatura']
        humidity = result['group_yu7xl31/Humidade']
        submission_time = result['_submission_time']
        #submittedby = result['_submitted_by'] or 'N/A'
        dados_data = result.get('Observacoes', [])
        # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (
            id_form, eventid, samplingprotocol, parenteventid, amostragemid, samplesizevalue, samplesizeunit, samplingeffort, eventdate, eventtime_start, eventtime_end, 
            locationid, recordedby, type_wind, cloudiness, type_precipitation, visibility, 
            temperature, type_humidity, submission_time
        )   
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, (
        id_form, eventid, protocolo, project_code, campanha, valor, unidade, esforço, date, start_hour, end_hour, location_code, recordedby, 
        type_wind, cloudiness, type_precipitation, visibility, temp, humidity, end_time))

        for dados_item in dados_data:
            especie = dados_item.get('Observacoes/Especie')
            tipo_obs = dados_item.get('Observacoes/Tipo_de_observacao')
            numero_individuos = dados_item.get('Observacoes/Numero_de_individuos')
            genero = dados_item.get('Observacoes/Genero')
            classe_etaria = dados_item.get('Observacoes/Classe_etaria')
            biotopo = dados_item.get('Observacoes/Biotopo')
            codigo_gps = dados_item.get('Observacoes/Codigo_GPS') 
            localizacao = dados_item.get('Observacoes/Localizacao') 
            estado = dados_item.get('Observacoes/Estado') 
            antiguidade = dados_item.get('Observacoes/Antiguidade') 
            traumatismos = dados_item.get('Observacoes/Tipo_de_traumatismos')
            indicios_de_predacao = dados_item.get('Observacoes/Indicios_de_predacao') 
            local_estrada = dados_item.get('Observacoes/Local_estrada') 
            tipo_estado = dados_item.get('Observacoes/Estado_001') 
            outro_indicio = dados_item.get('Observacoes/Outro_indicio')

            try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
            except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

            if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
            else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
            if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
            else:
                point_text = None

            # Insert into the database inside the loop
            # Check if most_recent_date is None or the result's date is more recent than the most recent in the database
            if most_recent_date is None or end_time > most_recent_date:
                cur.execute("""
        INSERT INTO occurrences (
            id_form, eventid, samplingprotocol, parenteventid, amostragemid,samplesizevalue, samplesizeunit, samplingeffort,
            locationid, recordedby, verbatimidentification, 
            organismquantitytype, individualcount, gender, age_class, biotype, gps_id, 
            decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, 
            state, antiquity, trauma, signs_of_predation, road_location, other_state, 
            other_organismQuantityType, geometry, submission_time
        )   
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s
        )
    """, (
        id_form, eventid, protocolo, project_code, campanha, valor, unidade, esforço, location_code, recordedby,
        especie, tipo_obs, numero_individuos, genero, classe_etaria, biotopo, 
        codigo_gps, latitude, longitude, altitude, precisao, estado, antiguidade, traumatismos, 
        indicios_de_predacao, local_estrada, tipo_estado, outro_indicio, point_text, end_time
    ))
            
#HABITAT e FLORA

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='HAFL';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/a9H5c5RbzQfyA3RB4G4Npj/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/a9H5c5RbzQfyA3RB4G4Npj/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result.get('dados_local/eventid')
        project_code = result.get('dados_local/Codigo_projeto')
        campanha = result.get('dados_local/campanha')
        protocolo = result['dados_local/Protocolo']
        valor = result.get('dados_local/Valor_da_amostragem')
        unidade = result.get('dados_local/Unidade_de_amostragem')
        esforço = result.get('dados_local/Esfor_o_de_amostragem')
        locationid = result.get('dados_local/C_digo_local')
        date = result.get('dados_local/Data')
        hour = result.get('dados_local/Hora')
        recordedby = result.get('dados_local/Tecnico')
        parcel_code = result.get('descricao_da_parcela/Codigo_Parcela')
        gps_point = result.get('descricao_da_parcela/Ponto_GPS')
        especie_alvo = result.get('descricao_da_parcela/Esp_cie_Alvo')
        other_especie_alvo = result.get('descricao_da_parcela/outra_especie')
        phenology = result.get('descricao_da_parcela/Fenologia')
        abundance = result.get('descricao_da_parcela/Abundancia')
        bush_cover = result.get('descricao_da_parcela/Cobertura_arbustiva')
        herbaceous_cover = result.get('descricao_da_parcela/Cobertura_herbacea')
        lichen_cover = result.get('descricao_da_parcela/Cobertura_liquenes')
        epiphytic_coverage = result.get('descricao_da_parcela/Cobertura_epifitico')
        bare_ground_cover = result.get('descricao_da_parcela/Cobertura_solo_nu')
        number_of_strata = result.get('descricao_da_parcela/Numero_de_estratos')
        dominant_stratum = result.get('descricao_da_parcela/Estrato_dominante')
        habitat = result.get('descricao_da_parcela/Habitat')
        other_habitat = result.get('descricao_da_parcela/outro_habitat')
        disturbances = result.get('descricao_da_parcela/Perturbacoes')
        other_disturbances = result.get('descricao_da_parcela/outras_pertubacoes')
        submission_time = result.get('_submission_time')
        submittedby = result.get('_submitted_by')
        dados_data = result.get('Inventario', [])
        # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (id_form, eventid, parenteventid, samplingprotocol, samplesizevalue, samplesizeunit,  samplingeffort, locationid, amostragemid, eventdate, eventtime, recordedby, parcel_code, gps_id, phenology, abundance, bush_cover, herbaceous_cover, lichen_cover, epiphytic_coverage, bare_ground_cover, number_of_strata, dominant_stratum, habitat, other_habitat, disturbances, other_disturbances, submission_time, target_species, other_target_species)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_form, eventid, project_code, protocolo, valor, unidade, esforço, locationid, campanha, date, hour, recordedby, parcel_code, gps_point, phenology, abundance, bush_cover, herbaceous_cover, lichen_cover, epiphytic_coverage, bare_ground_cover, number_of_strata, dominant_stratum, habitat, other_habitat, disturbances, other_disturbances,  end_time, especie_alvo, other_especie_alvo))

        for dados_item in dados_data:
            especie = dados_item.get('Inventario/Especie') or dados_item.get('Inventario/Esp_cie')
            other_especie = dados_item.get('Inventario/outra_especie')
            cobertura = dados_item.get('Inventario/Cobertura_Esp_cie_')
            index = dados_item.get('Inventario/Indice')
            occ_photosID = dados_item.get('Inventario/Codigo_fotos_especie')
            comments = dados_item.get('Inventario/Comentarios')

            # Insert into the database inside the loop
            # Check if most_recent_date is None or the result's date is more recent than the most recent in the database
    
            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification, specie_coverage, index, occ_photosid, comments, submission_time, other_verbatimidentification)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, cobertura, index, occ_photosID, comments, end_time, other_especie))


#ESCUTA AVES


# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='EAVE';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aGsgjmYvNB6okBLB9KEXM5/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aGsgjmYvNB6okBLB9KEXM5/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result.get('identificacao/eventid')
        project_code = result.get('identificacao/Codigo_projeto')
        campanha = result.get('identificacao/campanha')
        protocolo = result['identificacao/Protocolo']
        valor = result.get('identificacao/Valor_da_amostragem')
        unidade = result.get('identificacao/Unidade_de_amostragem')
        esforço = result.get('identificacao/Esfor_o_de_amostragem')
        locationid = result.get('identificacao/Codigo_Local')
        localizacao = result.get('identificacao/store_gps')
        date = result.get('identificacao/Data')
        start_hour = result.get('identificacao/Hora_in_cio')
        end_hour = result.get('identificacao/Hora_fim')
        recordedby = result.get('identificacao/Tecnicos_de_amostragem')
        gps_point = result.get('identificacao/C_digo_GPS')
        dir_vento = result.get('identificacao/dir_vento')
        int_vento = result.get('identificacao/int_vento')
        neb = result.get('identificacao/g_nebulosidade')
        prep = result.get('identificacao/g_precipitacao')
        visibility = result.get('identificacao/g_visibilidade')
        temp = result.get('identificacao/temperatura')
        habitat = result.get('identificacao/habitat')
        obs = result.get('identificacao/observacoes')
        submission_time = result.get('_submission_time')
        dados_data = result.get('repetir', [])
        # Parse the end time string into a datetime object

        try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
        except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

        if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
        else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
        if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
        else:
                point_text = None

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, locationid, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, eventdate, eventtime_start, eventtime_end, recordedby, gps_id, wind_direction, type_wind, cloudiness, type_precipitation, visibility, temperature, habitat, fieldNotes, submission_time, geometry)  
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, locationid, latitude, longitude, altitude, precisao, date, start_hour, end_hour, recordedby, gps_point, dir_vento, int_vento, neb, prep, visibility, temp, habitat, obs, end_time, point_text))

        for dados_item in dados_data:
            distancia_obs = dados_item.get('repetir/dados/distancia')
            especie = dados_item.get('repetir/dados/especie')
            other_especie = dados_item.get('repetir/dados/oespecie')
            numero_indicios = dados_item.get('repetir/dados/numero')
    
            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification,  submission_time, other_verbatimidentification, distance_observation, individualcount)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, end_time, other_especie, distancia_obs, numero_indicios))


#ESCUTA ANFIBIOS

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='EANF';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aR6qNkEtHrDiUQdgVjotAJ/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aR6qNkEtHrDiUQdgVjotAJ/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result.get('identificacao/eventid')
        project_code = result.get('identificacao/projeto')
        campanha = result.get('identificacao/campanha')
        protocolo = result['identificacao/Protocolo']
        valor = result.get('identificacao/Valor_da_amostragem')
        unidade = result.get('identificacao/Unidade_de_amostragem')
        esforço = result.get('identificacao/Esfor_o_de_amostragem')
        locationid = result.get('identificacao/codigo_local')
        localizacao = result.get('identificacao/localizacao')
        date = result.get('identificacao/Data')
        start_hour = result.get('identificacao/Hora_in_cio')
        end_hour = result.get('identificacao/Hora_fim')
        recordedby = result.get('identificacao/tecnico')
        gps_point = result.get('identificacao/C_digo_GPS')
        dir_vento = result.get('identificacao/dir_vento')
        int_vento = result.get('identificacao/int_vento')
        neb = result.get('identificacao/g_nebulosidade')
        hum = result.get('identificacao/g_hum')
        prep = result.get('identificacao/g_precipitacao')
        temp = result.get('identificacao/temperatura')
        habitat = result.get('identificacao/habitat')
        obs = result.get('identificacao/observacoes')
        submission_time = result.get('_submission_time')
        dados_data = result.get('repetir', [])
        # Parse the end time string into a datetime object

        try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
        except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

        if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
        else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
        if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
        else:
                point_text = None

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, locationid, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, eventdate, eventtime_start, eventtime_end, recordedby, gps_id, wind_direction, type_wind, cloudiness, type_humidity, type_precipitation, temperature, habitat, fieldNotes, submission_time, geometry)  
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, locationid, latitude, longitude, altitude, precisao, date, start_hour, end_hour, recordedby, gps_point, dir_vento, int_vento, neb, hum, prep, temp, habitat, obs, end_time, point_text))

        for dados_item in dados_data:
            especie = dados_item.get('repetir/dados/especie')
            other_especie = dados_item.get('repetir/dados/outra_especie')
            numero_indicios = dados_item.get('repetir/dados/numero')
    
            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification,  submission_time, individualcount, other_verbatimidentification)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, end_time, numero_indicios, other_especie))


#CAPTURA ANFIBIOS

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='CAPA';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aRVyjz5Pk6gpDmMY4oZEHT/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aRVyjz5Pk6gpDmMY4oZEHT/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result.get('identificacao/eventid')
        project_code = result.get('identificacao/projeto')
        campanha = result.get('identificacao/campanha_amostragem')
        protocolo = result['identificacao/Protocolo']
        valor = result.get('identificacao/Valor_da_Amostragem')
        unidade = result.get('identificacao/Unidade_de_Amostragem')
        esforço = result.get('identificacao/Esfor_o_de_Amostragem')
        locationid = result.get('identificacao/codigo_local')
        localizacao = result.get('identificacao/coordenadas_geo')
        date = result.get('identificacao/Data')
        start_hour = result.get('identificacao/Hora_in_cio')
        end_hour = result.get('identificacao/Hora_fim')
        recordedby = result.get('identificacao/tecnico')
        gps_point = result.get('identificacao/C_digo_GPS')
        dir_vento = result.get('identificacao/dir_vento')
        int_vento = result.get('identificacao/int_vento')
        neb = result.get('identificacao/g_nebulosidade')
        hum = result.get('identificacao/g_hum')
        prep = result.get('identificacao/g_precipitacao')
        temp = result.get('identificacao/temperatura')
        habitat = result.get('identificacao/habitat')
        obs = result.get('identificacao/observacoes')
        submission_time = result.get('_submission_time')
        dados_data = result.get('repetir', [])
        # Parse the end time string into a datetime object

        try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
        except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

        if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
        else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
        if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
        else:
                point_text = None

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, locationid, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, eventdate, eventtime_start, eventtime_end, recordedby, gps_id, wind_direction, type_wind, cloudiness, type_humidity, type_precipitation, temperature, habitat, fieldNotes, submission_time, geometry)  
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, locationid, latitude, longitude, altitude, precisao, date, start_hour, end_hour, recordedby, gps_point, dir_vento, int_vento, neb, hum, prep, temp, habitat, obs, end_time, point_text))

        for dados_item in dados_data:
            especie = dados_item.get('repetir/especie')
            numero_indicios = dados_item.get('repetir/N_mero_Ind_cios')
            other_especie = dados_item.get('repetir/outra_especie')
            occ_photosid = dados_item.get('repetir/C_digo_Fotos')
    
            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification,  submission_time, individualcount, other_verbatimidentification, occ_photosid, locationid, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, end_time, numero_indicios, other_especie, occ_photosid, locationid, latitude, longitude, altitude, precisao))


#TRANSETOS ANFIBIOS

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='TANF';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/aCxXLiaTVF3TRK6PPie2YP/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/aCxXLiaTVF3TRK6PPie2YP/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

# Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']
        eventid = result.get('group_xn9dx42/eventid')
        project_code = result.get('group_xn9dx42/Codigo_projeto')
        campanha = result.get('group_xn9dx42/Campanha_de_amostragem')
        protocolo = result['group_xn9dx42/Protocolo']
        valor = result.get('group_xn9dx42/Valor_da_amostragem')
        unidade = result.get('group_xn9dx42/Unidade_de_amostragem')
        esforço = result.get('group_xn9dx42/Esfor_o_de_amostragem')
        locationid = result.get('group_xn9dx42/Codigo_local')
        date = result.get('group_xn9dx42/Data')
        start_hour = result.get('group_xn9dx42/Hora_in_cio')
        end_hour = result.get('group_xn9dx42/Hora_fim')
        recordedby = result.get('group_xn9dx42/Tecnico')
        dir_vento = result.get('group_xn9dx42/dir_vento')
        int_vento = result.get('group_xn9dx42/int_vento')
        neb = result.get('group_xn9dx42/g_nebulosidade')
        prep = result.get('group_xn9dx42/g_precipitacao')
        temp = result.get('group_xn9dx42/temperatura')
        tipo_zona_hum = result.get('group_xn9dx42/tipo_zona_humida')
        largura = result.get('group_xn9dx42/largura')
        profundidade = result.get('group_xn9dx42/produndidade')
        cobertura_margem = result.get('group_xn9dx42/cobertura_margem')
        cobertura_agua = result.get('group_xn9dx42/cobertura_agua')
        vegetacao_imergente_flutuante = result.get('group_xn9dx42/Vegeta_o_Imergente_ou_Flutuante')
        submission_time = result.get('_submission_time')
        dados_data = result.get('dados', [])
        attachments_data = result.get('attachments', [])
        # Parse the end time string into a datetime object

        try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
        except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

        if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
        else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
        if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
        else:
                point_text = None

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, locationid, eventdate, eventtime_start, eventtime_end, recordedby, wind_direction, type_wind, cloudiness, type_precipitation, temperature, submission_time, type_of_wetland, width, depth, margin_coverage, water_coverage, immersive_vegetation, geometry)  
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, locationid, date, start_hour, end_hour, recordedby, dir_vento, int_vento, neb, prep, temp, end_time, tipo_zona_hum, largura, profundidade, cobertura_margem, cobertura_agua, vegetacao_imergente_flutuante, point_text))

        for dados_item in dados_data:
            especie = dados_item.get('dados/especie')
            n_larvar = dados_item.get('dados/especie')
            n_juvenil = dados_item.get('dados/especie')
            n_adulto = dados_item.get('dados/especie')
            numero_individuos = dados_item.get('dados/Numero_de_individuos')
            other_especie = dados_item.get('dados/outra_especie')
            gps_point = dados_item.get('dados/Codigo_gps')
            occ_photosid = dados_item.get('dados/Codigo_Fotos')
            localizacao = dados_item.get('dados/Localizacao')
            percurso_km = dados_item.get('dados/percurso_km')
    
            cur.execute("""
        INSERT INTO occurrences (id_form, eventid, parenteventid, amostragemid, samplingprotocol, samplesizevalue, samplesizeunit, samplingeffort, verbatimidentification,  submission_time, individualcount, other_verbatimidentification, occ_photosid, gps_id, decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, n_larva, n_juvenile, n_adult, route_km, geometry)   
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,ST_GeomFromText(%s, 4326))
        """, (id_form, eventid, project_code, campanha, protocolo, valor, unidade, esforço, especie, end_time, numero_individuos, other_especie, occ_photosid, gps_point, latitude, longitude, altitude, precisao, n_larvar, n_juvenil, n_adulto, percurso_km, point_text))
            
        # # Extract download_urls
        # for result in data['results']:
        #     for attachment in result['_attachments']:
        #          download_link = attachment.get('download_url')
                

        # cur.execute("""
        # INSERT INTO photos (id_form, eventid, recordedby, link_photo)   
        # VALUES (%s, %s, %s, %s))
        # """, (id_form, eventid, recordedby, download_link))
            

            # Initialize an empty list to store download URLs
    download_urls = []

# Extract download URLs from the response
    if 'results' in data:
        for result in data['results']:
            for attachment in result.get('_attachments', []):
                download_link = attachment.get('download_url')
                if download_link:
                    download_urls.append(download_link)

# Iterate over the download URLs and insert them into the database
    for download_link in download_urls:
        cur.execute("""
            INSERT INTO photos (id_form, eventid, recordedby, link_photo)   
            VALUES (%s, %s, %s, %s)
        """, (id_form, eventid, recordedby, download_link))
            

#TRANSETOS MORTALIDADE DE ANFIBIOS

# Query the most recent date in the table
cur.execute("SELECT MAX(submission_time) FROM occurrences where samplingprotocol='TMAF';")
most_recent_date_result = cur.fetchone()
most_recent_date = most_recent_date_result[0] if most_recent_date_result[0] is not None else None

# Check if most_recent_date is not None
if most_recent_date is not None:
    # Convert the date to the desired format
    most_recent_date = datetime.datetime.strptime(str(most_recent_date), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    # URL of the desired Kobo Toolbox API endpoint to access
    url = f'https://kf.kobotoolbox.org/api/v2/assets/a6nyihNUN7q8AED3ryMbEV/data/?format=json&query={{"_submission_time":{{"$gt": "{most_recent_date}"}}}}'
else:
    url = 'https://kf.kobotoolbox.org/api/v2/assets/a6nyihNUN7q8AED3ryMbEV/data/?format=json'

# Make a GET request to fetch data from the API
response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
   # Now 'data' contains the response data from the API
else:
    print('Error accessing the API. Status code:', response.status_code)

# If there is no data in the database, set most_recent_date to None
if most_recent_date is not None:
    most_recent_date = datetime.datetime.strptime(most_recent_date, '%Y-%m-%dT%H:%M:%S')
    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)
else:
    most_recent_date = None

    # Iterate list
# Check if 'results' key exists in the response
if 'results' in data:
    # Iterate over the list of dictionaries under 'results' key
    for result in data['results']:
        id_form = result['_id']#
        eventid = result['dados_local/eventid']#
        project_code = result['dados_local/Codigo_projeto']#
        campanha = result['dados_local/campanha']#
        protocolo = result['dados_local/Protocolo']#
        valor = result.get('dados_local/Valor_da_amostragem')#
        unidade = result.get('dados_local/Unidade_de_amostragem')#
        esforço = result.get('dados_local/Esfor_o_de_amostragem')#
        date = result['dados_local/Data']#
        start_hour = result['dados_local/Hora_inicio']#
        end_hour = result['dados_local/Hora_fim']#
        location_code = result['dados_local/Codigo_local']#
        recordedby = result['dados_local/Tecnico']#
        type_wind = result['group_yu7xl31/Vento']#
        cloudiness = result['group_yu7xl31/Nebulosidade']#
        type_precipitation = result ['group_yu7xl31/Precipitacao']#
        #visibility = result['group_yu7xl31/Visibilidade']
        temp = result['group_yu7xl31/Temperatura']#
        #humidity = result['group_yu7xl31/Humidade']
        submission_time = result['_submission_time']#
        #submittedby = result['_submitted_by'] or 'N/A'
        dados_data = result.get('Observacoes', [])
        # Parse the end time string into a datetime object

        submission_time = datetime.datetime.strptime(submission_time, '%Y-%m-%dT%H:%M:%S')
        # Now you can format it as needed
        submission_time = submission_time.strftime('%Y-%m-%d %H:%M:%S')

        end_time = datetime.datetime.strptime(submission_time, '%Y-%m-%d %H:%M:%S')
        # Add timezone information to end_time to make it offset-aware
        end_time = end_time.replace(tzinfo=timezone.utc)

        if most_recent_date is None or end_time > most_recent_date:
            cur.execute("""
        INSERT INTO events (
            id_form, eventid, samplingprotocol, parenteventid, amostragemid, samplesizevalue, samplesizeunit, samplingeffort, eventdate, eventtime_start, eventtime_end, 
            locationid, recordedby, type_wind, cloudiness, type_precipitation, 
            temperature, submission_time
        )   
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
    """, (
        id_form, eventid, protocolo, project_code, campanha, valor, unidade, esforço, date, start_hour, end_hour, location_code, recordedby, 
        type_wind, cloudiness, type_precipitation, temp, end_time))

        for dados_item in dados_data:
            especie = dados_item.get('Observacoes/especie')#
            outra_especie = dados_item.get('Observacoes/outra_especie')#
            tipo_obs = dados_item.get('Observacoes/Tipo_de_observacao')#
            outro_indicio = dados_item.get('Observacoes/Outro_indicio')#
            numero_individuos = dados_item.get('Observacoes/Numero_de_individuos')#
            genero = dados_item.get('Observacoes/Genero')#
            classe_etaria = dados_item.get('Observacoes/Classe_etaria')#
            biotopo = dados_item.get('Observacoes/Biotopo')#
            outro_biotopo = dados_item.get('Observacoes/Outro_biotopo')#
            codigo_gps = dados_item.get('Observacoes/Codigo_GPS') #
            localizacao = dados_item.get('Observacoes/Localizacao') #
            estado = dados_item.get('Observacoes/Estado') #
            antiguidade = dados_item.get('Observacoes/Antiguidade') #
            #traumatismos = dados_item.get('Observacoes/Tipo_de_traumatismos')
            indicios_de_predacao = dados_item.get('Observacoes/Indicios_de_predacao') #
            #local_estrada = dados_item.get('Observacoes/Local_estrada') 
            tipo_estado = dados_item.get('Observacoes/Estado_001') 
            

            try:
                x = localizacao.split()
                latitude = float(x[0])
                longitude = float(x[1])
                altitude = float(x[2])
                precisao = float(x[3])
            except:
                latitude = None
                longitude = None
                altitude = None
                precisao = None

            if latitude is not None and longitude is not None:
            # Create a point object using the Shapely library
                point = Point(longitude, latitude)
            else:
                point = None

        # Convert the Shapely object to a PostGIS text representation
            if latitude is not None and longitude is not None:
                point_text = f'POINT({point.x} {point.y})'
            else:
                point_text = None

            # Insert into the database inside the loop
            # Check if most_recent_date is None or the result's date is more recent than the most recent in the database
            if most_recent_date is None or end_time > most_recent_date:
                cur.execute("""
        INSERT INTO occurrences (
            id_form, eventid, samplingprotocol, parenteventid, amostragemid,samplesizevalue, samplesizeunit, samplingeffort,
            locationid, recordedby, verbatimidentification, 
            organismquantitytype,  other_organismQuantityType, individualcount, gender, age_class, biotype, gps_id, 
            decimallatitude, decimallongitude, verbatimelevation, coordinateprecision, 
            state, antiquity, signs_of_predation, other_state, 
            geometry, submission_time, other_biotype
        )   
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, ST_GeomFromText(%s, 4326), %s, %s
        )
    """, (
        id_form, eventid, protocolo, project_code, campanha, valor, unidade, esforço,
        location_code, recordedby, especie,
        tipo_obs,  outro_indicio, numero_individuos, genero, classe_etaria, biotopo, codigo_gps,
        latitude, longitude, altitude, precisao,
        estado, antiguidade, indicios_de_predacao, tipo_estado,
        point_text, end_time, outro_biotopo
    ))


# # Confirm the changes and close the connection
conn.commit()
cur.close()
conn.close()


# Função para executar as consultas SQL
def execute_sql_queries():
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(
            dbname="biota",
            user="postgres",
            password="datamiguel1M",
            host="localhost"
        )
        
        # Criar um cursor para executar as consultas
        cursor = conn.cursor()

        # Consulta SQL 1
        sql_query_1 = """
        UPDATE occurrences ot
        SET "class" = mt."class",
            "order" = mt."order",
            family = mt.family
        FROM species_table mt
        WHERE ot.verbatimidentification = mt.scientific_name
            AND (ot."class" IS NULL OR ot."order" IS NULL OR ot.family IS NULL);
        """
        cursor.execute(sql_query_1)
        
        # Consulta SQL 2
        sql_query_2 = """
        UPDATE occurrences ot
        SET family = mt.family
        FROM flora mt
        WHERE ot.verbatimidentification = mt.scientific_name
            AND (ot.family IS NULL);
        """
        cursor.execute(sql_query_2)

        # Commit para aplicar as alterações ao banco de dados
        conn.commit()
        print("Consultas SQL executadas com sucesso.")

    except psycopg2.Error as e:
        print("Erro ao executar as consultas SQL:", e)
    finally:
        # Fechar o cursor e a conexão
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Chamando a função para executar as consultas SQL
execute_sql_queries()








# # NAME CORRECTION

# #FAUNA

# from fuzzywuzzy import process
# import pandas as pd
# import psycopg2

# # Connect to the PostgreSQL database (replace with your own connection details)
# conn = psycopg2.connect(
#     dbname="biota",
#     user="postgres",
#     password="datamiguel1M",
#     host="localhost"
# )

# query = f"""
# SELECT * FROM occurrences
# WHERE samplingprotocol != 'IFLO';
# """

# query1 = f"""
# select*from species_table;
# """

# # Get data from the database and store it in a pandas DataFrame
# df = pd.read_sql_query(query, conn)
# df1 = pd.read_sql_query(query1, conn)

# # Close the database connection
# conn.close()

# occurrences = df.dropna(subset=['verbatimidentification'])
# fauna = df1.dropna(subset=['scientific_name'])

# occurrences['verbatimidentification'] = occurrences['verbatimidentification'].astype(str)
# fauna['scientific_name'] = fauna['scientific_name'].astype(str)

# # best match
# def find_best_match(name, choices):
#     return process.extractOne(name, choices)[0]

# # Apply function
# occurrences['corrected_name'] = occurrences['verbatimidentification'].apply(find_best_match, choices=fauna['scientific_name'])

# # result
# print(occurrences['corrected_name'])

# # Connect to the PostgreSQL database (replace with your own connection details)
# conn = psycopg2.connect(
#     dbname="biota",
#     user="postgres",
#     password="datamiguel1M",
#     host="localhost"
# )

# # Create a cursor object to execute SQL commands
# cur = conn.cursor()

#  #Iterate through the DataFrame and update the occurrence_table
# for index, row in occurrences.iterrows():
#     scientific_name = row['corrected_name']
#     original_name = row['verbatimidentification']

#     # Use an UPDATE statement to modify the occurrence_table
#     update_query = f"UPDATE occurrences SET verbatimidentification = '{scientific_name}' WHERE verbatimidentification = '{original_name}'"

#     # Execute the query
#     cur.execute(update_query)

# # Commit the changes
# conn.commit()

# # Close communication with the database
# cur.close()
# conn.close()

# #FLORA

# # Connect to the PostgreSQL database (replace with your own connection details)
# conn = psycopg2.connect(
#     dbname="biota",
#     user="postgres",
#     password="datamiguel1M",
#     host="localhost"
# )

# query = f"""
# SELECT * FROM occurrences
# WHERE samplingprotocol = 'IFLO';
# """

# query1 = f"""
# select*from flora;
# """

# # Get data from the database and store it in a pandas DataFrame
# df = pd.read_sql_query(query, conn)
# df1 = pd.read_sql_query(query1, conn)

# # Close the database connection
# conn.close()

# occurrences = df.dropna(subset=['verbatimidentification'])
# flora = df1.dropna(subset=['scientific_name'])

# occurrences['verbatimidentification'] = occurrences['verbatimidentification'].astype(str)
# flora['scientific_name'] = flora['scientific_name'].astype(str)

# # best match
# def find_best_match(name, choices):
#     return process.extractOne(name, choices)[0]

# # Apply function
# occurrences['corrected_name'] = occurrences['verbatimidentification'].apply(find_best_match, choices=flora['scientific_name'])

# # result
# print(occurrences['corrected_name'])

# # Connect to the PostgreSQL database (replace with your own connection details)
# conn = psycopg2.connect(
#     dbname="biota",
#     user="postgres",
#     password="datamiguel1M",
#     host="localhost"
# )

# # Create a cursor object to execute SQL commands
# cur = conn.cursor()

# # Iterate through the DataFrame and update the occurrence_table
# for index, row in occurrences.iterrows():
#     scientific_name = row['corrected_name']
#     original_name = row['verbatimidentification']

#     # Use an UPDATE statement to modify the occurrence_table
#     update_query = f"UPDATE occurrences SET verbatimidentification = '{scientific_name}' WHERE verbatimidentification = '{original_name}'"

#     # Execute the query
#     cur.execute(update_query)

# # Commit the changes
# conn.commit()

# # Close communication with the database
# cur.close()
# conn.close()
