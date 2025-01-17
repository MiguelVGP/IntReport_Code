import geopandas as gpd
import matplotlib.pyplot as plt
from tkinter import filedialog
#from PIL import Image, ImageTk
import psycopg2
import csv
import geopandas as gpd
#from shapely.geometry import Point
#import contextily as cx
#from matplotlib_scalebar.scalebar import ScaleBar
#from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import os
from pyproj import Transformer
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap



class ProtectedAreas:
    """
    A class to handle different types of protected areas and perform calculations on shapefiles.
    """

    def __init__(self, wk_dir):
        """
        Initialize the ProtectedAreas class with a working directory.
        
        Parameters:
        wk_dir (str): The working directory path.
        """
        self.wk_dir = wk_dir
   

    def process_protected_area(self, area_type, project_shapefile):
        """
        Process a specific type of protected area and perform calculations on the project shapefile.
        
        Parameters:
        area_type (str): The type of protected area (e.g., 'ramsar', 'zpe', 'bioesfera', 'sic', 'rnap', 'abrigo','iba').
        project_shapefile (str): The filename of the project shapefile.
        """
        if area_type=='rnap':
            nome=''
        # Read target shapefile
        project = gpd.read_file(os.path.join(self.wk_dir, project_shapefile))
        project.to_crs(epsg='3763')
        # Read reference shapefile for the specific protected area
        reference_shapefile = gpd.read_file(os.path.join(self.wk_dir, f'{area_type}.shp'))
        reference_shapefile.to_crs(epsg='3763')
        # Perform calculations and create results dataframe
        if area_type=='rnap':
            reference_shapefile['nome']=reference_shapefile['classifica']+ ' ' + reference_shapefile['nome_ap']
        elif area_type=='sic' or area_type=='zpe':
            reference_shapefile['nome']=reference_shapefile['site_name']
        elif area_type=='abrigo':
            reference_shapefile['nome']=reference_shapefile['Abrigo']
        elif area_type=='iba':
            reference_shapefile['nome']=reference_shapefile['IBA_NAME']
        else:
            pass

        results_df = pd.DataFrame(reference_shapefile['nome'].tolist(), columns=['name'])
        # print(results_df)
        results_df['distance'] = reference_shapefile.shortest_line(project.loc[0, 'geometry'])
        results_df['overlap'] = reference_shapefile.intersection(project.loc[0, 'geometry'])
        # print(results_df)
        # Fill output table for the specific protected area
        output_table = pd.DataFrame(columns=['tipo', 'nome', 'distancia', 'sobreposicao'])
        for index, row in results_df.iterrows():
            print(row['distance'].length)
            if row['distance'].length <= 20000:
                nome = row['name']
                distancia = row['distance'].length
                if row['overlap']:
                    overlap_area = row['overlap'].area
                else:
                    overlap_area = 0
                output_table.loc[index] = [f'{area_type}', nome, distancia, overlap_area]
        
        return output_table

    def export_to_csv(self, dataframe, output_filename):
        """
        Export the dataframe to a CSV file.
        
        Parameters:
        dataframe (pd.DataFrame): The dataframe to be exported.
        output_filename (str): The filename for the CSV output.
        """
        dataframe.to_csv(os.path.join(self.wk_dir, output_filename), index=False)


def select_output_path():
    file_path = filedialog.askdirectory(title="Select an output directory", mustexist=True)
    global selected_output_path
    if file_path:
        selected_output_path = file_path
        print("Selected output path:", selected_output_path)

def get_variable_name(var, local_vars):
        for name, value in local_vars.items():
            if value == var:
                return name
        return None

def main(parenteventid, amostragemid, locationid, samplingprotocol, output_dir, occ_csv_output, evt_csv_output, graph_output, specieslist_output, stationshape_output, G2_fauna, epsg_destination, G1_graph, tax_level, G2_flora, G7_graph, G8_graph, T9_graph, graph_type, T10_graph, T14_graph, T15_graph, color): #epsg_destination #samplingprotocol
        if len(locationid)>1:
            pass
        else:
            locationid=str(locationid).replace(',','')
        print('location is', locationid)

        if len(amostragemid)>1:
            pass
        else:
            amostragemid=str(amostragemid).replace(',','')
        print('amostragem is', amostragemid)

        if len(samplingprotocol)>1:
            pass
        else:
            samplingprotocol=str(samplingprotocol).replace(',','')
        print('protocol is', samplingprotocol)

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            database="biota",
            user="postgres",
            password="datamiguel1M",
            host="localhost"
        )
        cursor = conn.cursor()

        #query to get all data in a csv

        if occ_csv_output.get()==1:
            query_columns = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'occurrences' ORDER BY ORDINAL_POSITION  "
            cursor.execute(query_columns)
            data_columns = cursor.fetchall()
            query_data = "SELECT * FROM occurrences WHERE "
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query_data += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query_data += f' AND {get_variable_name(par, locals())} IN {par}'
            
                print(query_data)
            cursor.execute(query_data)
            selection_data = cursor.fetchall()
        
        # Save the  data to a CSV file in the output directory
            output_file = f"{output_dir}/Dados das Ocorrências_{amostragemid}_{samplingprotocol}.csv"
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(data_columns)
                for row in selection_data:
                    writer.writerow(row)
        else:
            
            pass 
        
        if evt_csv_output.get()==1:
            query_columns = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'occurrences' ORDER BY ORDINAL_POSITION  "
            cursor.execute(query_columns)
            data_columns = cursor.fetchall()
            query_data = "SELECT * FROM events WHERE "
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query_data += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query_data += f' AND {get_variable_name(par, locals())} IN {par}'
            
                print(query_data)
            cursor.execute(query_data)
            selection_data = cursor.fetchall()
        
        # Save the  data to a CSV file in the output directory
            output_file = f"{output_dir}/Dados dos Eventos_{amostragemid}_{samplingprotocol}.csv"
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(data_columns)
                for row in selection_data:
                    writer.writerow(row)
        else:
            
            pass   
    
        if graph_output.get()==1:
            plt.clf()
            # Get the species richness data from the database
            query = "SELECT locationid, count(Distinct verbatimidentification) FROM occurrences WHERE "
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query += " GROUP BY locationid"
            print(query)
            cursor.execute(query)
            species_richness_data = cursor.fetchall()

            # Create a DataFrame
            df = pd.DataFrame(species_richness_data, columns=['Estação', 'Riqueza'])

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/Riqueza Específica por Estação de Amostragem_{amostragemid}_{samplingprotocol}.csv'
            df.to_csv(csv_file_path, sep=';' ,index=False)

            # Your data
            estacao = df['Estação']
            riqueza = df['Riqueza']

            # Plot the horizontal histogram with percentages on the x-axis
            plt.bar(estacao, riqueza, color=color)
            plt.xlabel('Local de Amostragem')
            plt.ylabel('Nº de Espécies')
            plt.yticks(range(0, max(riqueza)+1, 1))
            plt.tight_layout()
            plt.savefig(f"{output_dir}/Riqueza Específica por Estação de Amostragem_{amostragemid}_{samplingprotocol}.jpg", format='jpg', dpi=300)

        else:
            pass

        if specieslist_output.get()==1:
        # T1 code to extract species list
        # Your SQL query
            query = f"select distinct occurrences.class, occurrences.order, occurrences.family, (occurrences.verbatimidentification), species_table.lvmp, species_table.iucn, species_table.berna, species_table.bona, species_table.cities, species_table.dl_49_2005, species_table.origin  from occurrences inner join species_table on occurrences.verbatimidentification = species_table.scientific_name where "
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            
            print(query)
            cursor.execute(query)
            species_richness_data = cursor.fetchall()    

            # Get data from the database and store it in a pandas DataFrame
            species_list = pd.read_sql_query(query, conn, )
            species_list.columns=['Classe', 'Ordem', 'Família','Espécie', 'LVP', 'IUCN', 'Berna', 'Bona', 'CITES', 'DL n.º 49/2005', 'Origem']

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/Lista de Espécies_{amostragemid}_{samplingprotocol}.csv'
            species_list.to_csv(csv_file_path, sep=';' ,index=False)


            fig, ax = plt.subplots()

            # hide axes
            fig.patch.set_visible(False)

            # Set the background color for the header (column names row) to green
            header_colors = [color] * len(species_list.columns)

            # Create a table with bold black text for data cells and green background for the header
            table = ax.table(cellText=species_list.values,
                            cellLoc='center',
                            colLabels=species_list.columns,
                            colColours=header_colors,
                            cellColours=[['#FFFFFF']*len(species_list.columns) for _ in range(len(species_list))],
                            colLoc='center',
                            loc='bottom',
                            edges='closed', # Use closed edges
                            bbox=[0, 0, 1, 1])  # Set bbox to cover the entire subplot

            # Set the fontweight to bold for the header
            for (i, j), cell in table.get_celld().items():
                if i == 0:  # Assuming the header is in the first row
                    cell.set_text_props(fontweight='bold')

            # Set the edge color to green for all edges
            for key, cell in table.get_celld().items():
                cell.set_edgecolor(color)
            
            # Set the font style to italic for the ESPECIE column
            for (i, j), cell in table.get_celld().items():
                if j == 0:   # Index of the ESPECIE column
                    cell.set_text_props(fontstyle='italic')


            fig.tight_layout()

            ax.axis('off')  # Add this line to show the axes

            # Save the figure to a JPG file
            plt.savefig(f"{output_dir}/Lista de Espécies_{amostragemid}_{samplingprotocol}.jpg", format='jpg', dpi=400)
        else:
            pass
        
        # T18 + create sampling stations shapefile 
        if stationshape_output.get()==1:
            
            query = f"SELECT DISTINCT locationid, decimallatitude, decimallongitude FROM occurrences WHERE "
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query+= ' ORDER BY locationid'
            print(query)
            cursor.execute(query, params)
            stations_data = cursor.fetchall()

            # Get data from the database and store it in a pandas DataFrame
            df = pd.DataFrame(stations_data, columns=['locationid', 'decimallatitude', 'decimallongitude'])


            def transform_coordinates(df, epsg_destination):
                # Define the original coordinate system (EPSG:4326 is the default system for latitude and longitude)
                origin_system = 'epsg:4326'

                # Define the destination coordinate system
                destination_system = f'epsg:{epsg_destination}'

                # Create a Transformer for coordinate transformation
                transformer = Transformer.from_crs(origin_system, destination_system, always_xy=True)

                # Function to transform coordinates
                def transform_row(row):
                    new_longitude, new_latitude = transformer.transform(row['decimallongitude'], row['decimallatitude'])
                    return pd.Series({'decimallatitude': new_latitude, 'decimallongitude': new_longitude})

                # Apply the transformation to each row in the dataframe
                df[['decimallatitude', 'decimallongitude']] = df.apply(transform_row, axis=1)

            # Call the function to transform coordinates to the user-specified EPSG code
            transform_coordinates(df, epsg_destination)

            # Display the dataframe after transformation
            # print(df)

            # Rename columns
            final_df = df.rename(columns={
                'locationid': 'Estação de Amostragem',
                'decimallatitude': 'Latitude',
                'decimallongitude': 'Longitude'
            })

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/Dados Coordenadas das Estações_{amostragemid}_{samplingprotocol}.csv'
            final_df.to_csv(csv_file_path, index=False)

            fig, ax = plt.subplots()

            # hide axes
            fig.patch.set_visible(False)

            # Set the background color for the header (column names row) to green
            header_colors = [color] * len(final_df.columns)

            # Create a table with bold black text for data cells and green background for the header
            table = ax.table(cellText=final_df.values,
                            cellLoc='center',
                            colLabels=final_df.columns,
                            colColours=header_colors,
                            cellColours=[['#FFFFFF']*len(final_df.columns) for _ in range(len(final_df))],
                            colLoc='center',
                            loc='bottom',
                            edges='closed',  # Use closed edges
                            bbox=[0, 0, 1, 1])  # Set bbox to cover the entire subplot

            table.auto_set_font_size(False)  # Turn off auto font size adjustment
            table.set_fontsize(9)  # Set the desired font size

            # Set the fontweight to bold for the header
            for (i, j), cell in table.get_celld().items():
                if i == 0:  # Assuming the header is in the first row
                    cell.set_text_props(fontweight='bold')

            # Set the edge color to green for all edges
            for key, cell in table.get_celld().items():
                cell.set_edgecolor(color)

            fig.tight_layout()

            ax.axis('off')  # Add this line to show the axes

            # Save the figure to a JPG file
            plt.savefig(f"{output_dir}/Coordenadas das Estações_{amostragemid}_{samplingprotocol}.jpg", format='jpg', dpi=300)

            # Create the shapefile

            # Create the GeoDataFrame
            geo_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['decimallongitude'], df['decimallatitude']), crs= epsg_destination)
            
            geo_gdf.to_file(f'{output_dir}/locations_shapefile_{amostragemid}_{samplingprotocol}')


        else: 
            pass

        # G2 FAUNA
        
        if G2_fauna.get()==1:
            plt.clf()
            
            query = f"""
            SELECT st.lvmp,
            COUNT(st.lvmp) AS lvmp_count
            FROM occurrences ot
            INNER JOIN species_table st ON ot.verbatimidentification = st.scientific_name
            WHERE
        """
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query += """GROUP BY st.lvmp
            ORDER BY st.lvmp"""
            print(query)
            cursor.execute(query)
            pie_data = cursor.fetchall()

            # Create a DataFrame
            df = pd.DataFrame(pie_data, columns=['LVMP', 'Count'])

            # Rename columns
            final_table = df.rename(columns={
                'lvmp': 'LVMP',
                'lvmp_count': 'Count',
            })

            # Replace null values in the "LVFP" column with "Not Defined"
            final_table['LVMP'].replace('', 'Not Defined', inplace=True)


            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/pie_data_{amostragemid}_{samplingprotocol}.csv'
            final_table.to_csv(csv_file_path, sep=';' ,index=False)

            # Your data
            lvmp = final_table['LVMP']
            count = final_table['Count']

            import matplotlib.colors as mcolors
            import colorsys
            color_rgb = mcolors.hex2color(color)

            # Convert RGB to HSL
            color_hsl = colorsys.rgb_to_hls(*color_rgb)

            # Diminuir a luminosidade (lightness) da cor
            darken_factor = 0.2  # Adjust this as needed for smoother darkening
            darkened_lightness = max(0, color_hsl[1] - darken_factor)

            # Convert HSL back to RGB
            darkened_color_rgb = colorsys.hls_to_rgb(color_hsl[0], darkened_lightness, color_hsl[2])

            # Converting RGB to HEX
            darkened_color_hex = mcolors.rgb2hex(darkened_color_rgb)

            cmap = LinearSegmentedColormap.from_list('custom_color', [color, darkened_color_hex], N=len(lvmp))

            # Plot the pie chart with custom colors
            colors = [cmap(i) for i in range(len(lvmp))]
            plt.pie(count, labels=lvmp, autopct='%1.1f%%', startangle=140, colors=colors)

            # Salvar e mostrar o gráfico
            plt.savefig(f"{output_dir}/G2_{amostragemid}_{samplingprotocol}_fauna.jpg", format='jpg', dpi=300)

        else: 
            pass


        #G1 Número de Espécies por Grupo Taxonómico

        if G1_graph.get()==1:
            plt.clf()

            # Your SQL query
            if tax_level.upper() == 'F':
                query = f"SELECT family, SUM(individualcount) AS total_individuals FROM occurrences WHERE "

            elif tax_level.upper() == 'O':
                query = f"""SELECT "order", SUM(individualcount) AS total_individuals FROM occurrences WHERE """

            elif tax_level.upper() == 'C':
                query = f"SELECT class, SUM(individualcount) AS total_individuals FROM occurrences WHERE "

            elif tax_level.upper() == 'S':
                query = f"SELECT verbatimidentification, SUM(individualcount) AS total_individuals FROM occurrences WHERE "

            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'

            if tax_level.upper() == 'F':        
                query += """ GROUP BY family
                Order by total_individuals DESC"""
                print(query)
                cursor.execute(query)
                g1_data = cursor.fetchall()

                df = pd.DataFrame(g1_data, columns=['family', 'count'])

            elif tax_level.upper() == 'O':        
                query += """ GROUP BY "order"
                Order by total_individuals DESC;"""
                print(query)
                cursor.execute(query)
                g1_data = cursor.fetchall()

                df = pd.DataFrame(g1_data, columns=['order','count'])

            elif tax_level.upper() == 'C':        
                query += """ GROUP BY "class"
                Order by total_individuals DESC"""
                print(query)
                cursor.execute(query)
                g1_data = cursor.fetchall()

                df = pd.DataFrame(g1_data, columns=['class', 'count'])
                print(df)

            elif tax_level.upper() == 'S':        
                query += """ GROUP BY verbatimidentification
                Order by total_individuals DESC"""
                print(query)
                cursor.execute(query)
                g1_data = cursor.fetchall()

                df = pd.DataFrame(g1_data, columns=['verbatimidentification', 'count'])

            
            # Rename columns
            final_table_nd = df.rename(columns={
                'count': 'Nº de espécies',
                'family': 'Família',
                'class': 'Classe',
                'order': 'Ordem',
                'verbatimidentification': 'Espécie'
                })

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/G1_data_{amostragemid}_{samplingprotocol}.csv'
            final_table_nd.to_csv(csv_file_path, index=False)

            # Your data
            if tax_level.upper() == 'F':
                level = final_table_nd['Família']
            elif tax_level.upper() == 'O':
                level = final_table_nd['Ordem']
            elif tax_level.upper() == 'C':
                level = final_table_nd['Classe']
            elif tax_level.upper() == 'S':
                level = final_table_nd['Espécie']

            count = final_table_nd['Nº de espécies']
            print(count)

            # Plot the horizontal histogram with percentages on the x-axis
            plt.barh(level, count, color=color)
            plt.xlabel('Nº de Indíviduos')

            if tax_level.upper() == 'F':
                plt.ylabel('Família')
            elif tax_level.upper() == 'O':
                plt.ylabel('Ordem')
            elif tax_level.upper() == 'C':
                plt.ylabel('Classe')
            elif tax_level.upper() == 'S':
                plt.ylabel('Espécie')

            plt.xticks(range(0, max(count)+1, 1))
            plt.tight_layout()
            plt.savefig(f"{output_dir}/G1_{amostragemid}_{samplingprotocol}_Número de Indíviduos por Grupo Taxonómico.jpg", format='jpg', dpi=300)
        else:
            pass

        # G2 Representatividade das Espécies FLORA
        if G2_flora.get()==1:
            plt.clf()

                    # Your SQL query
            query = f"""
            SELECT fl.lvfp,
                COUNT(fl.lvfp) AS lvfp_count
            FROM occurrences oc
            INNER JOIN flora fl ON oc.verbatimidentification = fl.scientific_name WHERE
            """

            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query += """ GROUP BY fl.lvfp
            ORDER BY fl.lvfp"""
            print(query)
            cursor.execute(query)
            G2_data = cursor.fetchall()

            # Create a DataFrame
            df = pd.DataFrame(G2_data, columns=['LVFP', 'Count'])

            # Rename columns
            final_table = df.rename(columns={
                'lvfp': 'LVFP',
                'lvfp_count': 'Count',
            })

            # Replace null values in the "LVFP" column with "Not Defined"
            final_table['LVFP'].replace('', 'Not Defined', inplace=True)

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/G2_data_{amostragemid}_{samplingprotocol}.csv'
            final_table.to_csv(csv_file_path, sep=';' ,index=False)

            # Your data
            lvfp = final_table['LVFP']
            count = final_table['Count']

            import matplotlib.colors as mcolors
            # Convert hexadecimal color to RGB tuple
            import colorsys

            color_rgb = mcolors.hex2color(color)

            # Convert RGB to HSL
            color_hsl = colorsys.rgb_to_hls(*color_rgb)

            # Diminuir a luminosidade (lightness) da cor
            darken_factor = 0.2  # Adjust this as needed for smoother darkening
            darkened_lightness = max(0, color_hsl[1] - darken_factor)

            # Convert HSL back to RGB
            darkened_color_rgb = colorsys.hls_to_rgb(color_hsl[0], darkened_lightness, color_hsl[2])

            # Converting RGB to HEX
            darkened_color_hex = mcolors.rgb2hex(darkened_color_rgb)

            cmap = LinearSegmentedColormap.from_list('custom_color', [color, darkened_color_hex], N=len(lvfp))

            # Plot the pie chart with custom colors
            colors = [cmap(i) for i in range(len(lvfp))]
            plt.pie(count, labels=lvfp, autopct='%1.1f%%', startangle=140, colors=colors)

            # Salvar e mostrar o gráfico
            plt.savefig(f"{output_dir}/G2_{amostragemid}_{samplingprotocol}_flora.jpg", format='jpg', dpi=300)

        else: 
            pass


        # G7 Abundância por Estação de Amostragem
        if G7_graph.get()==1:
            plt.clf()

            # Your SQL query
            query = f"""
            SELECT locationid, SUM(individualcount) as total_count
            FROM occurrences
            WHERE
            """

            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query += """ GROUP BY locationid
            Order by locationid"""
            print(query)
            cursor.execute(query)
            G7_data = cursor.fetchall()

            # Create a DataFrame
            df = pd.DataFrame(G7_data, columns=['Estação', 'Nº de Indíviduos'])

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/G7_data_{amostragemid}_{samplingprotocol}.csv'
            df.to_csv(csv_file_path, sep=';' ,index=False)


                        # Your data
            estacao = df['Estação']
            indicios = df['Nº de Indíviduos']

            # Plot the horizontal histogram with percentages on the x-axis
            plt.bar(estacao, indicios, color=color)
            plt.xlabel('Local de Amostragem')
            plt.ylabel('Nº de Indíviduos')
            plt.yticks(range(0, max(indicios)+1, 1))
            plt.tight_layout()
            plt.savefig(f"{output_dir}/G7_{amostragemid}_{samplingprotocol}_Abundância por Estação de Amostragem.jpg", format='jpg', dpi=300)

        else: 
            pass

            # G8 Nº espécies por categoria
        if G8_graph.get()==1:
            plt.clf()

            query = f"""
            SELECT fl.naturalness,
                COUNT(fl.naturalness) AS naturalness_count
            FROM occurrences oc
            INNER JOIN flora fl ON oc.verbatimidentification = fl.scientific_name
            WHERE
            """
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query += """ GROUP BY fl.naturalness
            ORDER BY fl.naturalness"""
            print(query)
            cursor.execute(query)
            G8_data = cursor.fetchall()


            # Create a DataFrame
            df = pd.DataFrame(G8_data, columns=['Naturalidade', 'Número de Espécies'])

            df['Naturalidade'].replace('', 'Não definido', inplace=True)

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/G8_data_{amostragemid}_{samplingprotocol}.csv'
            df.to_csv(csv_file_path, sep=';' ,index=False)


                        # Your data
            estacao = df['Naturalidade']
            indicios = df['Número de Espécies']

            # Plot the horizontal histogram with percentages on the x-axis
            plt.bar(estacao, indicios, color=color)
            plt.xlabel('Naturalidade')
            plt.ylabel('Número de Espécies')
            plt.yticks(range(0, max(indicios)+1, 1))
            plt.tight_layout()
            plt.savefig(f"{output_dir}/G8_{amostragemid}_{samplingprotocol}_Nº espécies por categoria.jpg", format='jpg', dpi=300)

        else: 
            pass

        if T9_graph.get()==1:
            plt.clf()


            # Your SQL query
            query = f"""
            SELECT verbatimidentification, SUM(individualcount) AS total_individuals FROM occurrences
            WHERE
            """


            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query += """ GROUP BY verbatimidentification
            Order by total_individuals DESC;"""
            print(query)
            cursor.execute(query)
            T9_data = cursor.fetchall()

            # Create a DataFrame
            df = pd.DataFrame(T9_data, columns=['Espécies', 'Abundância'])

            df['Espécies'].replace('', 'Não definido', inplace=True)

                        # Your data
            species = df['Espécies']
            abundance = df['Abundância']

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/T9_{amostragemid}_{samplingprotocol}.csv'
            df.to_csv(csv_file_path, index=False)

            # Check the input and create the corresponding graph
            if graph_type.upper() == 'R':
                # Calculate percentages in relation to total abundance
                percentage = abundance / abundance.sum() * 100
                # Plot the horizontal histogram with percentages on the x-axis
                plt.barh(species, percentage, color=color)
                plt.xlabel('Abudância Relativa(%)')
                plt.ylabel('Espécie')
                plt.tight_layout()
                plt.savefig(f"{output_dir}/T9_{amostragemid}_{samplingprotocol}_Abundância Relativa por Espécie.jpg", format='jpg', dpi=300)
                

            elif graph_type.upper() == 'A':
                # Plot the horizontal histogram with absolute abundance
                import matplotlib.ticker as ticker
                plt.barh(species, abundance, color=color)
                plt.xlabel('Abundância Absoluta')
                plt.ylabel('Espécie')
                plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
                plt.tight_layout()
                plt.savefig(f"{output_dir}/T9_{amostragemid}_{samplingprotocol}_Abundância Absoluta por Espécie.jpg", format='jpg', dpi=300)

        else: 
            pass

        if T10_graph.get()==1:
            plt.clf()

            query = f"""
            SELECT verbatimidentification, SUM(individualcount) as total_count
            FROM occurrences
            WHERE
            """

            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'
            query += """ GROUP BY verbatimidentification"""
            print(query)
            cursor.execute(query)
            T10_data = cursor.fetchall()

            # Create a DataFrame
            df = pd.DataFrame(T10_data, columns=['Espécies', 'Contagem Total'])

            df['Espécies'].replace('', 'Não definido', inplace=True)

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/T10_data_{amostragemid}_{samplingprotocol}.csv'
            df.to_csv(csv_file_path, index=False)

            fig, ax = plt.subplots()

            # hide axes
            fig.patch.set_visible(False)

            # Set the background color for the header (column names row) to green
            header_colors = [color] * len(df.columns)

            # Create a table with bold black text for data cells and green background for the header
            table = ax.table(cellText=df.values,
                            cellLoc='center',
                            colLabels=df.columns,
                            colColours=header_colors,
                            cellColours=[['#FFFFFF']*len(df.columns) for _ in range(len(df))],
                            colLoc='center',
                            loc='bottom',
                            edges='closed',  # Use closed edges
                            bbox=[0, 0, 1, 1])  # Set bbox to cover the entire subplot

            table.auto_set_font_size(False)  # Turn off auto font size adjustment
            table.set_fontsize(9)  # Set the desired font size

            # Set the fontweight to bold for the header
            for (i, j), cell in table.get_celld().items():
                if i == 0:  # Assuming the header is in the first row
                    cell.set_text_props(fontweight='bold')

            # Set the edge color to green for all edges
            for key, cell in table.get_celld().items():
                cell.set_edgecolor(color)

            fig.tight_layout()

            ax.axis('off')  # Add this line to show the axes

            plt.savefig(f"{output_dir}/T10_{amostragemid}_{samplingprotocol}_Distribuição de Espécies.jpg", format='jpg', dpi=300)

        else: 
            pass

        if T14_graph.get()==1:
            plt.clf()

            query = f"""
            SELECT species_table.class,
            COUNT(CASE WHEN lvmp = 'NE' THEN 1 END) AS lvmp_NE,
            COUNT(CASE WHEN lvmp = 'NA' THEN 1 END) AS lvmp_NA,
            COUNT(CASE WHEN lvmp = 'DD' THEN 1 END) AS lvmp_DD,
            COUNT(CASE WHEN lvmp = 'LC' THEN 1 END) AS lvmp_LC,
            COUNT(CASE WHEN lvmp = 'NT' THEN 1 END) AS lvmp_NT,
            COUNT(CASE WHEN lvmp = 'VU' THEN 1 END) AS lvmp_VU,
            COUNT(CASE WHEN lvmp = 'EN' THEN 1 END) AS lvmp_EN,
            COUNT(CASE WHEN lvmp = 'CR' THEN 1 END) AS lvmp_CR,
            COUNT(CASE WHEN lvmp = 'RE' THEN 1 END) AS lvmp_RE,
            COUNT(CASE WHEN iucn = 'NE' THEN 1 END) AS iucn_NE,
            COUNT(CASE WHEN iucn = 'NA' THEN 1 END) AS iucn_NA,
            COUNT(CASE WHEN iucn = 'DD' THEN 1 END) AS iucn_DD,
            COUNT(CASE WHEN iucn = 'LC' THEN 1 END) AS iucn_LC,
            COUNT(CASE WHEN iucn = 'NT' THEN 1 END) AS iucn_NT,
            COUNT(CASE WHEN iucn = 'VU' THEN 1 END) AS iucn_VU,
            COUNT(CASE WHEN iucn = 'EN' THEN 1 END) AS iucn_EN,
            COUNT(CASE WHEN iucn = 'CR' THEN 1 END) AS iucn_CR,
            COUNT(CASE WHEN iucn = 'RE' THEN 1 END) AS iucn_RE,
            COUNT(CASE WHEN berna = 'II' THEN 1 END) AS berna_II,
            COUNT(CASE WHEN berna = 'III' THEN 1 END) AS berna_III,
            COUNT(CASE WHEN bona = 'I' THEN 1 END) AS bona_I,
            COUNT(CASE WHEN bona = 'II' THEN 1 END) AS bona_II,
            COUNT(CASE WHEN cities = 'A-I' THEN 1 END) AS cities_A_I,
            COUNT(CASE WHEN cities = 'A-II' THEN 1 END) AS cities_A_II,
            COUNT(CASE WHEN cities = 'A' THEN 1 END) AS cities_A,
            COUNT(CASE WHEN cities = 'B-II' THEN 1 END) AS cities_B_II,
            COUNT(CASE WHEN cities = 'C' THEN 1 END) AS cities_C,
            COUNT(CASE WHEN cities = 'C-III' THEN 1 END) AS cities_C_III,
            COUNT(CASE WHEN cities = 'D' THEN 1 END) AS cities_D,
            COUNT(CASE WHEN dl_49_2005 = 'A-I' THEN 1 END) AS dl_49_2005_A_I,
            COUNT(CASE WHEN dl_49_2005 = 'A-III' THEN 1 END) AS dl_49_2005_A_III,
            COUNT(CASE WHEN dl_49_2005 = 'B-II' THEN 1 END) AS dl_49_2005_B_II,
            COUNT(CASE WHEN dl_49_2005 = 'B-IV' THEN 1 END) AS dl_49_2005_B_IV,
            COUNT(CASE WHEN dl_49_2005 = 'B-V' THEN 1 END) AS dl_49_2005_B_V,
            COUNT(CASE WHEN dl_49_2005 = 'D' THEN 1 END) AS dl_49_2005_D
            FROM species_table
            INNER JOIN occurrences ON occurrences.verbatimidentification = species_table.scientific_name
            WHERE


            """


            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'

            query += """ 
            GROUP BY species_table.class
	        ORDER BY species_table.class

            """

            print(query)
            cursor.execute(query)


            df = pd.read_sql_query(query, conn)

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/T14_data_{amostragemid}_{samplingprotocol}.csv'
            df.to_csv(csv_file_path, index=False)

        else: 
            pass

        if T15_graph.get()==1:
            plt.clf()

            query = f"""
            select Distinct amostragemid,eventdate,eventtime_start, eventtime_end
            from events
            WHERE
            """
            params = [parenteventid,  locationid, amostragemid, samplingprotocol]
            for x, par in enumerate(params):
                if x==0:
                    query += f'{get_variable_name(par, locals())} IN ({par})'
                else:
                    query += f' AND {get_variable_name(par, locals())} IN {par}'

            query += """ 
            ORDER BY amostragemid

            """

            print(query)
            cursor.execute(query)
            T15_data = cursor.fetchall()

            df = pd.DataFrame(T15_data, columns=['Amostragem', 'Data', 'Horário do Ínicio', 'Horário do Fim'])

            # Converta a coluna de data para o tipo datetime se ainda não estiver no formato certo
            df['Data'] = pd.to_datetime(df['Data'])

            # Use o método strftime para formatar a coluna de data como uma string
            df['Data'] = df['Data'].dt.strftime('%d/%m/%Y')

            # Save the DataFrame to a CSV file
            csv_file_path = f'{output_dir}/T15_data_{amostragemid}_{samplingprotocol}.csv'
            df.to_csv(csv_file_path, index=False)

            fig, ax = plt.subplots()

            # hide axes
            fig.patch.set_visible(False)

            # Set the background color for the header (column names row) to green
            header_colors = [color] * len(df.columns)

            # Create a table with bold black text for data cells and green background for the header
            table = ax.table(cellText=df.values,
                            cellLoc='center',
                            colLabels=df.columns,
                            colColours=header_colors,
                            cellColours=[['#FFFFFF']*len(df.columns) for _ in range(len(df))],
                            colLoc='center',
                            loc='bottom',
                            edges='closed',  # Use closed edges
                            bbox=[0, 0, 1, 1])  # Set bbox to cover the entire subplot

            table.auto_set_font_size(False)  # Turn off auto font size adjustment
            table.set_fontsize(9)  # Set the desired font size

            # Set the fontweight to bold for the header
            for (i, j), cell in table.get_celld().items():
                if i == 0:  # Assuming the header is in the first row
                    cell.set_text_props(fontweight='bold')

            # Set the edge color to green for all edges
            for key, cell in table.get_celld().items():
                cell.set_edgecolor(color)

            fig.tight_layout()

            ax.axis('off')  # Add this line to show the axes

            plt.savefig(f"{output_dir}/T15_{amostragemid}_{samplingprotocol}_Datas de realização das Amostragens_Esforço de Amostragem.jpg", format='jpg', dpi=300)



