import pandas as pd
import geopandas as gpd
#import itertools
#import matplotlib.pyplot as plt
#from tkinter import simpledialog
from tkinter import Tk,Toplevel, Frame, Label, Button, Checkbutton, filedialog, messagebox, PhotoImage, Listbox, Scrollbar, IntVar
import tkinter as tk
#from ttkthemes import ThemedStyle
from PIL import Image, ImageTk
import psycopg2
#import csv
import geopandas as gpd
#from shapely.geometry import Point
#import contextily as cx
#from matplotlib_scalebar.scalebar import ScaleBar
#from matplotlib.offsetbox import OffsetImage, AnnotationBbox
#import os
#from pyproj import Transformer
from aux_functions_v6 import ProtectedAreas, main
#import rasterio.sample

# Main window
root = Tk()
root.title("BIOTA OUTPUT APP")
root.geometry("600x600")

# Set background color to dark green
root.configure(bg="#92d050")

window_width = root.winfo_width()
window_height = root.winfo_height()
photo = PhotoImage(file=r"biota.png")
root.wm_iconphoto(False, photo)



def button1():
    biota_img = ImageTk.PhotoImage(Image.open(r'biota.png'))
    window = Toplevel(root)
    window.wm_iconphoto(False, photo)
        #Create frame for outputs generation
    frame_outputs = Frame(window, width=500, height=500)
    frame_outputs.grid(sticky=('N', 'S', 'E', 'W'))  

    # Set background color of the frame to green
    frame_outputs.configure(bg="#92d050")
    
    def select_output_path():
        file_path = filedialog.askdirectory(title="Escolher onde guardar os outputs", mustexist=True)
        global selected_output_path
        if file_path:
            selected_output_path = file_path
            print("Selected output path:", selected_output_path)

    def select_project(event):
        global selected_project
        selected_project = projects_listbox.get(projects_listbox.curselection())
        print(selected_project)
    
    def select_campaigns():

        if selected_project:
        # Clear the campaigns listbox to avoid duplicates
            campaigns_listbox.delete(0, 'end')

        # Query the database to retrieve campaigns for the selected project
        conn = psycopg2.connect(
            database="biota",
            user="postgres",
            password="datamiguel1M",
            host="localhost"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT distinct amostragemid FROM events WHERE parenteventid IN ('" + selected_project +"')") 
        campaigns_list = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Add campaign options to the listbox
        for campaign in campaigns_list:
            campaigns_listbox.insert('end', campaign[0])  


    def select_stations():
        
        if selected_project:
            # Clear the stations listboxes to avoid duplicates
            stations_listbox.delete(0, 'end')

            # Query the database to retrieve stations for the selected project
            conn = psycopg2.connect(
                database="biota",
                user="postgres",
                password="datamiguel1M",
                host="localhost"
            )
            cursor = conn.cursor()
            cursor.execute("SELECT distinct locationid FROM events WHERE parenteventid IN ('" + selected_project +"')") 
            stations_list = cursor.fetchall()
            # print(stations_list)
            cursor.close()
            conn.close()

            # Add campaigns options to the listbox
            for station in stations_list:
                stations_listbox.insert('end', station)
            # print(stations_listbox)
    
    def select_protocol():

        if selected_project:
            # Clear the stations listboxes to avoid duplicates
            protocol_listbox.delete(0, 'end')

            # Query the database to retrieve stations for the selected project
            conn = psycopg2.connect(
                database="biota",
                user="postgres",
                password="datamiguel1M",
                host="localhost"
            )
            cursor = conn.cursor()
            cursor.execute("SELECT distinct samplingprotocol FROM events WHERE parenteventid IN ('" + selected_project +"')") 
            protocol_list = cursor.fetchall()
            cursor.close()
            conn.close()

            # Add campaigns options to the listbox
            for protocol in protocol_list:
                protocol_listbox.insert('end', protocol)
            # print(stations_listbox)
    def pop_up_all():
        messagebox.showinfo("Parâmetros","Confirma que todos os 4 elementos foram selecionados?")

    def epsg_null():
        global epsg_destination
        epsg_destination = None

    def tax_level_null():
        global tax_level
        tax_level = None

    def graph_type_null():
        global graph_type
        graph_type = None

    def color_null():
        global color
        color = None

    #epsg_destination = None
    def select_epsg():
        global epsg_destination
        epsg_destination = None  # Define inicialmente como nulo

        def select_epsg_value(epsg_value):
            global epsg_destination
            epsg_destination = epsg_value
            popup_window.destroy()

        popup_window = tk.Toplevel(root)
        popup_window.title("Escolher o código EPSG")
        popup_window.configure(bg="#92d050")
        popup_window.wm_iconphoto(False, photo)

        button_3857 = tk.Button(popup_window, text="EPSG 3857", command=lambda: select_epsg_value(3857), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        button_3857.pack(pady=10)

        button_4326 = tk.Button(popup_window, text="EPSG 4326", command=lambda: select_epsg_value(4326), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        button_4326.pack(pady=10)

    def select_tax_level():
        global tax_level
        tax_level = None  # Define inicialmente como nulo

        def select_tax_value(tax_value):
            global tax_level
            tax_level = tax_value
            popup_window.destroy()

        popup_window = tk.Toplevel(root)
        popup_window.title("Escolher Nível Taxonómico")
        popup_window.configure(bg="#92d050")
        popup_window.wm_iconphoto(False, photo)

        button_class = tk.Button(popup_window, text="Classe", command=lambda: select_tax_value('C'), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        button_class.pack(pady=10)

        button_order = tk.Button(popup_window, text="Ordem", command=lambda: select_tax_value('O'), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        button_order.pack(pady=10)

        button_family = tk.Button(popup_window, text="Família", command=lambda: select_tax_value('F'), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        button_family.pack(pady=10)

        button_species = tk.Button(popup_window, text="Espécie", command=lambda: select_tax_value('S'), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        button_species.pack(pady=10)

    def select_graph_type():
        global graph_type
        graph_type = None  # Define inicialmente como nulo

        def select_graph_type_value(graph_type_value):
            global graph_type
            graph_type = graph_type_value
            popup_window.destroy()

        popup_window = tk.Toplevel(root)
        popup_window.title("Escolher tipo de Abundância")
        popup_window.configure(bg="#92d050")
        popup_window.wm_iconphoto(False, photo)

        relativa = tk.Button(popup_window, text="Abundância Relativa", command=lambda: select_graph_type_value('R'), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        relativa.pack(pady=10)

        absoluta = tk.Button(popup_window, text="Abundância Absoluta", command=lambda: select_graph_type_value('A'), bg="#5d872f", fg="white", font=("Arial", 11, "bold"))
        absoluta.pack(pady=10)


    def select_color():
        global color
        color = None  # Define inicialmente como nulo

        def select_color_type(color_value):
            global color
            color = color_value
            popup_window.destroy()

        popup_window = tk.Toplevel(root)
        popup_window.title("Escolher Cor")
        popup_window.configure(bg="#6c9144")
        popup_window.wm_iconphoto(False, photo)

        verde_biota = tk.Button(popup_window, text="Verde BIOTA", command=lambda: select_color_type('#92d050'), bg="#92d050", fg="white", font=("Arial", 11, "bold"))
        verde_biota.pack(pady=5)

        azul = tk.Button(popup_window, text="Azul", command=lambda: select_color_type('#484b96'), bg="#484b96", fg="white", font=("Arial", 11, "bold"))
        azul.pack(pady=5)

        laranja = tk.Button(popup_window, text="Laranja", command=lambda: select_color_type('#db8a18'), bg="#db8a18", fg="white", font=("Arial", 11, "bold"))
        laranja.pack(pady=5)

        vermelho = tk.Button(popup_window, text="Vermelho", command=lambda: select_color_type('#d41608'), bg="#d41608", fg="white", font=("Arial", 11, "bold"))
        vermelho.pack(pady=5)

        outro_label = tk.Label(popup_window, text="Outra:", bg="#6c9144", fg="white", font=("Arial", 11, "bold"))
        outro_label.pack(pady=5)

        custom_color_entry = tk.Entry(popup_window, bg="white", fg="black", font=("Arial", 11))
        custom_color_entry.pack(padx=5)

        confirm_button = tk.Button(popup_window, text="Confirmar", command=lambda: select_color_type(custom_color_entry.get()), bg="gray", fg="white", font=("Arial", 8, "bold"))
        confirm_button.pack(pady=5)

    
    # Label for image description
    image_description = Label(frame_outputs, image=biota_img, text="Select your data:", justify='center',anchor="center")
    image_description.grid(row=1, column=0, sticky=('N', 'W', 'E', 'S'))
    image_description.configure(bg="#92d050")

    # create frame for data selection
    frame0 = Frame(frame_outputs, width=600, height=600)
    frame0.grid(sticky=('N', 'S', 'E', 'W'))  
    frame0.configure(bg="#92d050")

    bold_font = ("Arial", 10, "bold")
    # Projects Listbox
    label1 = Label(frame0, text="Projetos:", font=bold_font)
    label1.grid(padx=(20,0),row=0, column=0)
    label1.configure(bg="#92d050")

    # Create a listbox to store the unique project option
    projects_listbox = Listbox(frame0, selectmode='single', exportselection=False)
    projects_listbox.grid(padx=(20,0),row=1, column=0,  sticky=('N', 'W', 'E', 'S'))
    projects_scrollbar=Scrollbar(frame0,orient="vertical", command=projects_listbox.yview)
    projects_scrollbar.grid(row=1, column=1, sticky=('N','S'))
    projects_listbox['yscrollcommand']=projects_scrollbar.set

    # Protocol Listbox
    label4 = Label(frame0, text="Protocolos:", font=bold_font)
    label4.grid(padx=(0,10), row=0, column=5)
    label4.configure(bg="#92d050")

    # Create a listbox to store the unique protocol option
    protocol_listbox = Listbox(frame0, selectmode='multiple', exportselection=False)
    protocol_listbox.grid(padx=(70,90),row=1, column=5,  sticky=('N', 'W', 'E', 'S'))
    protocol_scrollbar=Scrollbar(frame0,orient="vertical", command=protocol_listbox.yview)
    protocol_scrollbar.grid(padx=(125,0),row=1, column=5, sticky=('N','S'))
    protocol_listbox['yscrollcommand']=protocol_scrollbar.set

    def generate_results():
        locationid = ()
        amostragemid = ()
        samplingprotocol = ()

        missing_params = []

        if not selected_project:
            missing_params.append("Projeto")
        if not stations_listbox.curselection():
            missing_params.append("Estações")
        if not campaigns_listbox.curselection():
            missing_params.append("Campanhas")
        if not protocol_listbox.curselection():
            missing_params.append("Protocolos")
        if not selected_output_path:
            missing_params.append("Local para armazenar outputs")
        if T9.get() and not graph_type:
            missing_params.append("Graph type")
        if StationShapefile.get() and not epsg_destination:
            missing_params.append("Código EPSG")
        if G1.get() and not tax_level:
            missing_params.append("Nível Taxonómico")
        if (RichGraph.get() or SpeciesList.get() or StationShapefile.get() or Pie.get() or G1.get() or G2.get() or G7.get() or G8.get() or T9.get() or T10.get() or T14.get() or T15.get()) and not color:
            missing_params.append("Cor")

        if missing_params:
            messagebox.showerror("Erro", f"Parâmetros ausentes: {', '.join(missing_params)}. Por favor, forneça todos os parâmetros necessários.")
            return

        parenteventid = "'" + selected_project + "'"
        print("this is", parenteventid)

        for index in stations_listbox.curselection():
            locationid+=(stations_listbox.get(index))

        for index in campaigns_listbox.curselection():
            amostragemid+=(campaigns_listbox.get(index),)
        
        for index in protocol_listbox.curselection():
            samplingprotocol+=(protocol_listbox.get(index))

        output_dir = selected_output_path
        occ_csv_output = occ_DataCsv
        evt_csv_output = evt_DataCsv
        graph_output = RichGraph
        specieslist_output = SpeciesList
        stationshape_output = StationShapefile
        G2_fauna = Pie
        G1_graph = G1
        G2_flora = G2
        G7_graph = G7
        G8_graph = G8
        T9_graph = T9
        T10_graph = T10
        T14_graph = T14
        T15_graph = T15

        # Confirmation dialog
        confirmation = messagebox.askquestion("Confirmação", "Tem a certeza de que deseja prosseguir com a geração dos resultados?")
    
        if confirmation == "yes":
        # Proceed with generating the results
            try:
                main(parenteventid, amostragemid, locationid, samplingprotocol, output_dir, occ_csv_output, evt_csv_output,
                 graph_output, specieslist_output, stationshape_output, G2_fauna, epsg_destination, G1_graph, tax_level,
                 G2_flora, G7_graph, G8_graph, T9_graph, graph_type, T10_graph, T14_graph, T15_graph, color)
                messagebox.showinfo("Processo Completo", "Outputs gerados com sucesso!")
            except Exception as e:
                messagebox.showerror("Erro", f"Um erro ocorreu: {e}")
                print(e)
        else:
        # User chose not to proceed
            return

    # Connect to database and get project list
    conn = psycopg2.connect(
        database="biota",
        user="postgres",
        password="datamiguel1M",
        host="localhost"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT distinct parenteventid FROM events order by parenteventid")
    projects_data = cursor.fetchall()
    cursor.close()
    conn.close()


    # Add project options to the listbox
    for project in projects_data:
        projects_listbox.insert('end', project[0])

    # Event handler for selecting a project from the listbox
    projects_listbox.bind('<<ListboxSelect>>',  select_project)
    # selected_project = projects_listbox.get(projects_listbox.curselection())
    # print(selected_project)

    # Create a listbox to store the campaigns options
    
    label2 = Label(frame0, text="Campanhas:", font=bold_font)
    label2.grid(padx=(60,0),row=0, column=2)
    label2.configure(bg="#92d050")
    campaigns_listbox = Listbox(frame0, selectmode='multiple', exportselection=False)
    campaigns_listbox.grid(padx=(50,0), row=1, column=2,  sticky=('N', 'W', 'E', 'S'))
    
    campaigns_scrollbar=Scrollbar(frame0,orient="vertical", command=campaigns_listbox.yview)
    campaigns_scrollbar.grid(row=1, column=3, sticky=('N','S'))
    campaigns_listbox['yscrollcommand']=campaigns_scrollbar.set

    #TODO  Connect to database and get campaign list ##TEM de ser uma função tal como para a station

    # Campaigns Listbox
    def select_all_campaigns():
    # Get the number of items in the listbox
        num_items = campaigns_listbox.size()
    # Select all items in the listbox
        campaigns_listbox.selection_set(0, num_items-1)

    # Stations Listbox
    def select_all_stations():
    # Get the number of items in the listbox
        num_items = stations_listbox.size()
    # Select all items in the listbox
        stations_listbox.selection_set(0, num_items-1)

    # Stations Listbox
    def select_all_protocol():
    # Get the number of items in the listbox
        num_items = protocol_listbox.size()
    # Select all items in the listbox
        protocol_listbox.selection_set(0, num_items-1)

    def start():
        epsg_null()
        tax_level_null()
        select_campaigns()
        graph_type_null()
        color_null()
    
    label3 = Label(frame0, text="Estações:", font=bold_font)
    label3.grid(padx=(70,0),row=0, column=4)
    label3.configure(bg="#92d050")
    stations_listbox = Listbox(frame0, selectmode='multiple')
    stations_listbox.grid(padx=(50,0),row=1, column=4,  sticky=('N', 'W', 'E', 'S'))
    stations_scrollbar=Scrollbar(frame0,orient="vertical", command=stations_listbox.yview)
    stations_scrollbar.grid(padx=(0,300),row=1, column=5, sticky=('N','S'))

    select_all_button_campaigns = Button(frame0, text="Selecionar tudo", command=select_all_campaigns, bg="#5d872f", fg="white", font=("Arial", 8, "bold"))
    select_all_button_campaigns.grid(padx=(59,0),row=2, column=2)

    select_all_button_stations = Button(frame0, text="Selecionar tudo", command=select_all_stations, bg="#5d872f", fg="white", font=("Arial", 8, "bold"))
    select_all_button_stations.grid(padx=(65,0),row=2, column=4)

    select_all_button_protocol = Button(frame0, text="Selecionar tudo", command=select_all_protocol, bg="#5d872f", fg="white", font=("Arial", 8, "bold"))
    select_all_button_protocol.grid(padx=(0,10), row=2, column=5)

    # Get stations list button TODO: transform this in select campaign
    project_button = Button(
        frame0, text="Escolher Projeto", command=start, bg="#5d872f", fg="white", font=bold_font)
    project_button.grid(padx=(27,0),row=6, column=0)

    stations_button = Button(
        frame0, text="Escolher Estações", command=select_protocol, bg="#5d872f", fg="white", font=bold_font)
    stations_button.grid(padx=(60,0),row=6, column=4)

    # Get campaign  list button TODO: transform this snippet in select stations
    campaign_button = Button(
        frame0, text="Escolher Campanhas", command=select_stations, bg="#5d872f", fg="white", font=bold_font)
    campaign_button.grid(padx=(59,0),row=6, column=2)

    # Get campaign  list button TODO: transform this snippet in select stations
    protocol_button = Button(
        frame0, text="Escolher Protocolos", command=pop_up_all, bg="#5d872f", fg="white", font=bold_font)
    protocol_button.grid(padx=(0,10),row=6, column=5)

    #create frame for output specs
    frame1=Frame(frame_outputs, pady=25, width=100, height=100)
    frame1.grid(sticky=('N','W', 'E', 'S'))  
    frame1.configure(bg="#92d050")
        # Projects Listbox
    label1 = Label(frame1, text="Escolha os Outputs:", font=bold_font, bg="#5d872f", fg="white")
    label1.grid(padx=(20,380),row=2, column=0)

    #create data csv option
    occ_DataCsv=IntVar()
    occ_datacsv=Checkbutton(frame1,variable=occ_DataCsv, text='Exportar Occorrências para um CSV',font=bold_font, bg="#92d050")
    occ_datacsv.grid(row=3, column=0, sticky='w')

    #create data csv option
    evt_DataCsv=IntVar()
    evt_datacsv=Checkbutton(frame1,variable=evt_DataCsv, text='Exportar Eventos para um CSV',font=bold_font, bg="#92d050")
    evt_datacsv.grid(row=3, column=1, sticky='w')
        
    #create richness graph  option
    RichGraph=IntVar()
    richgraph=Checkbutton(frame1,variable=RichGraph, text='Exportar Gráfico Riqueza Específica', font=bold_font, bg="#92d050")
    richgraph.grid(row=4, column=0, sticky='w')

    #create species list table option
    SpeciesList=IntVar()
    specieslist=Checkbutton(frame1,variable=SpeciesList, text='Exportar Lista de Espécies', font=bold_font, bg="#92d050")
    specieslist.grid(row=4, column=1, sticky='w')

    #create sampling stations shapefile
    StationShapefile=IntVar()
    stationshapefile=Checkbutton(frame1,variable=StationShapefile, text='Exportar Tabela com Estações + Shapefile', font=bold_font, bg="#92d050")
    stationshapefile.grid(row=5, column=0, sticky='w')

    select_epsg_button = Button(frame1, text="Código EPSG", command=select_epsg, bg="#5d872f", fg="white", font=("Arial", 6, "bold"))
    select_epsg_button.grid(padx=(159,0), row=5, column=0)

    # #create map option
    # Map=IntVar()
    # map=Checkbutton(frame1,variable=Map, text='Export sampling stations map', font=bold_font, bg="#92d050")
    # map.grid(row=7, column=0, sticky='w')

    #create pie option
    Pie=IntVar()
    pie=Checkbutton(frame1,variable=Pie, text='Exportar Gráfico Circular FAUNA', font=bold_font, bg="#92d050")
    pie.grid(row=5, column=1, sticky='w')

    G2=IntVar()
    g2=Checkbutton(frame1,variable=G2, text='Exportar Gráfico Circular FLORA', font=bold_font, bg="#92d050")
    g2.grid(row=6, column=0, sticky='w')

    #G1 Número de Espécies por Grupo Taxonómico
    G1=IntVar()
    g1=Checkbutton(frame1,variable=G1, text='Exportar Gráfico de Barras Número de Indíviduos por Grupo Taxonómico', font=bold_font, bg="#92d050")
    g1.grid(row=6, column=1, sticky='w')

    select_tax_button = Button(frame1, text="Nível Taxonómico", command=select_tax_level, bg="#5d872f", fg="white", font=("Arial", 6, "bold"))
    select_tax_button.grid(padx=(470,0), row=6, column=1)

    G7=IntVar()
    g7=Checkbutton(frame1,variable=G7, text='Exportar Gráfico de Barras Abundância por Estação de Amostragem', font=bold_font, bg="#92d050")
    g7.grid(row=7, column=0, sticky='w')

    G8=IntVar()
    g8=Checkbutton(frame1,variable=G8, text='Exportar Gráfico de Barras Nº espécies por categoria FLORA', font=bold_font, bg="#92d050")
    g8.grid(row=7, column=1, sticky='w')

    T9=IntVar()
    t9=Checkbutton(frame1,variable=T9, text='Exportar Abundância por Espécie', font=bold_font, bg="#92d050")
    t9.grid(row=8, column=0, sticky='w')

    T10=IntVar()
    t10=Checkbutton(frame1,variable=T10, text='Exportar Distribuição de Espécies', font=bold_font, bg="#92d050")
    t10.grid(row=8, column=1, sticky='w')

    T14=IntVar()
    t14=Checkbutton(frame1,variable=T14, text='Exportar Distribuição das espécies por Classe por Convenção/Estatuto', font=bold_font, bg="#92d050")
    t14.grid(row=9, column=0, sticky='w')

    T15=IntVar()
    t15=Checkbutton(frame1,variable=T15, text='Exportar Datas de realização das Amostragens/Esforço de Amostragem', font=bold_font, bg="#92d050")
    t15.grid(row=9, column=1, sticky='w')

    select_graph_button = Button(frame1, text="Tipo de Abundância", command=select_graph_type, bg="#5d872f", fg="white", font=("Arial", 6, "bold"))
    select_graph_button.grid(padx=(65,0), row=8, column=0)

    select_color_button = Button(frame1, text="Escolher Cor", command=select_color, bg="#5d872f", fg="white", font=("Arial", 10, "bold"))
    select_color_button.grid(padx=(25), pady=(10,0) ,row=11, column=0)


    # Output directory button
    output_dir_button = Button(
        frame1, text="Escolher onde guardar os Outputs", command=select_output_path, bg="#5d872f", fg="white", font=bold_font)
    output_dir_button.grid(padx=25, pady=(50,0), row=10, column=0)

    # Generate outputs button
    generate_results_button = Button(
        frame1, text="Gerar Outputs", command=generate_results, bg="#5d872f", fg="white", font=bold_font)
    generate_results_button.grid(padx=(0,500), pady=(50,0), row=10, column=1)

    quit_w1=Button(frame1, text='Sair', command=quit, bg="#5d872f", fg="white", font=bold_font)
    quit_w1.grid(padx=(0,500), pady=(10,0), row=11,column=1)
    #quit_w1.place(relx=0.5, rely=0.5, anchor='center')


    # #create exit button
    # frame2=Frame(window, pady=25, width=500, height=50)#, highlightbackground="blue", highlightthickness=2
    # frame2.grid(sticky=('W', 'E', 'S'))  
    # frame2.configure(bg="#92d050")

    # quit_w1=Button(frame2, text='Sair', command=quit, bg="#5d872f", fg="white", font=bold_font)
    # quit_w1.grid(row=5,column=3,sticky=('N','W', 'E', 'S'))
    # quit_w1.place(relx=0.5, rely=0.5, anchor='center')


def button2():
    photo = ImageTk.PhotoImage(Image.open(r'C:\Users\migue\Desktop\BIOTA\DATA-BIOTA\miguel_app\biota.png'))
    window = Toplevel(root)
    window.wm_iconphoto(False, photo)
        #Create frame for outputs generation
    frame_outputs = Frame(window, width=400, height=400)
    frame_outputs.grid(sticky=('N', 'S', 'E', 'W')) 
    frame_outputs.configure(bg="#92d050") 
    label1 = Label(frame_outputs, text="Calculate habitats and impacted areas:", font=("Arial", 9, "bold"))
    label1.grid(row=0, column=0)
    label1.configure(bg="#92d050")
    label2 = Label(frame_outputs, text="Calculate distance and overlap with protected areas:", font=("Arial", 9, "bold"))
    label2.grid(row=0, column=1)
    label2.configure(bg="#92d050")
        #select working_directory
    def select_work_dir_habitat():
        file_path = filedialog.askdirectory(title="Select a working directory", mustexist=True)
        global work_dir
        if file_path:
            work_dir = file_path
            print("Selected working directory:", work_dir)
    
    def select_output_path_habitat():
        file_path = filedialog.askdirectory(title="Select an output directory", mustexist=True)
        global selected_output_path
        if file_path:
            selected_output_path = file_path
            print("Selected output path:", selected_output_path)

    def select_work_dir_protected():
        file_path = filedialog.askdirectory(title="Select a working directory", mustexist=True)
        global work_dir_p
        if file_path:
            work_dir_p = file_path
            print("Selected working directory:", work_dir_p)
    
    def select_output_path_protected():
        file_path = filedialog.askdirectory(title="Select an output directory", mustexist=True)
        global selected_output_path_p
        if file_path:
            selected_output_path_p = file_path
            print("Selected output path:", selected_output_path_p)
    
   
    def calculate_habitat_area():#T2
        print('projects s:', Project.get())
        if Project.get()==1:
        # Read the habitat shapefile
            habitat_path = filedialog.askopenfilename(title="Select an habitats, biotopes or land use shapefile",filetypes=[('Shp','.shp')])
            print("Selected habitats file:", habitat_path )
            global habitats_gdf
            habitats_gdf = gpd.read_file(habitat_path)

            # Read the project shapefile
            project_path = filedialog.askopenfilename(title="Select a project shapefile",filetypes=[('Shp','.shp')])
            global project_gdf
            project_gdf = gpd.read_file(project_path)

             # Check if the 'class' column exists in the habitat shapefile
            if 'class' not in habitats_gdf.columns:
                raise ValueError("The 'class' column does not exist in the provided shapefile.")

            # Create a list to store intermediate results
            int=[]
            # Calculate absolute and proportional area for each category
            for x,category in enumerate(habitats_gdf['class'].unique()):
                #print(category)
            
                category_gdf = habitats_gdf[habitats_gdf['class'] == category]
                # print(category_gdf)
                total_area = category_gdf.geometry.area.sum()
                print(total_area)
                proportional_area = (total_area / habitats_gdf.geometry.area.sum()) * 100
                print(proportional_area)
                int.append([category,total_area,proportional_area])
                print(int)

            area_results = pd.DataFrame(int,columns=['category','total_area','proportional_area'])
            print(area_results)
            # If project shapefile is provided, calculate impacted area
            if not project_gdf.empty:

            # Spatial join to find overlapping areas
                overlap_gdf = gpd.overlay(habitats_gdf, project_gdf, how='intersection')

                # Calculate impacted area for each category
                for category in area_results['category']:
                    category_overlap_gdf = overlap_gdf[overlap_gdf['class'] == category]
                    impacted_area = category_overlap_gdf.geometry.area.sum()
                    impacted_proportional_area = (impacted_area / habitats_gdf.geometry.area.sum()) * 100

                    # Add results to the DataFrame
                    area_results.loc[area_results['category'] == category, 'impacted_area'] = impacted_area
                    area_results.loc[area_results['category'] == category, 'impacted_area_%'] = impacted_proportional_area

        else:
                    # Read the habitat shapefile
            habitat_path = filedialog.askopenfilename(title="Select an habitats, biotopes or land use shapefile",filetypes=[('Shp','.shp')])
            print("Selected habitats file:", habitat_path )
            global habitats_gdf2
            habitats_gdf2 = gpd.read_file(habitat_path)

            # Check if the 'class' column exists in the habitat shapefile
            if 'class' not in habitats_gdf.columns:
                raise ValueError("The 'class' column does not exist in the provided shapefile.")

            # Create a list to store intermediate results
            int=[]
            # Calculate absolute and proportional area for each category
            for x,category in enumerate(habitats_gdf['class'].unique()):
                #print(category)
            
                category_gdf = habitats_gdf[habitats_gdf['class'] == category]
                # print(category_gdf)
                total_area = category_gdf.geometry.area.sum()
                print(total_area)
                proportional_area = (total_area / habitats_gdf.geometry.area.sum()) * 100
                print(proportional_area)
                int.append([category,total_area,proportional_area])
                print(int)

            area_results = pd.DataFrame(int,columns=['category','total_area','proportional_area'])
            print(area_results)


        # Save results to CSV
        area_results.to_csv( f"{selected_output_path}/habitat_area_results.csv", index=False, sep=';')
        print("Area calculation completed. Results saved to habitat_area_results.csv")

    def calculate_protected_area():#T4
            
        protected_areas = ProtectedAreas(work_dir_p)
        areas_prot=['iba','ramsar_continente', 'zpe', 'biosfera', 'sic', 'rnap','abrigo'] #TODO eventually create an alternative to select the protected shapefiles 
        all_outputs=pd.DataFrame(columns=['tipo','nome','distancia','sobreposicao'])
        for area in areas_prot:
            output=pd.DataFrame(protected_areas.process_protected_area(area, 'project_over.shp'))
            all_outputs=pd.concat([all_outputs,output])
        # Concatenate all the output tables into a single dataframe
        # print(all_outputs)
        # Export the concatenated dataframe to a CSV file
        protected_areas.export_to_csv(all_outputs, f"{selected_output_path_p}/protected_areas_output.csv")
 
    # Working directory button
    output_dir_button = Button(
        frame_outputs, text="Select working directory", command=select_work_dir_habitat, bg="#5d872f", fg="white", font=("Arial", 13, "bold")
    )
    output_dir_button.grid(padx=25, row=2, column=0)

    # Output directory button
    output_dir_button = Button(
        frame_outputs, text="Select output directory", command=select_output_path_habitat, bg="#5d872f", fg="white", font=("Arial", 13, "bold")
    )
    output_dir_button.grid(padx=25, row=3, column=0)
    
    label5 = Label(frame_outputs, text="Calculate also impacted areas by habitat?", font=("Arial", 9, "bold"))
    label5.grid(row=4, column=0)
    label5.configure(bg="#92d050")
      
    #create habitat and project shapefiles option
    Project=IntVar()
    Projects=Checkbutton(frame_outputs,variable=Project, text='Yes', bg="#92d050", font=("Arial", 13, "bold"))
    Projects.grid(row=4, column=0, sticky='w')

      # Generate outputs button
    generate_results_button = Button(
        frame_outputs, text="Generate outputs", command=calculate_habitat_area, bg="#5d872f", fg="white", font=("Arial", 13, "bold")
    )
    generate_results_button.grid(padx=25, row=6, column=0)

    # Working directory button
    output_dir_button = Button(
        frame_outputs, text="Select working directory", command=select_work_dir_protected, bg="#5d872f", fg="white", font=("Arial", 13, "bold")
    )
    output_dir_button.grid(padx=25, row=2, column=1)

    # Output directory button
    output_dir_button = Button(
        frame_outputs, text="Select output directory", command=select_output_path_protected, bg="#5d872f", fg="white", font=("Arial", 13, "bold")
    )
    output_dir_button.grid(padx=25, row=3, column=1)
    
    # Generate outputs button
    generate_results_button = Button(
        frame_outputs, text="Generate outputs", command=calculate_protected_area, bg="#5d872f", fg="white", font=("Arial", 13, "bold")
    )
    generate_results_button.grid(padx=25, row=4, column=1)

    
    #create exit button
    frame2=Frame(frame_outputs, pady=25, width=500, height=50)#, highlightbackground="blue", highlightthickness=2
    frame2.grid(sticky=('W', 'E', 'S'))  
    frame2.configure(bg="#92d050")
    quit_w1=Button(frame_outputs, text='Back to app', command=quit, bg="#5d872f", fg="white", font=("Arial", 13, "bold"))
    # quit_w1.grid(column=2,sticky=('N','W', 'E', 'S'))
    quit_w1.grid(padx=25, row=6, column=1)
    # quit_w1.place(relx=0.5, rely=0.5, anchor='center')

 
# Buttons to open new windows
# Define a bold font
bold_font = ("Arial", 20, "bold")

# Create the button with the bold font
hello_world_button = tk.Button(root, text="Dados, Tabelas e Gráficos", command=button1, bg="#5d872f", fg="white", font=bold_font)
hello_world_button.grid(column=2, sticky=('N', 'W', 'E', 'S'))
hello_world_button.place(relx=0.5, rely=0.3, anchor='center')

hello_moon_button = Button(root, text="Operações SIG (Em progresso)", command=button2, bg="#5d872f", fg="white", font=bold_font)
hello_moon_button.grid(column=2, sticky=('N', 'W', 'E', 'S'))
hello_moon_button.place(relx=0.5, rely=0.45, anchor='center')

quit_app=Button(root, text='Sair', command=root.quit, bg="#5d872f", fg="white", font=bold_font)
quit_app.grid(column=2,sticky=('N','W', 'E', 'S'))
quit_app.place(relx=0.5, rely=0.8, anchor='center')



root.mainloop()
