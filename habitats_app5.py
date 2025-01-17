import tkinter as tk
from tkinter import filedialog, messagebox, Scrollbar, Listbox, Button, Label, Frame, Toplevel, Text
import pandas as pd
import numpy as np

import ttkbootstrap as ttk
from ttkbootstrap.constants import *

# Função para carregar dados do CSV com tratamento de erro
def carregar_csv(csv_path):
    try:
        dados = pd.read_csv(csv_path, encoding='utf-8')
        return dados
    except FileNotFoundError:
        messagebox.showerror("Erro", f"O arquivo {csv_path} não foi encontrado.")
        return None

# Função para limpar texto removendo caracteres indesejados
def limpar_texto(texto):
    texto = str(texto)
    # Remover espaços não quebráveis e outros caracteres estranhos
    return texto.replace("\xa0", " ")  # Substituir por espaços normais

# Carregar dados do CSV
csv_path = 'habitats_final.csv'  # Substitua pelo caminho do seu arquivo CSV
dados = carregar_csv(csv_path)

# Certificar-se de que os valores das colunas são strings
dados = dados.astype(str)

# Limpar os dados para evitar caracteres estranhos
dados = dados.applymap(limpar_texto)  # Aplicar a função para toda a DataFrame

if dados is None:
    exit(1)  # Sair se o arquivo não foi encontrado

# Extrair a coluna das espécies e remover NaN
coluna_especies = 'Espécies'  # Substitua pelo nome da coluna contendo as espécies
especies = dados[coluna_especies].dropna().unique()  # Remover valores NaN e obter únicos
especies = sorted(especies)  # Ordenar alfabeticamente

# Criar a janela Tkinter
janela = tk.Tk()
janela.title("Lista de Espécies")
janela.configure(bg="#92d050")
janela.iconbitmap('biota_arvore.ico')

# Criar os frames para organizar as listboxes
frame1 = Frame(janela)
frame2 = Frame(janela)
frame1.configure(bg="#92d050")
frame2.configure(bg="#92d050")

frame1.grid(row=0, column=0, padx=10, pady=10)
frame2.grid(row=0, column=1, padx=10, pady=10)

bold_font_15 = ("Arial", 15, "bold")
bold_font_12 = ("Arial", 15, "bold")

# Adicionar labels em cima das listboxes
label1 = Label(frame1, text="Espécie(s) Indicadora(s)", font=bold_font_15, fg="white")
label1.configure(bg="#5d872f")
label2 = Label(frame2, text="Outra(s) Espécie", font=bold_font_15, fg="white")
label2.configure(bg="#5d872f")

label1.grid(row=0, column=0, pady=(0, 5))
label2.grid(row=0, column=0, pady=(0, 5))

# Criar scrollbars para as listboxes
scrollbar1 = Scrollbar(frame1, orient=tk.VERTICAL)
scrollbar2 = Scrollbar(frame2, orient=tk.VERTICAL)

# Criar listboxes e conectar com as scrollbars
listbox1 = Listbox(frame1, height=20, width=45, yscrollcommand=scrollbar1.set, selectmode='multiple', exportselection=False)
listbox1.grid(row=2, column=0, padx=5, pady=5)
scrollbar1.config(command=listbox1.yview)
scrollbar1.grid(row=2, column=1, sticky='ns')  # scrollbar vertical para listbox1

listbox2 = Listbox(frame2, height=20, width=45, yscrollcommand=scrollbar2.set, selectmode='multiple', exportselection=False)
listbox2.grid(row=2, column=0, padx=5, pady=5)
scrollbar2.config(command=listbox2.yview)
scrollbar2.grid(row=2, column=1, sticky='ns')  # scrollbar vertical para listbox2

# Adicionar as espécies às listboxes
for especie in especies:
    listbox1.insert(tk.END, especie)
    listbox2.insert(tk.END, especie)



# Adicionar campo de texto para pesquisar na lista "Indicadora"
campo_pesquisa1 = tk.Entry(frame1)
campo_pesquisa1.grid(row=1, column=0, pady=(5, 5))

# Adicionar campo de texto para pesquisar na lista "Outra"
campo_pesquisa2 = tk.Entry(frame2)
campo_pesquisa2.grid(row=1, column=0, pady=(5, 5))

# Função para atualizar a listbox com base no texto de pesquisa
def atualizar_listbox(listbox, campo_pesquisa, especies):
    texto_pesquisa = campo_pesquisa.get().lower()
    listbox.delete(0, tk.END)
    for especie in especies:
        if especie.lower().startswith(texto_pesquisa):  # Verificar se começa com o texto de pesquisa
            listbox.insert(tk.END, especie)

# Funções que são chamadas ao digitar nos campos de pesquisa
def atualizar_listbox1(event):
    atualizar_listbox(listbox1, campo_pesquisa1, especies)

def atualizar_listbox2(event):
    atualizar_listbox(listbox2, campo_pesquisa2, especies)

# Associar a atualização à entrada de texto nos campos de pesquisa
campo_pesquisa1.bind("<KeyRelease>", atualizar_listbox1)
campo_pesquisa2.bind("<KeyRelease>", atualizar_listbox2)

# Atualizar listboxes inicialmente
atualizar_listbox(listbox1, campo_pesquisa1, especies)
atualizar_listbox(listbox2, campo_pesquisa2, especies)


# Função para procurar habitats correspondentes
def procurar_habitat():
    # Coletar as espécies selecionadas nas listboxes
    especies_indicadoras = [listbox1.get(i) for i in listbox1.curselection()]
    especies_outras = [listbox2.get(i) for i in listbox2.curselection()]

    # Se não houver espécies selecionadas, mostrar uma mensagem informativa
    if not especies_indicadoras and not especies_outras:
        messagebox.showinfo("Info", "Nenhuma espécie selecionada.")
        return

    matching_habitats = set()

    # Agrupar por subtipos e código do habitat
    grouped = dados.groupby(["Subtipos", "Código do habitat"])

    for (subtipo, codigo), grupo in grouped:
        especies_no_habitat = set(grupo[coluna_especies])

        # Verificar se as espécies indicadoras estão presentes no habitat
        if especies_indicadoras:
            especies_indicadoras_no_habitat = grupo[
                (grupo[coluna_especies].isin(especies_indicadoras)) &
                (grupo["tipo_sp"] == "Indicadora")
            ]
            if not set(especies_indicadoras).issubset(set(especies_indicadoras_no_habitat[coluna_especies])):
                continue  # Se as indicadoras não estiverem todas presentes, pular este grupo

        # Verificar se as outras espécies estão presentes no habitat
        if especies_outras:
            especies_outras_no_habitat = grupo[
                (grupo[coluna_especies].isin(especies_outras)) &
                (grupo["tipo_sp"] == "Outra")
            ]
            if not set(especies_outras).issubset(set(especies_outras_no_habitat[coluna_especies])):
                continue  # Se as outras espécies não estiverem todas presentes, pular este grupo

        # Se passou em ambas as condições, adicionar ao conjunto de habitats correspondentes
        matching_habitats.add((subtipo, codigo))  # Adicionar como tupla de subtipo e código do habitat

    # Criar uma janela pop-up para exibir a informação
    popup = Toplevel(janela)
    popup.title("Habitat")
    popup.configure(bg="#92d050")
    popup.iconbitmap('biota_arvore.ico')

    # Criar um widget Text para exibir o conteúdo
    text_widget = Text(popup, height=10, wrap='word')
    text_widget.configure(bg="#5d872f")
    text_widget.pack(pady=10, padx=10, fill='both', expand=True)

    # Definir a tag para texto itálico
    text_widget.tag_configure("italic", font=bold_font_12, foreground="white")

    # Se houver habitats correspondentes, exibir a informação
    if matching_habitats:
        count = 0  # Para espaçar entre habitats, se houver mais de um

        for subtipo, codigo in matching_habitats:
            habitat_info = dados[(dados['Subtipos'] == subtipo) & (dados['Código do habitat'] == codigo)].iloc[0]
            designacao = habitat_info['Designação']
            nome = habitat_info['Nome']

            habitat_text = f"Código: {codigo}; Designação: {designacao}; Subtipo: {subtipo}; Nome: {nome}"

            if count > 0:
                text_widget.insert(tk.END, "\n")  # Adicionar uma linha em branco entre entradas

            text_widget.insert(tk.END, habitat_text + "\n", "italic")

            count += 1

    else:
        text_widget.insert(tk.END, "Nenhum habitat correspondente encontrado para as espécies selecionadas.", "italic")







# Adicionar botão para procurar habitat
botao_procurar_habitat = Button(janela, text="Procurar Habitat", command=procurar_habitat, font=bold_font_12, fg="white")
botao_procurar_habitat.grid(row=3, column=0, padx=(0,0), pady=(10, 10))
botao_procurar_habitat.configure(bg="#5d872f") 

# Função para sair da aplicação
def sair_da_aplicacao():
    janela.quit()  # Encerra a aplicação

# Adicionar botão para sair da aplicação
botao_sair = Button(janela, text="Sair", command=sair_da_aplicacao, font=bold_font_12, fg="white")
botao_sair.grid(row=4, column=0, padx=0, pady=(10, 10))  # Abaixo do botão "Procurar Habitat"  
botao_sair.configure(bg="#5d872f") 

# Iniciar o loop principal da janela
janela.mainloop()



