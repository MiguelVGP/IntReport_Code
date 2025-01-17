import tkinter as tk
from tkinter import messagebox
from PIL import Image, ImageTk
import csv

def exibir_especies(especies):
    # Exibir a janela pop-up com as espécies e imagens correspondentes
    popup = tk.Toplevel()
    popup.title("Espécies Encontradas")
    popup.configure(bg="burlywood")

    # Calcular o número de linhas e colunas necessárias para exibir todas as espécies
    num_colunas = 3
    num_linhas = (len(especies) + num_colunas - 1) // num_colunas
    
    # Criar uma grade para dispor as espécies
    for i, especie in enumerate(especies):
        imagem_path = especies_imagens.get(especie, 'C:/Users/migue/Desktop/BIOTA/DATA-BIOTA/alexandre_morcegos/Imagens/desconhecido.png')
        imagem = Image.open(imagem_path)
        imagem = imagem.resize((200, 200))
        imagem = ImageTk.PhotoImage(imagem)

        label_nome = tk.Label(popup, text="Espécie: " + especie, font=bold_font, bg="burlywood")
        label_nome.grid(row=i % num_linhas, column=i // num_linhas * 2)

        label_imagem = tk.Label(popup, image=imagem)
        label_imagem.image = imagem
        label_imagem.grid(row=i % num_linhas, column=i // num_linhas * 2 + 1)


def comparar_valores():
    # Obter os valores digitados pelo usuário
    valor_a = float(entry_a.get())
    valor_b = float(entry_b.get())
    
    # Lista para armazenar as espécies correspondentes
    especies_encontradas = []

    # Abrir o arquivo CSV e comparar os valores
    with open('C:/Users/migue/Desktop/BIOTA/DATA-BIOTA/alexandre_morcegos/csv-teste.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Ignorar o cabeçalho
        for linha in reader:
            especie = linha[0]
            valor_a_min = float(linha[1])
            valor_a_max = float(linha[2])
            valor_b_min = float(linha[3])
            valor_b_max = float(linha[4])
            
            # Verificar se os valores estão dentro do intervalo (fechado)
            if valor_a_min <= valor_a <= valor_a_max and valor_b_min <= valor_b <= valor_b_max:
                especies_encontradas.append(especie)
    
    # Mostrar as espécies encontradas na janela pop-up
    if especies_encontradas:
        exibir_especies(especies_encontradas)
    else:
        messagebox.showinfo("Nenhuma espécie encontrada", "Não foram encontradas espécies com os valores fornecidos.")

# Criar a janela principal
janela = tk.Tk()
janela.title("Comparador de Espécies")

# Criar os campos de entrada
bold_font = ("Arial", 10, "bold")
label_a = tk.Label(janela, text="Fator A:", font=bold_font)
label_a.grid(row=0, column=0)
label_a.configure(bg="burlywood")
entry_a = tk.Entry(janela)
entry_a.grid(row=0, column=1)

label_b = tk.Label(janela, text="Fator B:", font=bold_font)
label_b.grid(row=1, column=0)
label_b.configure(bg="burlywood")
entry_b = tk.Entry(janela)
entry_b.grid(row=1, column=1)

# Botão para comparar os valores
botao_comparar = tk.Button(janela, text="Comparar", command=comparar_valores, bg="burlywood", fg="black", font=("Arial", 11, "bold"))
botao_comparar.grid(row=2, column=0, columnspan=2, pady=20)

# Dicionário com o nome da espécie e o caminho da imagem correspondente
especies_imagens = {
    "Morcego1": "C:/Users/migue/Desktop/BIOTA/DATA-BIOTA/alexandre_morcegos/Imagens/morcego1.jpeg",
    "Morcego2": "C:/Users/migue/Desktop/BIOTA/DATA-BIOTA/alexandre_morcegos/Imagens/morcego2.jpeg",
    "Morcego3": "C:/Users/migue/Desktop/BIOTA/DATA-BIOTA/alexandre_morcegos/Imagens/morcego3.jpg",
    "Morcego4": "C:/Users/migue/Desktop/BIOTA/DATA-BIOTA/alexandre_morcegos/Imagens/morcego4.jpeg",
    "Morcego5": "C:/Users/migue/Desktop/BIOTA/DATA-BIOTA/alexandre_morcegos/Imagens/morcego5.png"
}

janela.configure(bg="burlywood")
# Iniciar o loop da aplicação
janela.mainloop()
