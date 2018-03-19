#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
conda install -c anaconda tk=8.5.18
"""
from tkinter import *

fenetre = Tk()

label = Label(fenetre,text = "Hello World")
label.pack()

# bouton de sortie
bouton = Button(fenetre,text="fermer",command = fenetre.quit)

bouton.pack()

# label
label = Label(fenetre, text="Texte par défaut", bg="yellow")

label.pack()

# entrée
string =""
value = StringVar()
value.set("texte par défaut")
entree = Entry(fenetre, textvariable=string, width=30)

entree.pack()

# checkbutton
bouton = Checkbutton(fenetre, text="Nouveau?")

bouton.pack()

# radiobutton
value = StringVar()
bouton1 = Radiobutton(fenetre, text="Oui", variable=value, value=1)
bouton2 = Radiobutton(fenetre, text="Non", variable=value, value=2)
bouton3 = Radiobutton(fenetre, text="Peu être", variable=value, value=3)

bouton1.pack()
bouton2.pack()
bouton3.pack()

# liste
liste = Listbox(fenetre)
liste.insert(1, "Python")
liste.insert(2, "PHP")
liste.insert(3, "jQuery")
liste.insert(4, "CSS")
liste.insert(5, "Javascript")

liste.pack()

# canvas
"""
Un canvas (toile, tableau en français) est un espace dans lequel vous pouvez dessiner ou écrire ce que vous voulez
"""
"""
create_arc()        :  arc de cercle
create_bitmap()     :  bitmap
create_image()      :  image
create_line()       :  ligne
create_oval()       :  ovale
create_polygon()    :  polygone
create_rectangle()  :  rectangle
create_text()       :  texte
create_window()     :  fenetre
"""
canvas = Canvas(fenetre, width=150, height=120, background='green')
ligne1 = canvas.create_line(75, 0, 75, 120)
ligne2 = canvas.create_line(0, 60, 150, 60)
txt = canvas.create_text(75, 60, text="Cible", font="Arial 16 italic", fill="blue")

canvas.pack()

# Le widget scale permet de récupérer une valeur numérique via un scroll
value = DoubleVar()
scale = Scale(fenetre, variable=value)
scale.pack()

#Les frames (cadres) sont des conteneurs qui permettent de séparer des éléments.
fenetre['bg']='white'

# frame 1
Frame1 = Frame(fenetre, borderwidth=2, relief=GROOVE)
Frame1.pack(side=LEFT, padx=30, pady=30)

# frame 2
Frame2 = Frame(fenetre, borderwidth=2, relief=GROOVE)
Frame2.pack(side=LEFT, padx=10, pady=10)

# frame 3 dans frame 2
Frame3 = Frame(Frame2, bg="white", borderwidth=2, relief=GROOVE)
Frame3.pack(side=RIGHT, padx=5, pady=5)

# Ajout de labels
Label(Frame1, text="Frame 1").pack(padx=10, pady=10)
Label(Frame2, text="Frame 2").pack(padx=10, pady=10)
Label(Frame3, text="Frame 3",bg="white").pack(padx=10, pady=10)

# Le panedwindow est un conteneur qui peut contenir autant de panneaux que nécessaire disposé horizontalement ou verticalement.
p = PanedWindow(fenetre, orient=HORIZONTAL)
p.pack(side=TOP, expand=Y, fill=BOTH, pady=2, padx=2)
p.add(Label(p, text='Volet 1', background='blue', anchor=CENTER))
p.add(Label(p, text='Volet 2', background='white', anchor=CENTER) )
p.add(Label(p, text='Volet 3', background='red', anchor=CENTER) )
p.pack()

# Spinbox

s = Spinbox(fenetre, from_=0, to=10)
s.pack()

# Le labelframe est un cadre avec un label.
l = LabelFrame(fenetre, text="Titre de la frame", padx=20, pady=20)
l.pack(fill="both", expand="yes")

Label(l, text="A l'intérieure de la frame").pack()

fenetre.mainloop()