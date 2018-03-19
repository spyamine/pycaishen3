#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt

plt.plot([1,2,3,4])
plt.ylabel('label 1')
plt.show()

plt.title("Danger de la vitesse")
plt.plot([50,100,150,200], [1,2,3,4])
plt.xlabel('Vitesse')
plt.ylabel('Temps')
plt.show()



"""
axis(xmin, xmax, ymin, ymax)
La courbe est juste mais je ne souhaite délimiter moi même les limites du graphique. Seul les vitesses entre 80 et 180
m'intéressent et le temps de 1 secondes à 10 secondes. Il est possible d'encadrer son graphique à l'aide de la méthode
axis(xmin, xmax, ymin, ymax)
"""
plt.plot([50,100,150,200], [1,2,3,4])
plt.xlabel('Vitesse')
plt.ylabel('Temps')
plt.axis([80, 180, 1, 10])
plt.show()


"""
plusieurs courbes
"""
plt.plot([50,100,150,200], [1,2,3,4])
plt.plot([50,100,150,200], [2,3,7,10])
plt.plot([50,100,150,200], [2,7,9,10])
plt.show()

"""
possible d'utiliser la syntaxe suivante
"""

plt.plot([50,100,150,200], [1,2,3,4], [50,100,150,200], [2,3,7,10], [50,100,150,200], [2,7,10,20])
plt.show()

"""
Courbes possibles: - -- -. :
Couleurs possibles: b g r c m y k w
"""

plt.plot([50,100,150,200], [1,2,3,4], "r--")
plt.plot([50,100,150,200], [2,3,7,10], "bs")
plt.plot([50,100,150,200], [2,7,9,10], "g^")
plt.show()

"""
Vous pouvez changer la largeur des courbes comme ceci:
"""
plt.plot([50,100,150,200], [1,2,3,4], "r--", linewidth=5)
plt.plot([50,100,150,200], [2,3,7,10], "b", linewidth=3)
plt.plot([50,100,150,200], [2,7,9,10], "g", linewidth=10)
plt.show()


"""
Plusieurs graphiques
Il est possible de mettre plusieurs graphiques sur une même image:
"""



plt.subplot(211)
plt.plot([50,100,150,200], [1,2,3,4], "r--", linewidth=5)
plt.plot([50,100,150,200], [2,3,7,10], "b", linewidth=3)
plt.plot([50,100,150,200], [2,7,9,10], "g", linewidth=10)
plt.xlabel('Vitesse')
plt.ylabel('Temps')
plt.axis([80, 180, 1, 10])

plt.subplot(212)
plt.plot([50,100,150,200], [1,2,3,15], "r--", linewidth=5)
plt.plot([50,100,150,200], [2,3,7,10], "b", linewidth=3)
plt.plot([50,100,150,200], [2,7,9,10], "g", linewidth=10)
plt.xlabel('Vitesse')
plt.ylabel('Temps')
plt.axis([80, 180, 1, 10])

plt.show()

"""
Grille
Vous pouvez également ajouter une grille (et des marqueurs):

marqueurs possibles : o + . x * ^
"""
plt.grid(True)
plt.plot([50,100,150,200], [2,3,7,10], "b", linewidth=0.8, marker="*")
plt.plot([50,100,150,200], [2,7,9,10], "g", linewidth=0.8, marker="+")


"""
Ecrire du texte dans votre graphique
Il est possible d'ajouter du texte en indiquant les coordonnées:
"""

plt.text(150, 6.5, r'Danger')

plt.show()

"""
Ajouter une légende pour les courbes
"""
plt.grid(True)
plt.plot([50,100,150,200], [2,3,7,10], "b", linewidth=0.8, marker="*", label="Trajet 1")
plt.plot([50,100,150,200], [2,7,9,10], "g", linewidth=0.8, marker="+", label="Trajet 2")
plt.axis([80, 180, 1, 10])
plt.xlabel('Vitesse')
plt.ylabel('Temps')
plt.legend()
plt.show()


"""
Ajouter une flèche descriptive
"""
plt.grid(True)
plt.plot([50,100,150,200], [2,3,7,10], "b", linewidth=0.8, marker="*")
plt.plot([50,100,150,200], [2,7,9,10], "g", linewidth=0.8, marker="+")
plt.axis([80, 180, 1, 10])
plt.annotate('Limite', xy=(150, 7), xytext=(165, 5.5),
arrowprops={'facecolor':'black', 'shrink':0.05} )
plt.xlabel('Vitesse')
plt.ylabel('Temps')
plt.show()



"""
Les histogrammes
Pour créer un histogramme on utilise la méthode hist. On peut lui donner des données brutes et il s'occupera de faire
les calcules nécessaires à la présentation du graphique.

On a prit ici l'exemple de 1000 tirage au sort (random) de valeur entre 0 et 150 et voici le résultat:
"""
import random

# 1000 tirages entre 0 et 150
x = [random.randint(0,150) for i in range(1000)]
n, bins, patches = plt.hist(x, 50, normed=1, facecolor='b', alpha=0.5)

plt.xlabel('Mise')
plt.ylabel('Probabilité')
plt.axis([0, 150, 0, 0.02])
plt.grid(True)
plt.show()

"""
pie / diagramme circulaire
Un des graphiques les plus utilisés doit être le diagramme circulaire (ou camembert) :
"""

name = ['-18', '18-25', '25-50', '50+']
data = [5000, 26000, 21400, 12000]

explode=(0, 0.15, 0, 0)
plt.pie(data, explode=explode, labels=name, autopct='%1.1f%%', startangle=90, shadow=True)
plt.axis('equal')
plt.show()