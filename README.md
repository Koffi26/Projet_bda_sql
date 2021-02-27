<h1><center>Jointure Spark </center> </h1> 
------------------
## Exécuter le code + les dossiers

Pour exécuter et voir les réponses il est conseillé dans un premier temps de télécharger le dossier (en utilisant le fichier git clones )

Par la suite il est conseillé à l'utilisateur d'exécuter la commande suivante 

``submit-spark run.py``

Le projet script est composé de plusieurs fichiers :

- fichier contention contenant la durée totale d'emprunt en termes de mois
- script dans le quel se trouve :
  
    - la création de nos bases de données
    - les codes permettant d'extraire des informations depuis les différentes base de données
    
- un fichier output avec toutes les bases de données en csv.
    
------------------
## Les bases de données :
L'objectif du projet est de faire des jointures sur spark. Pour ce faire nous disposons de plusieurs bases de données. 
les bases sont les suivantes :

- Author :

| aid               | name |
| :--------------- |:---------------:| 
|07890             | Jean Paul Sartre|
| 05678            | Pierre de Ronsard |

- Book :

| bid | title |category|
|----:|:------|-------:|
| 0001  |L'existentialisme est un humanisme |Philosophie|
| 0002  |Huis clos. Suivi de Les Mouches    |Philosophie|
| 0003  |Mignonne allons voir si la rose    |Poeme      |
| 0004  |Les Amours                         |Poème      |

- Student

| sid | sname | dept|
| :---- |:---:|----:|
| S15 | toto| Math|
| S16 | popo | Eco|
| S17 | fofo |Mécanique|

- Write :

| aid | bid |
|----:|-----:|
| 07890 |0001|
| 07890| 0002|
|05678 | 0003|
| 05678|0003 |

- borrow :

|sid |bid| checkout-time | return-time |
|----|----|--------------|-------------|
|S15 |0003| 02-01-2020   |01-02-2020   |
|S15 |0002|13-06-2020    |null         |
|S15 |0001|13-06-2020    |13-10-2020   |
|S16 |0002|24-01-2020    |24-01-2020   |
|S17 |0001|12-04-2020    |01-07-2020   |

------------------------------------------

## Information concernant les bouquins empruntés.

## Résultats issus des questionnaires

## Question 1

L'étudiant S15 a emprunté 3 livres qui sont :
   
+---+----------------------------------+
|sid|               title              |
+---+----------------------------------+
|S15|Huis clos. Suivi de Les Mouches   |
|S15|Mignonne allons voir si la rose   |
|S15|L'existentialisme est un humanisme|
+---+----------------------------------+


## Question 2

Les titres de tous les livres qui n'ont jamais été empruntés par un étudiant sont les Amours.

## Question 3

Tous les étudiants qui ont emprunté le livre bid='002' sont 

+-----+---+----+
|sname|sid| bid|
+-----+---+----+
| popo|S16|0002|
| toto|S15|0002|
+-----+---+----+

Il s'agit des étudiants toto et popo

## Question 4

Les étudiants du Département Mécanique ont emprunté les livres titrés : "L'existentialisme est un humanisme"

## Question 5

Quels sont les étudiants qui n’ont jamais emprunté de livre :
Il n'existe pas d'étudiant qui n'a pas emprunté de livre. Tous ont emprunté au moins un livre.


## Question 6

Pour la question concernant l'auteur qui a plus écrit de livres, on se retrouve avec deux auteurs qui ont écrit chacun 2 livres. 

+-----------------+---+
|             name|cnt|
+-----------------+---+
| Jean Paul Sartre|  2|
|Pierre de Ronsard|  2|
+-----------------+---+

Il s'agit de Jean Paul Sartre et Pierre de Ronsard


## Question 7

Seule une personne n'a pas rendu les livres. Il s'agit de l'étudiant toto.

## Question 8

Création d'une nouvelle colonne "Duree) dans la base borrow : elle prend la valeur 1  si la durée d'emprunt est supérieure à 3 mois, sinon 0

## Question 10 

Commit et et Push sur le repository Git (Projet_bda_sql)



