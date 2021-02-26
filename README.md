<h1><center>Jointure Spark </center> </h1> 
------------------
## Executer le code + les dossiers

Pour exéxuter et voir les réponses il est conseillé dans un premier temps de télécharger le dossier (en utilisant le fichier git clones )

Par la suite il est conseiller o l'utilisateur d'executer la commande suivante 

``submit-spark run.py``

Le projet script est composé de plusieurs fichier :

- fichier contention contenant la durée de total des empreint en termes de mois
- scriot dans le quel se trouve :
  
    - la création de nos bases de données
    - les codes permettant d'extraire des informations depuis les différentes base de données
    
- un fichier output avec toutes les bases de donnée en csv.
    
------------------
## Les bases de données :
L'objectif du projet est de faire des jointures sur spark. Pour ce faire nous disposons de plusieurs base de données. 
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
|----|----|---------------|-------------|
|S15 |0003| 02-01-2020 |01-02-2020|
|S15 |0002|13-06-2020|null|
|S15 |0001|13-06-2020|13-10-2020|
|S16 |0002|24-01-2020|24-01-2020|
|S17 |0001|12-04-2020|01-07-2020

------------------------------------------

## Information concernant les bouquins emprunter.