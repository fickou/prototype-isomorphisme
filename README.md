# Prototype Isomorphisme de Graphes (Apache Giraph)

Ce projet implémente un algorithme distribué de recherche d'isomorphismes de sous-graphes exacts, en s'appuyant sur l'algorithme d'Ullmann et le modèle **BSP (Bulk Synchronous Parallel)** d'Apache Giraph (écosystème Hadoop).

Ce logiciel est spécialement paramétré pour résoudre l'explosion combinatoire des calculs de chemins complexes dans des grands graphes en divisant la force de calcul entre plusieurs processus (_Mappers_ Hadoop).

## 🚀 Fonctionnalités

- **Filtrage Distribué** : Élimine les sommets sans espoir (degrés entrants/sortants insuffisants).
- **Exploration Répartie** : Étend les mappings de manière asynchrone (Superstep par Superstep).
- **Validation Structurelle Stricte** : Garantit le respect méticuleux des relations d'arc (Ullmann).
- **Orchestration Centralisée** : Le `MaitreUnique.java` pilote l'aiguillage, détecte le silence d'activité du cluster et rassemble les résultats.

## 📋 Prérequis

- **Cluster Hadoop** fonctionnel (testé sur Ubuntu / Hadoop v1 ou v2)
- **Apache Giraph** installé sur le cluster
- **Java 8+**
- Un environnement HDFS initialisé avec votre grand graphe (données) et votre petit graphe (motif).

## 🛠️ Compilation

Le projet est packagé classiquement avec Maven. À la racine du projet (là où se trouve le `pom.xml`) :

```bash
mvn clean package
```

> Cela va générer un fichier `.jar` (par exemple `prototype-isomorphisme-1.0-SNAPSHOT.jar` ou un "_jar-with-dependencies_") dans le dossier `/target/`.

## 💻 Exécution

### 1. Préparer les données sur HDFS

Assurez-vous d'avoir transféré vos fichiers texte (liste d'adjacence) vers le système de fichiers distribué.

```bash
# Exemple de création du répertoire HDFS et d'envoi des fichiers
hadoop dfs -mkdir -p /user/hduser/input/
hadoop dfs -copyFromLocal donnees/donnees_exemple.txt /user/hduser/input/donnees_exemple.txt
hadoop dfs -copyFromLocal donnees/motif_exemple.txt /user/hduser/input/motif_exemple.txt
```

### 2. Lancer le Job Giraph

Exécutez la classe principale `LanceurJob` via la commande Hadoop. (Remplacez le nom du `jar` par le vôtre).

```bash
hadoop jar target/prototype-isomorphisme-1.0-SNAPSHOT-job.jar \
  com.memoire.giraph.LanceurJob \
  /user/hduser/input/donnees_exemple.txt \
  /user/hduser/input/motif_exemple.txt \
  /user/hduser/output/isomorphisme_resultats \
  1
```

_(Vous pouvez également passer par un script shell alias `LanceurJob` si vous en avez configuré un)._

### 3. Lire les résultats

À la fin de l'algorithme (détection d'activité à 0 par le Master), les différentes solutions possibles d'isomorphisme sont enregistrées.

```bash
hadoop fs -cat /user/hduser/output/isomorphisme_resultats
```

## 🧠 Architecture du calcul (BSP)

L'algorithme tourne sur les **Supersteps** suivantes :

1. **S0 & S1 - L'écrémage** : `CalculFiltre.java` gère le signalement des sommets (`PING_DEGRE`), les rassemble, et transmet les degrés (entrant/sortant) globaux au Master (`AggregateurDegresSommets`).
2. **S2 - Le Jugement Master** : `MaitreUnique.java` examine les requêtes, exclut les sommets impertinents sur la base des degrés du motif, élit une racine par ordre DFS (la plus optimisée possible), et distribue les "candidats" élus.
3. **S3 - Le Top Départ** : `CalculCorrespondance.java` lance la première ramification d'exploration depuis la racine vers ses voisins autorisés.
4. **S≥4 - La Recherche Distribuée** : Les sous-mappings transitent localement en validant rigoureusement la topologie (injectivité, sens des arcs).
5. **Fin & Convergence** : Dès l'instant où un Superstep s'achève sur une absence d'extension (`activité = 0`), le Master prend le relais, compile l'`AggregateurResultats` persistant sur HDFS et désactive gentiment le circuit (`haltComputation()`).

---

**Licence** : Projet Académique / Prototype.
