# 🧪 Lab4 – Apache Kafka  jaber soufiyan
**Big Data Engineering | Année universitaire 2025–2026**

Ce TP a pour objectif de maîtriser **Apache Kafka**, système de streaming distribué, à travers des manipulations pratiques : production/consommation de messages, ingestion avec Kafka Connect, traitement en temps réel avec Kafka Streams, et configuration d’un cluster multi-brokers.

---

## 🎯 Objectifs

- ✅ Installer et démarrer Kafka + ZooKeeper dans un environnement Docker (cluster Hadoop)
- ✅ Créer, lister et décrire des **topics Kafka**
- ✅ Utiliser les outils en ligne de commande (`kafka-console-producer`, `kafka-console-consumer`)
- ✅ Développer des applications Java utilisant les API `KafkaProducer` et `KafkaConsumer`
- ✅ Mettre en œuvre **Kafka Connect** pour ingérer des données depuis un fichier vers Kafka, puis vers un autre fichier
- ✅ Implémenter une application **Kafka Streams** pour le **Word Count en temps réel**
- ✅ Configurer un **cluster Kafka multi-brokers** (3 brokers)
- ✅ Visualiser le cluster avec **Kafka UI**

---

## 📁 Structure du projet
kafka_lab/
├── pom.xml
├── src/
│ └── main/
│ └── java/
│ └── edu/supmti/kafka/
│ ├── EventProducer.java # Producteur Kafka personnalisé
│ ├── EventConsumer.java # Consommateur Kafka personnalisé
│ └── WordCountApp.java # Application Kafka Streams (comptage de mots)
└── README.md

---

## ⚙️ Technologies utilisées

- **Apache Kafka** 3.5.1  
- **Kafka Streams** & **Kafka Connect**  
- **Java 8**  
- **Maven**  
- **Docker** (conteneurs `hadoop-master`, `hadoop-slave1`, `hadoop-slave2`)  
- **ZooKeeper** (coordonnateur du cluster Kafka)

---

## ▶️ Instructions d'exécution

### 1. Compiler le projet
```bash
mvn clean package
