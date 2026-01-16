# Hadoop HDFS – Configuration et mise en place

## 1. Architecture du cluster

Le stockage distribué repose sur **Hadoop HDFS** avec une architecture multi-nœuds :

- **1 NameNode** : gestion des métadonnées HDFS  
- **1 SecondaryNameNode** : gestion des checkpoints  
- **2 DataNodes** : stockage des blocs de données  


---

## 2. Machines virtuelles

| Rôle | Hostname | Adresse IP | Utilisateur |
|----|----|----|----|
| NameNode + SecondaryNameNode | vm-datalake | 172.31.249.134 | hadoop |
| DataNode | vm-datanode-1 | 172.31.250.130 | hadoop |
| DataNode | vm-datanode-2 | 172.31.249.137 | hadoop |

---

## 3. Environnement logiciel

- **OS** : Ubuntu Server 24.04 LTS  
- **Java** : OpenJDK 11  
- **Hadoop** : 3.3.6  

---

## 4. Variables d’environnement (utilisateur `hadoop`)

```bash
export HADOOP_HOME=/home/hadoop/apps/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin