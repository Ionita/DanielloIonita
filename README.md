# Systems and Architectures for Big Data: Project 1
Project for the course of Systems and Architectures for Big Data at University of Rome Tor Vergata
In this github page you can find the entire Java code and a directory containing the runnable. If you are not interested in 
code you can download only RUNNER_DIRECTORY but remember, you have to move the file d14_filtered from textfiles/ to RUNNER_DIRECTORY.

## Introduction to the project
http://www.ce.uniroma2.it/courses/sabd1718/projects/SABD1718_progetto1.pdf

At this link you can find the project requirements in italian. For the english translation read the project report

## Prerequisites
In order to run this project you need the following softwares to be installed on your computer:
* HDFS (http://hadoop.apache.org/releases.html)
* Spark (https://spark.apache.org/downloads.html)
* MongoDB (https://www.mongodb.com/download-center)
* NiFi (optional) (https://nifi.apache.org/download.html)

Note that in NiFi templates we used the standard hadoop directory /usr/local/hadoop. If your directory is different please change the NiFi template configuration setting the correct directory. For problems write an email to simone.daniello@alumni.uniroma2.eu 

## Content of this page
In this page you can find the entire Java code, the report and files needed for the execution.

## RUNNER_DIRECTORY
This is the most important folder of the project. Inside this folder you can find:
* SABDanielloIonita_report.pdf : it is the project report
* SABDanielloIonita.jar : the executable jar (needed in order to run runner_sabd.sh)
* SaveOnMongoDB_OSX.sh : script to save local results on mongoDB, OSX version (needed in order to run runner_sabd.sh but can be run independently)
* SaveOnMongoDB_linux.sh : script to save local results on mongoDB, linux version (needed in order to run runner_sabd.sh but can be run independently)
* SABD_SaveResultsOnLocalStorage.xml : second NiFi template needed to download file from HDFS
* SABD_PutHDFS.xml : first NiFi template needed to load d14_filtered on HDFS

## Run
In order to launch this project you have to run the script runner_SABD.sh inside the directory RUNNER_DIRECTORY. You have to move d14_filtered.csv from /textfiles to this directory before doing it. Remember to run the script inside RUNNER_DIRECTORY.

## Owners
D'Aniello Simone - Data Science and Engineering - University of Rome Tor Vergata

Ionita Marius Dragos - Data Science and Engineering - University of Rome Tor Vergata
