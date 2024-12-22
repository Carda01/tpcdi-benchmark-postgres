# TPC-DI benchmark in PostgreSQL
This proyect aims to perform and improve the TPC-DI benchmark in PostgreSQL for the class Data Warehouses of the Erasmus Mundus Masters program BDMA. In this README.md file we will explain in detail how to set up and run the proyect. This process should be pretty straitfordward since we have make a big effort on making it easy for every operating system.

## REQUIREMENTS
 1. Having Docker installed and running.
 2. Having PostgreSQL 16 installed and running. 
 3. Having PgAdmin or some sort of PostgreSQL CLI tool.
 
## HOW TO SET UP
The set up is programmed in different python notebooks so the user just must click through the cells.
1. Open and run src/setup. Also make sure that you follow the instructions written between cells.  This notebook will install and configure Airflow in Docker and connect it to your PostgreSQL database. 
2. Open and run src/create_sf for every Scale Factor that you want to test. This notebook will create the necessary DAGs and tasks for one scale factor that will perform the benchmark.
3. Now everything should be up and working. Feel free to run and test any DAG or task, they can be run from terminal or from the CLI app.
