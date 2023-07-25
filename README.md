# Asynchronous data extractor
This repository contains building blocks and interfaces for a data-processing pipeline intended for asynchronous extraction of datasets from SQL databases.
Base classes may be extended to match given requirement. MS SQL connector and Pandas CSV data writer have been implemented as a sample solution.

## Connectors
Database connector objects can be found in detk/connectors. Connector should be implemented for given type od database. 
MSSqlPDConnector has been defined for connecting to an MS SQL Database. It uses SQLAlchemy to establish the connection and Pandas methods to extract data. 

## Writers
Data writer objects can be found in detk/writers. Writer should be implemented for given output solution.
PDCsvWriter uses Pandas methods to save dataset to csv file.

## Pipeline configuration
Data pipelines can be found in detk/dataprocessing. 
DataPipeline uses asynchronous methods to extract and process data as Pandas dataframes.
DataChunksPipeline derived from multiprocessing process uses a queue and asynchronous methods to extract and process data in chunks as Pandas dataframes.

## Running the pipeline
Pipeline must first be defined in pipeline_config.yaml configuration file. To run it from repo folder:

    run_extractor.py -o output_path