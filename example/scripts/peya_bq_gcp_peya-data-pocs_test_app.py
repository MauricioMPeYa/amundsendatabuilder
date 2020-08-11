# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script demonstrating how to load data into Neo4j and
Elasticsearch without using an Airflow DAG.

It contains several jobs:
- `run_csv_job`: runs a job that extracts table data from a CSV, loads (writes)
  this into a different local directory as a csv, then publishes this data to
  neo4j.
- `run_table_column_job`: does the same thing as `run_csv_job`, but with a csv
  containing column data.
- `create_last_updated_job`: creates a job that gets the current time, dumps it
  into a predefined model schema, and publishes this to neo4j.
- `create_es_publisher_sample_job`: creates a job that extracts data from neo4j
  and pubishes it into elasticsearch.

For other available extractors, please take a look at
https://github.com/lyft/amundsendatabuilder#list-of-extractors
"""

import logging
import os
import sqlite3
import sys
import uuid

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.csv_extractor import CsvTableColumnExtractor, CsvExtractor
from databuilder.extractor.neo4j_es_last_updated_extractor import Neo4jEsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_constants import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, \
    USER_ELASTICSEARCH_INDEX_MAPPING
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import ChainedTransformer
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS
from databuilder.transformer.generic_transformer import GenericTransformer, CALLBACK_FUNCTION, FIELD_NAME


from databuilder.extractor.hive_table_metadata_extractor import *

from databuilder.extractor.hive_table_last_updated_extractor import *

from databuilder.extractor.bigquery_metadata_extractor import *

from databuilder.extractor.bigquery_watermark_extractor import *

from databuilder.extractor.bigquery_usage_extractor import *

from databuilder.extractor.bigquery_application_extractor import *

from databuilder.transformer.bigquery_usage_transformer import BigqueryUsageTransformer

from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher


import time
#import hdfs3

#es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
#neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'localhost')


es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', '35.223.142.185')
neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', '34.66.160.232')


es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)
if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

#DB_FILE = '/tmp/test.db'
#SQLITE_CONN_STRING = 'sqlite:////tmp/test.db'
#Base = declarative_base()

NEO4J_ENDPOINT = 'bolt://{}:{}'.format(neo_host, neo_port)

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

LOGGER = logging.getLogger(__name__)


#def create_connection(db_file):
#    try:
#        conn = sqlite3.connect(db_file)
#        return conn
#    except Exception:
#        LOGGER.exception('exception')
#    return None


#def connection_string():
#    return "mysql+pymysql://hiveuser:hivepassword@192.168.20.201:3306/metastore"


def create_es_publisher_sample_job(elasticsearch_index_alias='table_search_index',
                                   elasticsearch_doc_type_key='table',
                                   model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                                   entity_type='table',
                                   elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_{uuid}`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param entity_type:                Entity type handed to the `Neo4jSearchDataExtractor` class, used to determine
                                       Cypher query to extract data from Neo4j. Defaults to `table`.
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = '{}_'.format(elasticsearch_doc_type_key) + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.entity_type': entity_type,
        'extractor.search_data.extractor.neo4j.graph_url': neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.model_class': model_name,
        'extractor.search_data.extractor.neo4j.neo4j_auth_user': neo4j_user,
        'extractor.search_data.extractor.neo4j.neo4j_auth_pw': neo4j_password,
        'extractor.search_data.extractor.neo4j.neo4j_encrypted': False,
        'loader.filesystem.elasticsearch.file_path': extracted_search_data_path,
        'loader.filesystem.elasticsearch.mode': 'w',
        'publisher.elasticsearch.file_path': extracted_search_data_path,
        'publisher.elasticsearch.mode': 'r',
        'publisher.elasticsearch.client': elasticsearch_client,
        'publisher.elasticsearch.new_index': elasticsearch_new_index_key,
        'publisher.elasticsearch.doc_type': elasticsearch_doc_type_key,
        'publisher.elasticsearch.alias': elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job



def run_bq_job(job_name):

    #where_clause_suffix = " "
    gcloud_project = "peya-data-pocs"
    #label_filter = ""

    tmp_folder = '/var/tmp/amundsen/{job_name}'.format(job_name=job_name)
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)


    job_config = ConfigFactory.from_dict({
            'extractor.bigquery_table_metadata.{}'.format(
            BigQueryMetadataExtractor.PROJECT_ID_KEY
            ): gcloud_project,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
        })

    #if label_filter:
    #    job_config[
    #        'extractor.bigquery_table_metadata.{}'
    #        .format(BigQueryMetadataExtractor.FILTER_KEY)
    #        ] = label_filter

    task = DefaultTask(extractor=BigQueryMetadataExtractor(),
                       loader=FsNeo4jCSVLoader(),
                       transformer=NoopTransformer())

    job = DefaultJob(conf=ConfigFactory.from_dict(job_config),
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_bq_wm_job(job_name):

    #where_clause_suffix = " "
    gcloud_project = "peya-data-pocs"
    #label_filter = ""

    tmp_folder = '/var/tmp/amundsen/{job_name}'.format(job_name=job_name)
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)


    job_config = ConfigFactory.from_dict({
            'extractor.bigquery_watermarks.{}'.format(
            BigQueryWatermarkExtractor.PROJECT_ID_KEY
            ): gcloud_project,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
        })

    #if label_filter:
    #    job_config[
    #        'extractor.bigquery_table_metadata.{}'
    #        .format(BigQueryMetadataExtractor.FILTER_KEY)
    #        ] = label_filter

    task = DefaultTask(extractor= BigQueryWatermarkExtractor(),
                       loader=FsNeo4jCSVLoader(),
                       transformer=NoopTransformer())

    job = DefaultJob(conf=ConfigFactory.from_dict(job_config),
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_bq_tu_job(job_name):
    
    #where_clause_suffix = " "
    gcloud_project = "peya-data-pocs"
    #label_filter = ""

    tmp_folder = '/var/tmp/amundsen/{job_name}'.format(job_name=job_name)
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    bq_usage_extractor = BigQueryTableUsageExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=bq_usage_extractor,
                       loader=csv_loader,
                       transformer=BigqueryUsageTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.bigquery_table_usage.{}'.format(BigQueryTableUsageExtractor.PROJECT_ID_KEY):
            gcloud_project,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR):
            True,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })


    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())    

    job.launch()



def run_bq_app_job(job_name):
    
    #where_clause_suffix = " "
    
    gcloud_project = "peya-data-pocs"
    #label_filter = ""

    tmp_folder = '/var/tmp/amundsen/{job_name}'.format(job_name=job_name)
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)


    job_config = ConfigFactory.from_dict({
            'extractor.bigquery_table_application.{}'.format(
            BigQueryApplicationExtractor.PROJECT_ID_KEY
            ): gcloud_project,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
        })

    #if label_filter:
    #    job_config[
    #        'extractor.bigquery_table_metadata.{}'
    #        .format(BigQueryMetadataExtractor.FILTER_KEY)
    #        ] = label_filter

    task = DefaultTask(extractor=BigQueryApplicationExtractor(),
                       loader=FsNeo4jCSVLoader(),
                       transformer=NoopTransformer())

    job = DefaultJob(conf=ConfigFactory.from_dict(job_config),
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)
    

    start_time_total = time.time()
    
    print("EMPIEZA A CORRER EL DATABUILDER...")

#    if create_connection(DB_FILE):
        #run_csv_job('example/sample_data/sample_table_column_stats.csv', 'test_table_column_stats',
        #            'databuilder.models.table_stats.TableColumnStats')
    print("EMPIEZA A CORRER EL JOB DE CARGA DE METADATA DE TABLAS...")

    start_time_neo4jtbl = time.time()

    run_bq_job("test_bq")

    print("TERMINA DE CORRER EL JOB DE CARGA DE METADATA DE TABLAS...")
    print("--- tiempo total de ejecucion de carga de metadata en neo4j %s seconds ---" % (time.time() - start_time_neo4jtbl))

    print("EMPIEZA A CORRER EL JOB DE WATERMARKS...")

    start_time_wm = time.time()

    run_bq_wm_job("test_bq_wm")

    print("TERMINA DE CORRER EL JOB DE WATERMARKS...")
    print("--- tiempo total de ejecucion de carga de watermarks en neo4j %s seconds ---" % (time.time() - start_time_wm))

    print("EMPIEZA A CORRER EL JOB DE TABLE USAGE...")

    start_time_tu = time.time()

    run_bq_tu_job("test_bq_tu")
    
    print("TERMINA DE CORRER EL JOB DE TABLE USAGE...")
    print("--- tiempo total de ejecucion de carga de table usage info en neo4j %s seconds ---" % (time.time() - start_time_tu))

    print("EMPIEZA A CORRER EL JOB DE APPLICATION...")

    start_time_app = time.time()

    run_bq_app_job("test_bq_app")
    
    print("TERMINA DE CORRER EL JOB DE APPLICATION...")
    print("--- tiempo total de ejecucion de carga de application info en neo4j %s seconds ---" % (time.time() - start_time_tu))


    job_es_table = create_es_publisher_sample_job(
        elasticsearch_index_alias='table_search_index',
        elasticsearch_doc_type_key='table',
        entity_type='table',
        model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
    job_es_table.launch()

    print("TERMINA DE CORRER DATABUILDER...")

    print("--- tiempo total de ejecucion %s seconds ---" % (time.time() - start_time_total))
        #create_last_updated_job().launch()


