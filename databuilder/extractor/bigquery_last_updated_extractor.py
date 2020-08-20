# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import namedtuple

from pyhocon import ConfigTree  # noqa: F401
from typing import List, Any  # noqa: F401

from databuilder.extractor.base_bigquery_extractor import BaseBigQueryExtractor
#from databuilder.models.application import Application
from databuilder.models.table_last_updated import TableLastUpdated

#TEST
from datetime import datetime



DatasetRef = namedtuple('DatasetRef', ['datasetId', 'projectId'])
TableKey = namedtuple('TableKey', ['schema', 'table_name'])

LOGGER = logging.getLogger(__name__)


class BigQueryLastUpdatedExtractor(BaseBigQueryExtractor):

    """ 

    """
    #PROJECT_ID_KEY = 'project_id'

    def init(self, conf):
        # type: (ConfigTree) -> None
        BaseBigQueryExtractor.init(self, conf)
        self.grouped_tables = set([])

    def _retrieve_tables(self, dataset):
        # type: () -> Any
        for page in self._page_table_list_results(dataset):
            if 'tables' not in page:
                continue

            for table in page['tables']:
                tableRef = table['tableReference']
                table_id = tableRef['tableId']

                # BigQuery tables that have 8 digits as last characters are
                # considered date range tables and are grouped together in the UI.
                # ( e.g. ga_sessions_20190101, ga_sessions_20190102, etc. )
                if self._is_sharded_table(table_id):
                    # If the last eight characters are digits, we assume the table is of a table date range type
                    # and then we only need one schema definition
                    table_prefix = table_id[:-BigQueryLastUpdatedExtractor.DATE_LENGTH]
                    if table_prefix in self.grouped_tables:
                        # If one table in the date range is processed, then ignore other ones
                        # (it adds too much metadata)
                        continue

                    table_id = table_prefix
                    self.grouped_tables.add(table_prefix)

                table = self.bigquery_service.tables().get(
                    projectId=tableRef['projectId'],
                    datasetId=tableRef['datasetId'],
                    tableId=tableRef['tableId']).execute(num_retries=BigQueryLastUpdatedExtractor.NUM_RETRIES)

                # BigQuery tables also have interesting metadata about partitioning
                # data location (EU/US), mod/create time, etc... Extract that some other time?
                #cols = []
                # Not all tables have schemas9
                #if 'schema' in table:
                #    schema = table['schema']
                #    if 'fields' in schema:
                #        total_cols = 0
                #        for column in schema['fields']:
                #            total_cols = self._iterate_over_cols('', column, cols, total_cols + 1)

                #table_app = Application(
                #    task_id='la_task',  # type: str
                #    dag_id='el_id',  # type: str,
                #    application_url_template='el_template',  # type: str
                #    db_name='bigquery',  # type: str
                #    cluster=tableRef['projectId'],  # type: str
                #    schema=tableRef['datasetId'],  # type: str
                #    table_name=table_id,  # type: str
                #    exec_date='20200811'
                #)

                #TableLastUpdated
                #def __init__(self,
                # table_name,  # type: str
                # last_updated_time_epoch,  # type: int
                # schema,  # type: str
                # db='hive',  # type: str
                # cluster='gold'  # type: str
                # ):

                
                # TEST                
                print("########")
                print(table_id)
                print(table['lastModifiedTime'])
                #print()
                datetime.utcfromtimestamp(int(table['lastModifiedTime'])//1000).strftime('%Y-%m-%d %H:%M:%S')

                print("########")



                table_last_upd = TableLastUpdated(table_name=table_id, last_updated_time_epoch=int(table['lastModifiedTime'])//1000,schema=tableRef['datasetId'],db='bigquery',cluster=tableRef['projectId'])
                    
                    #database='bigquery',
                    #cluster=tableRef['projectId'],
                    #schema=tableRef['datasetId'],
                    #name=table_id,
                    #description=table.get('description', ''),
                    #columns=cols,
                    #is_view=table['type'] == 'VIEW')

                yield(table_last_upd)

  
    def get_scope(self):
        # type: () -> str
        return 'extractor.bigquery_table_metadata'
