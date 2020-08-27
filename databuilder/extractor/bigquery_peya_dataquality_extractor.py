# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import namedtuple

from pyhocon import ConfigTree  # noqa: F401
from typing import List, Any  # noqa: F401

from databuilder.extractor.base_bigquery_extractor import BaseBigQueryExtractor
#from databuilder.models.application import Application
from databuilder.models.table_last_updated import TableLastUpdated

from databuilder.models.table_metadata import *


#TEST
from datetime import datetime
import requests
import json


DatasetRef = namedtuple('DatasetRef', ['datasetId', 'projectId'])
TableKey = namedtuple('TableKey', ['schema', 'table_name'])

LOGGER = logging.getLogger(__name__)


class BigQueryPeyaDQExtractor(BaseBigQueryExtractor):

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
                    table_prefix = table_id[:-BigQueryPeyaDQExtractor.DATE_LENGTH]
                    if table_prefix in self.grouped_tables:
                        # If one table in the date range is processed, then ignore other ones
                        # (it adds too much metadata)
                        continue

                    table_id = table_prefix
                    self.grouped_tables.add(table_prefix)

                #table = self.bigquery_service.tables().get(
                #    projectId=tableRef['projectId'],
                #    datasetId=tableRef['datasetId'],
                #    tableId=tableRef['tableId']).execute(num_retries=BigQueryPeyaDQExtractor.NUM_RETRIES)

                #table_meta = TableMetadata(
                #    database='bigquery',
                #    cluster=tableRef['projectId'],
                #    schema=tableRef['datasetId'],
                #    name=table_id,
                #    description=f'{table.get("description", "")}\nYou can find the table [here]({link}).',
                #    columns=cols,
                #    is_view=table['type'] == 'VIEW')

                desc_quality = 'N/A'

                if table_id == 'cart_checkout_raw_data'  :
                    payload = {'table_id': table_id}
                    resp = requests.post('http://localhost:16000/tables', params=payload)
                    desc_quality = 'DQ value : {} \n DQ value 2: {}'.format(resp.json()['dq'],resp.json()['dq2'])

                table_dq = TableMetadata(
                    database='bigquery',
                    cluster=tableRef['projectId'],
                    schema=tableRef['datasetId'],
                    name=table_id,
                    description=desc_quality,
                    description_source = 'quality_service',
                    is_view=table['type'] == 'VIEW')
                    
                    
                yield(table_dq)

  
    def get_scope(self):
        # type: () -> str
        return 'extractor.bigquery_table_metadata'