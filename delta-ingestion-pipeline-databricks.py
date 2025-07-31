# Databricks notebook source
# MAGIC %run ./requirements

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# way to define the widgets
# from datetime import datetime, timedelta
# dbutils.widgets.text('domain','testDomain')
# dbutils.widgets.text('data_source_name','Test')
# dbutils.widgets.text('data_source_token','testtoken')
# dbutils.widgets.text('frequency','daily')
# dbutils.widgets.text("processing_mode", "all")
# dbutils.widgets.text("table_name_list",'["DTestTable"]')
# dbutils.widgets.text("start_date", (datetime.now() - timedelta(1)).strftime("%Y-%m-%d"))
# dbutils.widgets.text("end_date", datetime.now().strftime("%Y-%m-%d"))

# COMMAND ----------

import requests
import json
import numpy as np
from datetime import datetime
from pyspark.sql import functions as f
import concurrent.futures

# COMMAND ----------


class IngestionTestAPI(DeltaIngestion):
    def get_num_pages(
        self,
        page_size: int,
        headers: dict,
    ) -> float:
        """Function that returns the number of pages based on page size.

        Args:
            page_size: number of records per page
        Returns:
            float
        """
        url = "test_api add your api here"

        response = requests.get(url, headers=headers)
        assert (
            response.status_code == 200
        ), f"Error 'get_api_data', response status is : {response.status_code} "
        total_materials = json.loads(response.text)["data"]["results"]
        total_pages = np.ceil(total_materials / page_size)
        return total_pages

    def get_api_data(
        self, page_index: int, page_size: int, headers: dict
    ) -> requests.Response:
        """Function that requests json data from the Product API for a page.

        Args:
            page_index: Index of the requested page
            page_size: number of records per page

        Returns:
            Response object
        """
        params = {
            "pageIndex": page_index,
            "pageSize": page_size,
            "includeFields": "if it supports else remove it",
        }
        url = f"test api url"
        response = requests.get(url, params, headers=headers)
        assert (
            response.status_code == 200
        ), f"Error 'get_api_data', response status is : {response.status_code} for page_index :{page_index} "
        if response.status_code != 200:
            print(response.text)
        return response

    def get_data_batch_generator(
        self, table_name: str, num_pages: int, page_size: int, headers: dict
    ):
        """Function to get the data from the data source.
        Args:
            table_name: Table to be processed as camel case and starts with F for Fact or D for Dimension
            identifier_value: Value of the identifier: customer_id
            format_type: Format the batch from source should be saved. E.g. json
            config: the config class that defines the queries, transformations, ...
        Returns:
            Dataframe
        """
        for page_index in range(int(num_pages)):
            try:
                self.logger.info(f"starting ingesting for page_index: {page_index+1}")
                response = self.get_api_data(page_index + 1, page_size, headers)
                response_data_list = json.loads(response.text)["data"]["items"]

# this will not give driver memory issue on  databricks or spark.rpc.message.maxSize , if large response is returned by an api server.

                for row in response_data_list:
                    yield {"DH_JsonResponseResults": json.dumps(row)}
                self.logger.info(f"completed: ingesting for page_index: {page_index+1}")

            except Exception as e:
                self.error_log.add_error(
                    table_name,
                    str(page_index + 1),
                    e,
                )

    def get_data_batch(self, table_name: str, identifier_value: str):
        """Function to get the data from the data source.
        Args:
            table_name: Table to be processed as camel case and starts with F for Fact or D for Dimension
            identifier_value: Value of the identifier: customer_id
            format_type: Format the batch from source should be saved. E.g. json
            config: the config class that defines the queries, transformations, ...
        Returns:
            Dataframe
        """
        page_size = 40

        key_api_unified = dbutils.secrets.get(
            scope="kv-scope", key="test-token"
        )
        tUnified = "if required"
        headers = {
            "Accept": "application/json",
            "tenant": tUnified,
            "lang": "en_GB",
            "country": "XX",
            "KeyId": key_api_unified,
            "Accept-Encoding": "gzip, deflate, br",
        }

        num_pages = self.get_num_pages(page_size, headers)
        self.logger.info(f"num of pages are :{num_pages}")

        sdf = spark.createDataFrame(
            self.get_data_batch_generator(table_name, num_pages, page_size, headers),
            schema="DH_JsonResponseResults string",
        )
        if sdf is None or sdf.isEmpty():
            return None
        else:
            return sdf


# COMMAND ----------

table_name_list = json.loads(dbutils.widgets.get("table_name_list"))
ingestion = IngestionTestAPI(
    table_name_list=table_name_list,
    identifier_value_list=None,
    merge_schema=True,
)

ingestion.ingest_all_properties(debug=False, threading=False)
