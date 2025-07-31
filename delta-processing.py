# Databricks notebook source
# MAGIC %run ./requirements

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql import functions as f
import json

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


def process_json_response(sdf: DataFrame, ddl_schema: str) -> DataFrame:
    sdf = sdf.withColumn(
        "DH_JsonResponseResults",
        f.from_json(f.col("DH_JsonResponseResults"), ddl_schema),
    )
    sdf = sdf.select("*", "DH_JsonResponseResults.*").drop("DH_JsonResponseResults")
    return sdf


# COMMAND ----------


class ProcessingTestAPI(ProcessingOverwrite):
    def tabularise(self, sdf: DataFrame, table_name: str, **properties) -> DataFrame:
        """Function to bring ingested data into a tabular format. Complex columns are dropped and the categories column keeps only their names in a list.

        Args:
            sdf: DataFrame where the load function, e.g. load json, has been applied
            table_name: Table to be processed as camel case and starts with F for Fact or D for Dimension e.g. DIgUsers
            **properties: Optional paramaters

        Returns:
            A Data Frame with the tabularised data
        """
        self.logger.info("Tabularisation started")
        ddl_schema = self.config_dict["ddl_schema"][table_name]
        sdf = process_json_response(sdf, ddl_schema)
        sdf.display()
        sdf = sdf.withColumn(
            "identificationNumber", f.explode_outer("identificationNumber")
        )
        result = (
            sdf.withColumn("gtin", f.col("identificationNumber.value"))
            .withColumn("gtinType", f.col("identificationNumber.type"))
            .drop("identificationNumber")
        )

        return result


# COMMAND ----------

table_name_list = json.loads(dbutils.widgets.get("table_name_list"))

# COMMAND ----------

processing = ProcessingTestAPI(
    table_name_list=table_name_list,
    identifier_column_name="identifier",
    identifier_value_list=None,
    merge_schema=True,
)
processing.process_all_properties(threading=True, debug=True)
