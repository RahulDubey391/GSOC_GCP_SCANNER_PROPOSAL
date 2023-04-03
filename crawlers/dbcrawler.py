import logging
from googleapiclient import discovery
from typing import Dict,List,Any
from httplib2 import Credentials
import sys

class DBManager:
  def __init__(self,project_name: str,credentials: Credentials):
    self.project_name = project_name
    self.credentails = credentials

    
  async def get_sql_instances(self) -> List[Dict[str, Any]]:
    """Retrieve a list of SQL instances available in the project.

    Args:
      project_name: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of sql instances in the project.
    """

    logging.info("Retrieving CloudSQL Instances")
    sql_instances_list = list()
    try:
      service = discovery.build(
          "sqladmin", "v1beta4", credentials=self.credentials, cache_discovery=False)

      request = service.instances().list(project=self.project_name)
      while request is not None:
        response = request.execute()
        for database_instance in response.get("items", []):
          sql_instances_list.append(database_instance)

        request = service.instances().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to get SQL instances for project %s", self.project_name)
      logging.info(sys.exc_info())

    return sql_instances_list


  async def get_bq_tables(self,project_id: str, dataset_id: str,
                    bq_service: discovery.Resource) -> List[Dict[str, Any]]:
    """Retrieve a list of BigQuery tables available in the dataset.

    Args:
      project_id: A name of a project to query info about.
      dataset_id: A name of dataset to query data from.
      bq_service: I do not know.

    Returns:
      A list of BigQuety tables in the dataset.
    """

    logging.info("Retrieving BigQuery Tables for dataset %s", dataset_id)
    list_of_tables = list()
    try:
      request = bq_service.tables().list(
          projectId=project_id, datasetId=dataset_id)
      while request is not None:
        response = request.execute()

        for table in response.get("tables", []):
          list_of_tables.append(table)

        request = bq_service.tables().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve BQ tables for dataset %s", dataset_id)
      logging.info(sys.exc_info())
    return list_of_tables


  async def get_bq(self) -> Dict[str, List[Dict[str, Any]]]:
    """Retrieve a list of BigQuery datasets available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A dictionary of BigQuery dataset and corresponding tables.
    """

    logging.info("Retrieving BigQuery Datasets")
    bq_datasets = dict()
    try:
      service = discovery.build(
          "bigquery", "v2", credentials=self.credentials, cache_discovery=False)

      request = service.datasets().list(projectId=self.project_name)
      while request is not None:
        response = request.execute()

        for dataset in response.get("datasets", []):
          dataset_id = dataset["datasetReference"]["datasetId"]
          bq_datasets[dataset_id] = self.get_bq_tables(self.project_name, dataset_id, service)

        request = service.datasets().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve BQ datesets for project %s", self.project_name)
      logging.info(sys.exc_info())
    return bq_datasets

  async def get_bigtable_instances(self) -> List[Dict[str, Any]]:
    """Retrieve a list of BigTable instances available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of BigTable instances in the project.
    """

    logging.info("Retrieving bigtable instances")
    bigtable_instances_list = list()
    try:
      service = discovery.build(
          "bigtableadmin", "v2", credentials=self.credentials, cache_discovery=False)

      request = service.projects().instances().list(
          parent=f"projects/{self.project_name}")
      while request is not None:
        response = request.execute()
        for instance in response.get("instances", []):
          bigtable_instances_list.append(instance)

        request = service.projects().instances().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve BigTable instances for project %s",
                  self.project_name)
      logging.info(sys.exc_info())
    return bigtable_instances_list


  async def get_spanner_instances(self) -> List[Dict[str, Any]]:
    """Retrieve a list of Spanner instances available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of Spanner instances in the project.
    """

    logging.info("Retrieving spanner instances")
    spanner_instances_list = list()
    try:
      service = discovery.build(
          "spanner", "v1", credentials=self.credentials, cache_discovery=False)

      request = service.projects().instances().list(
          parent=f"projects/{self.project_name}")
      while request is not None:
        response = request.execute()
        for instance in response.get("instances", []):
          spanner_instances_list.append(instance)

        request = service.projects().instances().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve Spanner instances for project %s",
                  self.project_name)
      logging.info(sys.exc_info())
    return spanner_instances_list

  def crawl(self):
    pass