from typing import List,Dict,Any
from httplib2 import Credentials
from googleapiclient import discovery
import logging
import sys

class ServerlessManager:
  def __init__(self,project_name:str, credentials:Credentials):
    self.project_name = project_name
    self.credentials = credentials

  async def get_cloudfunctions(self) -> List[Dict[str, Any]]:
    """Retrieve a list of CloudFunctions available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of CloudFunctions in the project.
    """

    logging.info("Retrieving CloudFunctions")
    functions_list = list()
    service = discovery.build(
        "cloudfunctions", "v1", credentials=self.credentials, cache_discovery=False)
    try:
      request = service.projects().locations().functions().list(
          parent=f"projects/{self.project_name}/locations/-")
      while request is not None:
        response = request.execute()
        for function in response.get("functions", []):
          functions_list.append(function)

        request = service.projects().locations().functions().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve CloudFunctions for project %s", self.project_name)
      logging.info(sys.exc_info())

    return functions_list

  async def get_app_services(self) -> Dict[str, Any]:
    """Retrieve a list of AppEngine instances available in the project.

    Args:
      project_name: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A dict representing default apps and services available in the project.
    """

    app_client = discovery.build(
        "appengine", "v1", credentials=self.credentials, cache_discovery=False)

    logging.info("Retrieving app services")
    app_services = dict()
    try:
      request = app_client.apps().get(appsId=self.project_name)
      response = request.execute()
      if response.get("name", None) is not None:
        app_services["default_app"] = (response["name"],
                                      response["defaultHostname"],
                                      response["servingStatus"])

      request = app_client.apps().services().list(appsId=self.project_name)

      app_services["services"] = list()
      while request is not None:
        response = request.execute()
        for service_entry in response.get("services", []):
          app_services["services"].append(service_entry)

        request = app_client.apps().services().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve App services for project %s", self.project_name)
      logging.info(sys.exc_info())
    return app_services

  def crawl(self):
    pass