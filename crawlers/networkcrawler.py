from typing import List,Any,Dict
from httplib2 import Credentials
from googleapiclient import discovery
import logging
import sys

class NetworkManager:
  def __init__(self,project_name:str,credentials:Credentials):
    self.project_name = project_name
    self.credentials = credentials
  
  async def get_managed_zones(self) -> List[Dict[str, Any]]:
    """Retrieve a list of DNS zones available in the project.

    Args:
      project_name: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of DNS zones in the project.
    """

    logging.info("Retrieving DNS Managed Zones")
    zones_list = list()

    try:
      service = discovery.build(
          "dns", "v1", credentials=self.credentials, cache_discovery=False)

      request = service.managedZones().list(project=self.project_name)
      while request is not None:
        response = request.execute()

        for managed_zone in response["managedZones"]:
          zones_list.append(managed_zone)

        request = service.managedZones().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to enumerate DNS zones for project %s", self.project_name)
      logging.info(sys.exc_info())

    return zones_list


  async def get_kms_keys(self) -> List[Dict[str, Any]]:
    """Retrieve a list of KMS keys available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of KMS keys in the project.
    """

    logging.info("Retrieving KMS keys")
    kms_keys_list = list()
    try:
      service = discovery.build(
          "cloudkms", "v1", credentials=self.credentials, cache_discovery=False)

      # list all possible locations
      locations_list = list()
      request = service.projects().locations().list(name=f"projects/{self.project_name}")
      while request is not None:
        response = request.execute()
        for location in response.get("locations", []):
          locations_list.append(location["locationId"])
        request = service.projects().locations().list_next(
            previous_request=request, previous_response=response)

      for location_id in locations_list:
        request_loc = service.projects().locations().keyRings().list(
            parent=f"projects/{self.project_id}/locations/{location_id}")
        while request_loc is not None:
          response_loc = request_loc.execute()
          for keyring in response_loc.get("keyRings", []):
            request = service.projects().locations().keyRings().cryptoKeys().list(
                parent=keyring["name"])
            while request is not None:
              response = request.execute()
              for key in response.get("cryptoKeys", []):
                kms_keys_list.append(key)

              request = service.projects().locations().keyRings().cryptoKeys(
              ).list_next(
                  previous_request=request, previous_response=response)

          request_loc = service.projects().locations().keyRings().list_next(
              previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve KMS keys for project %s", self.project_id)
      logging.info(sys.exc_info())
    return kms_keys_list


  async def get_endpoints(self) -> List[Dict[str, Any]]:
    """Retrieve a list of Endpoints available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of Endpoints in the project.
    """

    logging.info("Retrieving info about endpoints")
    endpoints_list = list()
    try:
      service = discovery.build(
          "servicemanagement",
          "v1",
          credentials=self.credentials,
          cache_discovery=False)

      request = service.services().list(producerProjectId=self.project_name)
      while request is not None:
        response = request.execute()
        for service_entry in response.get("services", []):
          endpoints_list.append(service_entry)

        request = service.services().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve endpoints list for project %s", self.project_name)
      logging.info(sys.exc_info())
    return endpoints_list

  def crawl(self):
    pass