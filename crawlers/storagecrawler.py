from typing import Dict, Tuple, Any, List
import googleapiclient
from googleapiclient import discovery
from httplib2 import Credentials
import logging
import sys
import json
import io

class StorageManager:
  def __init__(self,project_name:str,credentials:Credentials,dump_fd:io.TextIOWrapper):
    self.project_name = project_name
    self.credentials = credentials
    self.dump_fd = dump_fd
  
  async def get_bucket_names(self) -> Dict[str, Tuple[Any, List[Any]]]:
    """Retrieve a list of buckets available in the project.

    Args:
      project_name: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.
      dump_fd: If set, the function will enumerate files stored in buckets and
        save them in a file corresponding to provided file descriptor.
        This is a very slow, noisy operation and should be used with caution.

    Returns:
      A dictionary where key is bucket name and value is a bucket Object.
    """

    logging.info("Retrieving GCS Buckets")
    buckets_dict = dict()
    service = discovery.build(
        "storage", "v1", credentials=self.credentials, cache_discovery=False)
    # Make an authenticated API request
    request = service.buckets().list(project=self.project_name)
    while request is not None:
      try:
        response = request.execute()
      except googleapiclient.errors.HttpError:
        logging.info("Failed to list buckets in the %s", self.project_name)
        logging.info(sys.exc_info())
        break

      for bucket in response.get("items", []):
        buckets_dict[bucket["name"]] = (bucket, None)
        if self.dump_fd is not None:
          ret_fields = "nextPageToken,items(name,size,contentType,timeCreated)"

          req = service.objects().list(bucket=bucket["name"], fields=ret_fields)

          while req:
            try:
              resp = req.execute()
              for item in resp.get("items", []):
                self.dump_fd.write(json.dumps(item, indent=2, sort_keys=False))

              req = service.objects().list_next(req, resp)
            except googleapiclient.errors.HttpError:
              logging.info("Failed to read the bucket %s", bucket["name"])
              logging.info(sys.exc_info())
              break

      request = service.buckets().list_next(
          previous_request=request, previous_response=response)

    return buckets_dict

  def get_filestore_instances(self) -> List[Dict[str, Any]]:
    """Retrieve a list of Filestore instances available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of Filestore instances in the project.
    """

    logging.info("Retrieving filestore instances")
    filestore_instances_list = list()
    service = discovery.build(
        "file", "v1", credentials=self.credentials, cache_discovery=False)
    try:
      request = service.projects().locations().instances().list(
          parent=f"projects/{self.project_id}/locations/-")
      while request is not None:
        response = request.execute()
        for instance in response.get("instances", []):
          filestore_instances_list.append(instance)

        request = service.projects().locations().instances().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to get filestore instances for project %s", self.project_id)
      logging.info(sys.exc_info())
    return filestore_instances_list

  def crawl(self):
    pass