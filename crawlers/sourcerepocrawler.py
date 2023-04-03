from typing import List,Any
from googleapiclient import discovery
from httplib2 import Credentials
import logging
import sys

class SourceRepoManager:
  def __init__(self,project_name: str, credentials: Credentials):
    self.project_name = project_name
    self.credentials = credentials
  
  async def list_sourcerepo(self) -> List[Any]:
    """Retrieve a list of cloud source repositories enabled in the project.

    Args:
      project_id: An id of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of cloud source repositories in the project.
    """

    logging.info("Retrieving cloud source repositories %s", self.project_name)
    list_of_repos = list()
    service = discovery.build("sourcerepo", "v1", credentials=self.credentials)

    request = service.projects().repos().list(
      name="projects/" + self.project_name,
      pageSize=500
    )
    try:
      while request is not None:
        response = request.execute()
        list_of_repos.append(response.get("repos", None))

        request = service.projects().repos().list_next(
          previous_request=request,
          previous_response=response
        )
    except Exception:
      logging.info("Failed to retrieve source repos for project %s", self.project_name)
      logging.info(sys.exc_info())

    return list_of_repos
  
  def crawl(self):
    pass