from typing import List,Dict,Any
from httplib2 import Credentials
from googleapiclient import discovery
import logging
import sys

class MQManager:
  def __init__(self,project_name:str,credentials:Credentials):
    self.project_name = project_name
    self.credentials = credentials
    
  async def get_pubsub_subscriptions(self) -> List[Dict[str, Any]]:
    """Retrieve a list of PubSub subscriptions available in the project.

    Args:
      project_id: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of PubSub subscriptions in the project.
    """

    logging.info("Retrieving PubSub Subscriptions")
    pubsubs_list = list()
    try:
      service = discovery.build(
          "pubsub", "v1", credentials=self.credentials, cache_discovery=False)

      request = service.projects().subscriptions().list(
          project=f"projects/{self.project_name}")
      while request is not None:
        response = request.execute()
        for subscription in response.get("subscriptions", []):
          pubsubs_list.append(subscription)

        request = service.projects().subscriptions().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to get PubSubs for project %s", self.project_name)
      logging.info(sys.exc_info())
    return pubsubs_list

  def crawl(self):
    pass