from httplib2 import Credentials
from typing import Dict,Any,List,Tuple
from googleapiclient import discovery
import logging
import sys

class ProjectManager:
  def __init__(self,project_name: str, credentials: Credentials):
    self.project_name = project_name
    self.credentials = credentials

  def fetch_project_info(self) -> Dict[str, Any]:
    """Retrieve information about specific project.

    Args:
      project_name: Name of project to request info about
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      Project info object or None.
    """
    project_info = None
    logging.info("Retrieving info about: %s", self.project_name)

    try:
      service = discovery.build(
          "cloudresourcemanager",
          "v1",
          credentials=self.credentials,
          cache_discovery=False)
      request = service.projects().get(projectId=self.project_name)
      response = request.execute()
      if "projectNumber" in response:
        project_info = response

    except Exception:
      logging.info("Failed to enumerate projects")
      logging.info(sys.exc_info())

    return project_info


  def get_project_list(self) -> List[Dict[str, Any]]:
    """Retrieve a list of projects accessible by credentials provided.

    Args:
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of Project objects from cloudresourcemanager RestAPI.
    """

    logging.info("Retrieving projects list")
    project_list = list()
    try:
      service = discovery.build(
          "cloudresourcemanager",
          "v1",
          credentials=self.credentials,
          cache_discovery=False)
      request = service.projects().list()
      while request is not None:
        response = request.execute()

        for project in response.get("projects", []):
          project_list.append(project)

        request = service.projects().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to enumerate projects")
      logging.info(sys.exc_info())
    return project_list

  def get_iam_policy(self) -> List[Dict[str, Any]]:
    """Retrieve an IAM Policy in the project.

    Args:
      project_name: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      An IAM policy enforced for the project.
    """

    logging.info("Retrieving IAM policy for %s", self.project_name)
    service = discovery.build(
        "cloudresourcemanager",
        "v1",
        credentials=self.credentials,
        cache_discovery=False)

    resource = self.project_name

    get_policy_options = {
        "requestedPolicyVersion": 3,
    }
    get_policy_options = {"options": {"requestedPolicyVersion": 3}}
    try:
      request = service.projects().getIamPolicy(
          resource=resource, body=get_policy_options)
      response = request.execute()
    except Exception:
      logging.info("Failed to get endpoints list for project %s", self.project_name)
      logging.info(sys.exc_info())
      return None

    if response.get("bindings", None) is not None:
      return response["bindings"]
    else:
      return None

  def get_associated_service_accounts(self,
      iam_policy: List[Dict[str, Any]]) -> List[str]:
    """Extract a list of unique SAs from IAM policy associated with project.

    Args:
      iam_policy: An IAM policy provided by get_iam_policy function.

    Returns:
      A list of service accounts represented as string
    """

    if not iam_policy:
      return []

    list_of_sas = list()
    for entry in iam_policy:
      for member in entry["members"]:
        if "deleted:" in member:
          continue
        account_name = None
        for element in member.split(":"):
          if "@" in element:
            account_name = element
            break
        if account_name and account_name not in list_of_sas:
          list_of_sas.append(account_name)

    return list_of_sas

  def get_service_accounts(self) -> List[Tuple[str, str]]:
    """Retrieve a list of service accounts managed in the project.

    Args:
      project_name: A name of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of service accounts managed in the project.
    """

    logging.info("Retrieving SA list %s", self.project_name)
    service_accounts = []
    service = discovery.build(
        "iam", "v1", credentials=self.credentials, cache_discovery=False)

    name = f"projects/{self.project_name}"

    try:
      request = service.projects().serviceAccounts().list(name=name)
      while request is not None:
        response = request.execute()

        for service_account in response.get("accounts", []):
          service_accounts.append(
              (service_account["email"], service_account["description"]
              if "description" in service_account else ""))

        request = service.projects().serviceAccounts().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve SA list for project %s", self.project_name)
      logging.info(sys.exc_info())

    return service_accounts


  def list_services(self) -> List[Any]:
    """Retrieve a list of services enabled in the project.

    Args:
      project_id: An id of a project to query info about.
      credentials: An google.oauth2.credentials.Credentials object.

    Returns:
      A list of service API objects enabled in the project.
    """

    logging.info("Retrieving services list %s", self.project_name)
    list_of_services = list()
    serviceusage = discovery.build("serviceusage", "v1", credentials=self.credentials)

    request = serviceusage.services().list(
        parent="projects/" + self.project_name, pageSize=200, filter="state:ENABLED")
    try:
      while request is not None:
        response = request.execute()
        list_of_services.append(response.get("services", None))

        request = serviceusage.services().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to retrieve services for project %s", self.project_name)
      logging.info(sys.exc_info())

    return list_of_services