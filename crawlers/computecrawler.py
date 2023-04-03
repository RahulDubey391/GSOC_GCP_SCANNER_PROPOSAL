import logging
import sys
from typing import Dict, Any, List
from httplib2 import Credentials
from googleapiclient import discovery

class ComputeManager:
  def __init__(self,project_name: str, credentials:Credentials):
    self.project_name = project_name
    self.credentials = credentials
    self.service = discovery.build('compute', 'v1', credentials=self.credentials, cache_discovery=False)

  async def get_compute_instances_names(self) -> List[Dict[str, Any]]:
    """Retrieve a list of Compute VMs available in the project.

    Args:
      project_name: A name of a project to query info about.
      service: A resource object for interacting with the Compute API.

    Returns:
      A list of instance objects.
    """

    logging.info("Retrieving list of Compute Instances")
    images_result = list()
    try:
      request = self.service.instances().aggregatedList(project=self.project_name)
      while request is not None:
        response = request.execute()
        if response.get("items", None) is not None:
          for _, instances_scoped_list in response["items"].items():
            for instance in instances_scoped_list.get("instances", []):
              images_result.append(instance)

        request = self.service.instances().aggregatedList_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to enumerate compute instances in the %s",
                  self.project_name)
      logging.info(sys.exc_info())
    return images_result


  async def get_compute_images_names(self) -> List[Dict[str, Any]]:
    """Retrieve a list of Compute images available in the project.

    Args:
      project_name: A name of a project to query info about.
      service: A resource object for interacting with the Compute API.

    Returns:
      A list of image objects.
    """

    logging.info("Retrieving list of Compute Image names")
    images_result = list()
    try:
      request = self.service.images().list(project=self.project_name)
      while request is not None:
        response = request.execute()
        for image in response.get("items", []):
          images_result.append(image)

        request = self.service.images().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to enumerate compute images in the %s", self.project_name)
      logging.info(sys.exc_info())
    return images_result


  async def get_compute_disks_names(self) -> List[Dict[str, Any]]:
    """Retrieve a list of Compute disks available in the project.

    Args:
      project_name: A name of a project to query info about.
      service: A resource object for interacting with the Compute API.

    Returns:
      A list of disk objects.
    """

    logging.info("Retrieving list of Compute Disk names")
    disk_names_list = list()
    try:
      request = self.service.disks().aggregatedList(project=self.project_name)
      while request is not None:
        response = request.execute()
        if response.get("items", None) is not None:
          for _, disks_scoped_list in response["items"].items():
            for disk in disks_scoped_list.get("disks", []):
              disk_names_list.append(disk)

        request = self.service.disks().aggregatedList_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to enumerate compute disks in the %s", self.project_name)
      logging.info(sys.exc_info())

    return disk_names_list


  async def get_static_ips(self) -> List[Dict[str, Any]]:
    """Retrieve a list of static IPs available in the project.

    Args:
      project_name: A name of a project to query info about.
      service: A resource object for interacting with the Compute API.

    Returns:
      A list of static IPs in the project.
    """

    logging.info("Retrieving Static IPs")

    ips_list = list()
    try:
      request = self.service.addresses().aggregatedList(project=self.project_name)
      while request is not None:
        response = request.execute()
        for name, addresses_scoped_list in response["items"].items():
          if addresses_scoped_list.get("addresses", None) is None:
            continue
          ips_list.append({name: addresses_scoped_list})

        request = self.service.addresses().aggregatedList_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to get static IPs in the %s", self.project_name)
      logging.info(sys.exc_info())

    return ips_list


  async def get_compute_snapshots(self) -> List[Dict[str, Any]]:
    """Retrieve a list of Compute snapshots available in the project.

    Args:
      project_name: A name of a project to query info about.
      service: A resource object for interacting with the Compute API.

    Returns:
      A list of snapshot objects.
    """

    logging.info("Retrieving Compute Snapshots")
    snapshots_list = list()
    try:
      request = self.service.snapshots().list(project=self.project_name)
      while request is not None:
        response = request.execute()
        for snapshot in response.get("items", []):
          snapshots_list.append(snapshot)

        request = self.service.snapshots().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to get compute snapshots in the %s", self.project_name)
      logging.info(sys.exc_info())

    return snapshots_list


  async def get_subnets(self) -> List[Dict[str, Any]]:
    """Retrieve a list of subnets available in the project.

    Args:
      project_name: A name of a project to query info about.
      compute_client: A resource object for interacting with the Compute API.

    Returns:
      A list of subnets in the project.
    """

    logging.info("Retrieving Subnets")
    subnets_list = list()
    try:
      request = self.service.subnetworks().aggregatedList(project=self.project_name)
      while request is not None:
        response = request.execute()
        if response.get("items", None) is not None:
          for name, subnetworks_scoped_list in response["items"].items():
            subnets_list.append((name, subnetworks_scoped_list))

        request = self.subnetworks().aggregatedList_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to get subnets in the %s", self.project_name)
      logging.info(sys.exc_info())

    return subnets_list


  async def get_firewall_rules(self) -> List[Dict[str, Any]]:
    """Retrieve a list of firewall rules in the project.

    Args:
      project_name: A name of a project to query info about.
      compute_client: A resource object for interacting with the Compute API.

    Returns:
      A list of firewall rules in the project.
    """

    logging.info("Retrieving Firewall Rules")
    firewall_rules_list = list()
    try:
      request = self.service.firewalls().list(project=self.project_name)
      while request is not None:
        response = request.execute()
        for firewall in response.get("items", []):
          firewall_rules_list.append((
              firewall["name"],
              # firewall['description'],
          ))

        request = self.service.firewalls().list_next(
            previous_request=request, previous_response=response)
    except Exception:
      logging.info("Failed to get firewall rules in the %s", self.project_name)
      logging.info(sys.exc_info())
    return firewall_rules_list
  
  def crawl(self):
    pass
