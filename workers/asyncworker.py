import asyncio
from ..crawlers import (ComputeManager,
                        DBManager,
                        GKEManager,
                        MQManager,
                        NetworkManager,
                        ServerlessManager,
                        SourceRepoManager,
                        StorageManager)
class Worker:
    def __init__(self,scan_config,project_name, credentials):
        self.scan_config = scan_config
        self.project_name = project_name
        self.credentails = credentials
        self.crawler_list = []

    def is_set(self,config, config_setting):
        if config is None:
            return True
        obj = config.get(config_setting, {})
        return obj.get('fetch', False)

    def spawn_crawlers(self):
        if self.is_set(self.scan_config, 'compute_instances'):
            self.crawler_list.append(ComputeManager(self.project_name,self.credentails))
            
        if self.is_set(self.scan_config, 'db_instances'):
            self.crawler_list.append(DBManager(self.project_name,self.credentails))

        if self.is_set(self.scan_config, 'gke_instances'):
            self.crawler_list.append(GKEManager(self.project_name,self.credentails))
        
        if self.is_set(self.scan_config, 'mq_instances'):
            self.crawler_list.append(MQManager(self.project_name,self.credentails))

        if self.is_set(self.scan_config, 'network_instances'):
            self.crawler_list.append(NetworkManager(self.project_name,self.credentails))
        
        if self.is_set(self.scan_config, 'serverless_instances'):
            self.crawler_list.append(ServerlessManager(self.project_name,self.credentails))

        if self.is_set(self.scan_config, 'sourcerepo_instances'):
            self.crawler_list.append(SourceRepoManager(self.project_name,self.credentails))

        if self.is_set(self.scan_config, 'storage_instances'):
            self.crawler_list.append(StorageManager(self.project_name,self.credentails))
        
        return self.crawler_list

    async def work(self):
        self.crawler_list = self.spawn_crawlers()
        results = []
        for crawler in self.crawlers_list:
            result = await crawler.crawl()
            results.append(result)
        return results
    
    async def run(self):
        loop = asyncio.get_event_loop()
        results = await loop.run_until_complete(self.work())
        return results