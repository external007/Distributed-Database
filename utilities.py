import multiprocessing as mp
import logging
import asyncio
import sys
import zmq
import time
import random
import argparse

from datastore import *

## Set the logger configs
logger = logging.getLogger("utilities")
handler   = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)-5s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

## Utility code starts here
## -----------------------------------------------------------------------------

class Utilities:

    def __init__(self, num_replicas, consistency_model):
        self.num_replicas = num_replicas
        self.consistency_model = consistency_model

        ## Allow upto 10 replicas for reasonable performance
        self.ValidateConfigs()

        ## Get the ports for starting the replicas
        logger.info("Requesting ports")
        self.ports = self.GetPorts()

    # validate the user supplied parameters
    def ValidateConfigs(self):
        flag1 = False
        flag2 = False
        if self.num_replicas > 10:
            logger.info("Invalid number of replicas requested")
            logger.info("Current configuration allows upto 10 replica/data-store.")
            logger.info("Please retry with a lower number of replicas")
            flag1= True

        if self.consistency_model not in ["sequential", "linearized", "causal", "eventual"]:
            logger.info("Invalid consistency model requested")
            logger.info("Valid options are")
            logger.info("1. sequential")
            logger.info("2. linearized")
            logger.info("3. causal")
            logger.info("4. eventual")
            flag2 = True

        if flag1 or flag2:
            exit(1)

        return True

    # Get a list po ports to start the server replicas
    def GetPorts(self):
        ## ports lower than 1024 are
        ## reserved by the system so we have to
        ## start after 1024, to be safe I am
        ## starting from 1050
        lower_port = 1050
        max_num_ports = lower_port + self.num_replicas

        ## get random port numbers
        ports = random.sample(range(lower_port, max_num_ports), k = self.num_replicas)
        return ports

    def GetPrimary(self):
        return random.sample(self.ports, k = 1)


## Server replicas code starts here
## ------------------------------------------------------------------------------------------
class SpawnReplicas():

    def __init__(self, num_replicas, ports, consistency_model):
        self.num_replicas = num_replicas
        self.ports = ports
        self.consistency_model = consistency_model

        logger.info("Creating the replicas/data-store")
        self.controllers = self.CreateReplicas()

        logger.info("Starting the replicas")
        self.StartReplicas()

    # Start the server replicas
    def run_server(self, cfg, dcs):
        try:
            dc = DatastoreController(cfg, dcs)
            loop = asyncio.get_event_loop()
            loop.create_task(dc.start_server())
            loop.run_forever()
        except (Exception, KeyboardInterrupt):
            dc.stop_server()
            loop.stop()
            return

    # Create the replicas and overwrite default configuration
    def CreateReplicas(self):

        ## Create the object for datacontroller config
        controller_object_list = [default_dc_config() for index in range(self.num_replicas)]
        ## Set the requested consistency model
        ## Indexing the list from 1 to make it easier to
        ## understand the logger messages. So replicas will
        ## be numbered starting from 1
        for index in range(1, len(controller_object_list)+1):

            datacontroller = controller_object_list[index-1]

            datacontroller.cm = parse_consistency_model(self.consistency_model)

            ## Set the id for the replica
            datacontroller.id = index

            ## Set the port number
            ## Reduce the index by 1 because the port list
            # is indexed from 0 (not 1), so allocate
            # 1st port to 1st replica, 2nd port to 2nd replica
            # and so on.
            datacontroller.port = self.ports[index-1]

            ## Put the configured controller object back in the list

            ## return the list of configured objects
            #controller_object_list[index-1] = datacontroller
        return controller_object_list


    # Start the replicas in seperate processes
    def StartReplicas(self):
        ## Start the replicas
        for index in range(self.num_replicas):
            other_replicas = [self.controllers[i] for i in range(self.num_replicas) if i != index]
            process = mp.Process(target=self.run_server, args=(self.controllers[index], other_replicas,))
            process.start()


## Client code starts here
## -----------------------------------------------------------------------------

class SpawnClient():

    def __init__(self, consistency_type, ports):
        self.consistency_model = consistency_type
        self.ports = ports

        logger.info("Validating the requested resource")
        self.ValidateConsistency()
        self.StartClients()

    def ValidateConsistency(self):
        if self.consistency_model not in ["sequential", "linearized", "causal", "eventual"]:
            logger.info("Invalid consistency model requested")
            logger.info("Valid options are")
            logger.info("1. sequential")
            logger.info("2. linearized")
            logger.info("3. causal")
            logger.info("4. eventual")
            ###
            ### Change it to
            exit(1)
            ####
        return True

    def StartClients(self):
        logger.info("Sending client requests")
        if self.consistency_model == "sequential":
            self.sequential_tests()
        elif self.consistency_model == "linearized":
            self.linearized_tests()    
        

    def sequential_tests(self):
        cfg = default_dc_config()
        ## randomly select a replica to process the client request
        cfg.port =  random.choice(self.ports)
    
        ## Test case 1
        client1 = Client(cfg.host, cfg.port)
        # Write operation
        rep = client1.set_("hello", "world")
        # Write operation
        rep = client1.set_("my", "world")
        # Write operation
        rep = client1.set_("hello", "new_world")
        # Get operation
        rep = client1.get("hello")
        print("Server response: ", rep)

        ## Test case 2
        client2 = Client(cfg.host, cfg.port)
        # Write operation
        rep = client1.set_("corona", "pandemic")
        # Read operation
        rep = client1.get("corona")
        print("Server response: ", rep)
        
        # Write operation
        rep = client2.set_("status", "all_good!")
        rep = client2.set_("country", "USA")
        # Read operation
        rep = client2.get("country")
        print("Server response: ", rep)

        client1.close_conn()
        client2.close_conn()


    def linearized_tests(self):
        cfg = default_dc_config()
        ## randomly select a replica to process the client request
        cfg.port =  random.choice(self.ports)

        client1 = Client(cfg.host, cfg.port)
        rep = client1.set_("hello", "world")
        rep = client1.set_("my", "world")
        rep = client1.set_("hello", "new_world")
        #rep = client1.get("hello")
        
        client2 = Client(cfg.host, cfg.port)
        rep = client2.get("hello")
        print("Server response is:", rep)
        # # Write operation

    
        ## Test case 1
        # client1 = Client(cfg.host, cfg.port)
        # # Write operation
        # rep = client1.set_("hello", "world")
        # # Write operation
        # rep = client1.set_("my", "world")
        # # Write operation
        # rep = client1.set_("hello", "new_world")
        # # Get operation
        # rep = client1.get("hello")
        # print("Server response: ", rep)
        # # Get operation
        # rep = client1.get("my")
        # print("Server response: ", rep)


        # ## Test case 2
        # client2 = Client(cfg.host, cfg.port)
        # # Write operation
        # rep = client1.set_("corona", "pandemic")
        # # Read operation
        # rep = client1.get("corona")
        # print("Server response: ", rep)
        
        # # Write operation
        # rep = client2.set_("status", "all_good!")
        # rep = client2.set_("country", "USA")
        # # Read operation
        # rep = client2.get("country")
        # print("Server response: ", rep)

        #client1.close_conn()
        #client2.close_conn()




# Tests for eventual consistemcy are a seperate class.
## -----------------------------------------------------------------------------

class TestsForEC:
    def __init__(self, ports):
        self.ports = ports

    def test(self):
        logger.info("Sending client requests")
        cfg = default_dc_config()

        ## randomly select a replica to process the client request
        cfg.port =  random.choice(self.ports)
        client = Client(cfg.host, cfg.port)
        rep = client.set_("hello", "world")
        rep = client.get("hello")
        assert rep == "world"

        async def subtest1():
            rep = client.set_("key1", "c311")
            rep = client.set_("key1", "b521")
            await asyncio.sleep(0.1)
            rep = client.get("key1")
            assert rep == "b521"

        asyncio.run(subtest1())

        async def subtest2():
            rep = {}
            async def set1():
                client.set_("key2", "c311")

            async def get():
                rep['return'] = client.get("key2")
                
            async def set2():
                await asyncio.sleep(0.5)
                client.set_("key2", "b521")
            
            await asyncio.gather(set1(), get(), set2())
            assert rep['return'] == "c311"

        asyncio.run(subtest2())
        client.close_conn()
