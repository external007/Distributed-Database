import asyncio
import enum
import logging
import zmq
import zmq.asyncio
import typing
import heapq as hq
import time
import multiprocessing as mp
import threading as th
import concurrent.futures

## -----------------------------------------------------------------------------

## Opening a logging session
logger    = logging.getLogger("datastore")
handler   = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)-5s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

## -----------------------------------------------------------------------------

## Consistency models supported by Datastore.
class ConsistencyModel(enum.Enum):
    SEQUENTIAL = 1
    LINEARIZABLE = 2
    CAUSAL = 3
    EVENTUAL = 4

    def __str__(self):
        return self.name

def parse_consistency_model(s: str) -> ConsistencyModel:
    return {
        "sequential": ConsistencyModel.SEQUENTIAL,
        "linearized": ConsistencyModel.LINEARIZABLE,
        "causal": ConsistencyModel.CAUSAL,
        "eventual": ConsistencyModel.EVENTUAL,
    }[s]


## A class to configure DatastoreController.
class DcConfig:
    def __init__(self, host: str, port: str, cm: ConsistencyModel, id: int):
        self.host = host
        self.port = port
        self.cm   = cm
        self.id   = id

def default_dc_config() -> DcConfig:
    cfg = DcConfig("127.0.0.1", "6001", ConsistencyModel.CAUSAL, 1)
    return cfg


## Datastore Controoller adds a layer of abstraction on top of a Datastore that supports different
## consistency models per get/set operation
class DatastoreController:
    def __init__(self, cfg: DcConfig, dcs: typing.List[DcConfig]) -> None:
        self.cfg = cfg
        self.store = Datastore(self.cfg.id, Backend.MEMORY)
        self.clients = []
        for c in dcs:
            self.clients.append(ReplicaClient(self.cfg.id, c.host, c.port))

        ## Useful for implementing ordered multicasting
        self.cluster_size = 1 + len(dcs)

        ## Initializing the lamport_clock and the counter increment 
        ## is done inside consistency model (linearized).
        ## Note: I am using a simple counter to represent the
        ## lamport clock where each timestamp now will also have the
        ## process_id (replica_id) appended to it in addition to the
        ## counter value.
        ## Example - replica 1 -- 1.1, 2.1...10.1
        ## replica 2 -- 1.2, 2.2, 3.2...10.2
        self.lamport_clock = 1 + (self.cfg.id/10)
        self.lamport_clock_per_key = {}

        ## Data structures to store consistency model related messages
        ## and acknowledgements
        ## ----------------------------------

        ## A queue to store multicast messages
        self.sc_queue = []
        self.linear_queue = []
        self.causal_queue = []

        ## A map from a message's timestamp to the number of servers that have
        ## ackd it
        self.sc_acks = {}
        self.linear_acks = {}
        self.causal_acks = {}

        ## ----------------------------------
        ## Declarartions ends here


    async def start_server(self):
        self.ctx  = zmq.asyncio.Context.instance()
        self.sock = self.ctx.socket(zmq.ROUTER)
        connstr   = "tcp://{}:{}".format(self.cfg.host,self.cfg.port)
        self.sock.bind(connstr)
        logger.info("DatastoreController #{} listening at {}:{}".format(
            self.cfg.id, self.cfg.host, self.cfg.port
        ))
        pending_writes = []
        while True:
            msgs = await self.sock.recv_multipart(copy=True)
            client_id = msgs[0]
            msg_body = msgs[1]
            logger.info("handle_message #{}: Received message {}".format(self.cfg.id, msg_body))
            dct = self.parse_message(msg_body)
            logger.info("handle_message #{}: Parsed message {}".format(self.cfg.id, dct))
            logger.info("DataController #{}: Pending writes {}".format(self.cfg.id, pending_writes))
            ## Always process the message
            if self.cfg.cm == ConsistencyModel.EVENTUAL:
                rep = await self.handle_message(dct)
                self.sock.send_multipart([client_id, rep.encode("ASCII")])
            ## Only process the message if there are no pending writes from this client.
            else:
                while (client_id in pending_writes):
                    time.sleep(0.5)
                if dct["op"] == "SET":
                    pending_writes.append(client_id)
                    logger.info("DataController #{}: Added a pending write {}".format(self.cfg.id, pending_writes))
                rep = await self.handle_message(dct)
                if dct["op"] == "SET":
                    logger.info("DataController #{}: Removing a pending write {}".format(self.cfg.id, pending_writes))
                    pending_writes.remove(client_id)
                self.sock.send_multipart([client_id, rep.encode("ASCII")])


    def stop_server(self):
        logger.info("Stopped DatastoreController-{}...".format(self.cfg.id))
        self.sock.close()
        self.ctx.term()
        self.store.close()
        logger.info("Done")

    async def handle_message(self, dct) -> str:
        if dct["op"] == "GET":
            rep = await self.get(dct["key"])
            return rep
        elif dct["op"] == "SET":
            rep = await self.set_(dct["key"], dct["val"])
            return rep
        elif dct["op"] == "UPDATE":
            rep = await self.update(dct["key"], dct["val"], float(dct["tm"]))
            return rep
        elif dct["op"] == "ACK_UPDATE":
            rep = await self.ack_update(dct["key"], dct["val"], dct["tm"])
            return rep
        elif dct["op"] == "READ":
            rep = await self.linearized_read(dct["key"], float(dct["tm"]))
            return rep
        elif dct["op"] == "READ_ACK":
            rep = await self.linearized_read_ack(dct["key"], float(dct["tm"]))
            return rep
        else:
            return "{}: {}".format(dct["op"], dct["reason"])


    ## A message can either be "GET KEY CONSISTENCY_MODEL" or
    ## "SET KEY VALUE CONSISTENCY_MODEL" such that KEY and VALUE only have
    ## ASCII characters, and CONSISTENCY_MODEL can be one of the supported
    ## models.
    ##
    ## Messaged are parsed into a dictionary. It's rather rudamentary
    ## although, it is efficient.
    def parse_message(self, msg: bytes) -> typing.Dict[str, str]:
        try:
            msg_str = msg.decode("ascii")
            msg_parts = msg_str.split()
            if msg_parts[0] == "GET":
                if len(msg_parts) != 2:
                    return { "op" : "PARSE_ERROR",
                             "reason": "LENGTH IN GET" }
                else:
                    return { "op": "GET",
                             "key": msg_parts[1] }
            elif msg_parts[0] == "SET":
                if len(msg_parts) != 3:
                    return { "op" : "PARSE_ERROR",
                             "reason": "LENGTH IN SET" }
                else:
                    return { "op": "SET",
                             "key": msg_parts[1],
                             "val": msg_parts[2] }
            elif msg_parts[0] == "UPDATE":
                if len(msg_parts) != 4:
                    return { "op" : "PARSE_ERROR",
                             "reason": "LENGTH IN UPDATE" }
                else:
                    return { "op": "UPDATE",
                             "key": msg_parts[1],
                             "val": msg_parts[2],
                             "tm" : msg_parts[3] }
            elif msg_parts[0] == "ACK_UPDATE":
                if len(msg_parts) != 4:
                    return { "op" : "PARSE_ERROR",
                             "reason": "LENGTH IN ACK_UPDATE" }
                else:
                    return { "op": "ACK_UPDATE",
                             "key": msg_parts[1],
                             "val": msg_parts[2],
                             "tm" : msg_parts[3] }
            
            elif msg_parts[0] == "READ":
                if len(msg_parts) != 4:
                    return { "op" : "PARSE_ERROR",
                             "reason": "LENGTH IN READ" }
                else:
                    return { "op": "READ",
                             "key": msg_parts[1],
                             "val": msg_parts[2],
                             "tm" : msg_parts[3] }
            
            elif msg_parts[0] == "READ_ACK":
                if len(msg_parts) != 4:
                    return { "op" : "PARSE_ERROR",
                             "reason": "LENGTH IN READ_ACK" }
                else:
                    return { "op": "READ_ACK",
                             "key": msg_parts[1],
                             "val": msg_parts[2],
                             "tm" : msg_parts[3] }
            else:
                return { "op" : "PARSE_ERROR",
                         "reason": "UNKNOWN_OP" }
        except Exception:
            return { "op" : "PARSE_ERROR",
                     "reason" : "NOT_ASCII" }

    def get_timestamp(self):
        return time.monotonic_ns()

    async def get(self, key: str) -> str:
        logger.info("DatastoreController #{} get: {} cm is {}".format(self.cfg.id, key, self.cfg.cm))
        if self.cfg.cm is ConsistencyModel.SEQUENTIAL:
            rep = await self.sc_get(key)
            return rep
        elif self.cfg.cm is ConsistencyModel.LINEARIZABLE:
            response = await self.linearized_get(key)
            return "OK: {}".format(response)
        elif self.cfg.cm is ConsistencyModel.CAUSAL:
            rep = self.causal_get(key)
            return "OK: {}".format(rep)
        else:
            val = self.ec_get(key)
            return format(val)


    async def set_(self, key: str, val: str) -> str:
        logger.info("DatastoreController #{} set: {}={} cm: {}".format(self.cfg.id, key, val, self.cfg.cm))
        if self.cfg.cm in {ConsistencyModel.SEQUENTIAL, ConsistencyModel.LINEARIZABLE}:
            rep = await self.sc_set(key, val)
            return rep
        elif self.cfg.cm is ConsistencyModel.CAUSAL:
            rep = await self.causal_set(key, val)
            return "OK: {}".format(rep)
        else:
            self.ec_set(key, val)
            return "OK"

    async def update(self, key: str, val: str, tm: float) -> str:
        logger.info("DatastoreController #{} update: {}={} tm={}".format(self.cfg.id, key, val, tm))
        if self.cfg.cm in {ConsistencyModel.SEQUENTIAL, ConsistencyModel.LINEARIZABLE}:
            rep = await self.sc_update(key, val, tm)
            return rep
        elif self.cfg.cm is ConsistencyModel.CAUSAL:
            rep = await self.causal_update(key, val, tm)
            return "OK"
        else:
            self.ec_update(key, val, tm)
            return "OK"

    async def ack_update(self, key: str, val: str, tm: float) -> str:
        logger.info("DatastoreController #{} ack_update: {}={} tm={}".format(self.cfg.id, key, val, tm))
        if self.cfg.cm in {ConsistencyModel.SEQUENTIAL, ConsistencyModel.LINEARIZABLE}:
            rep = await self.sc_ack_update(key, val, tm)
            return rep
        elif self.cfg.cm is ConsistencyModel.CAUSAL:
            rep = await self.causal_ack_update(key, val, tm)
            return "OK"
        else:
            return "OK"

    async def read(self, key: str, tm: float) -> str:
        logger.info("DatastoreController #{} read: key={}, tm={}".format(self.cfg.id, key, tm))
        response = await self.linearized_read(key, tm)
        return "OK"

    async def read_ack(self, key: str, tm: float) -> str:
        logger.info("DatastoreController #{} read_ack: key={}, tm={}".format(self.cfg.id, key, tm))
        response = await self.linearized_read_ack(key, tm)
        return "OK"

## ----------------------------------
## Eventual consistency logic starts here

    def ec_get(self, key : str) -> str:
        return self.store.get(key)

    def ec_set(self, key : str, val : str) -> None:
        async def update(c, tm) -> None:
            await c.send_update(key, val, tm)

        if key not in self.lamport_clock_per_key:
            self.lamport_clock_per_key[key] = 0
        self.lamport_clock_per_key[key] += 1
        self.store.set_(key, val)
        for c in self.clients:
            asyncio.create_task(update(c, self.lamport_clock_per_key[key]))


    def ec_update(self, key : str, val : str, tm : int) -> None:
        if self.lamport_clock_per_key.get(key, 0) < tm:
            self.lamport_clock_per_key[key] = tm + 1
            self.store.set_(key, val)

## Eventual consistency logic starts here
## ----------------------------------


## Sequential consistency logic starts here
## ----------------------------------

    ## Local read, nothing to do.
    async def sc_get(self, key: str) -> str:
        return self.store.get(key)

    def poll_acks(self, key, val, tm):
        want_to_pop = QueueMessage(key, val, tm)
        want_to_pop_str = str(want_to_pop)
        while((self.sc_acks.get(want_to_pop_str, 0) < self.cluster_size) or (min(self.sc_queue) != want_to_pop)):
            time.sleep(0.5)
        logger.info("DatastoreController #{} processing message set {}={}".format(
            self.cfg.id, key, val
        ))
        head_msg = hq.heappop(self.sc_queue)
        self.store.set_(key, val)
        logger.info("DatastoreController #{} Performing write store={}".format(
            self.cfg.id, self.store.get_all()
        ))
        return "OK"

    ## Uses totally ordered multicast to perform a remote write.
    async def sc_set(self, key: str, val: str) -> str:
        # Increase the event counter in the clock
        self.lamport_clock += 1
        tm = self.lamport_clock

        ## (1) send message to self
        broadcast_0 = self.sc_update(key, val, tm)

        ## (2) send message to others
        msg = QueueMessage(key, val, tm)
        logger.info("DatastoreController #{} broadcasting update {}".format(
            self.cfg.id, msg
        ))
        broadcasts = [broadcast_0]
        for c in self.clients:
            broadcasts.append(c.send_update(key, val, tm))
        broadcasts_f = asyncio.gather(*broadcasts)
        broadcasts_done = await(broadcasts_f)
        return "OK"


    ## (4) Upon receiving a message...
    async def sc_update(self, key: str, val: str, tm: float) -> str:
        ## (1) Put it in the queue
        msg = QueueMessage(key, val, tm)
        hq.heappush(self.sc_queue, msg)

        ## (2) Broadcast ack
        broadcast_0 = self.sc_ack_update(key, val, tm)
        broadcasts = [broadcast_0]
        for c in self.clients:
            broadcasts.append(c.send_ack_update(key,val,tm))
        broadcasts_f = asyncio.gather(*broadcasts)
        broadcasts_done = await(broadcasts_f)

        ## (3) Wait until we receive acks from everyone else
        logger.info("DatastoreController #{} blocking now....".format(self.cfg.id))
        # loop = asyncio.get_event_loop()
        loop = asyncio.new_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor()
        val = loop.run_in_executor(executor, self.poll_acks, key, val, tm)
        return "OK"

    ## Acknowledge an update
    async def sc_ack_update(self, key: str, val: str, tm: float) -> None:
        msg = QueueMessage(key, val, tm)
        dkey = str(msg)
        dval = self.sc_acks.get(dkey, 0)
        ## Bump the ack for this message
        self.sc_acks[dkey] = dval + 1
        logger.info("DatastoreController #{} bumped ack {}={}".format(
            self.cfg.id, dkey, self.sc_acks[dkey]
        ))
        return "OK"

## ----------------------------------
## Sequential consistency ends here


## Linearized consistency starts here
## ----------------------------------

    def poll_read_acks(self, key, val, tm, dst):
        ## This is the message that we got when we start polling.
        want_to_pop = QueueMessage(key, val, tm)
        want_to_pop_str = str(want_to_pop)
        try:
            while((self.linear_acks.get(want_to_pop_str, 0) < self.cluster_size) or (min(self.linear_queue) != want_to_pop)):
                time.sleep(0.5)
            logger.info("DatastoreController #{} processing READ message set {}={}".format(
                self.cfg.id, key, val
            ))
            head_msg = hq.heappop(self.linear_queue)
            ## Here key and val should be equal to head_msg.key and head_msg.val
            logger.info("DatastoreController #{} Performing READ store={}".format(
                self.cfg.id, self.store.get_all()
            ))
            dst.append(self.store.get(key))
        except Exception as e:
            print("DUDE Boom:) .......")
        return "OK"


    ## Totally ordered multicast for reads.
    async def linearized_get(self, key: str) -> str:

        # (0) Adjust the lamport clock for this replica
        self.lamport_clock += 1
        tm = self.lamport_clock

        ## (1) send message to self
        broadcast_0 = self.linearized_read(key, tm)

        ## (2) Use NULL (not a write operation) as value and
        ## send message to others
        ## (2) send message to others
        msg = QueueMessage(key, "_", tm)
        logger.info("DatastoreController #{} broadcasting READ {}".format(
            self.cfg.id, msg
        ))
        broadcasts = [broadcast_0]
        for c in self.clients:
            broadcasts.append(c.send_read(key, "_", tm))
        broadcasts_f = asyncio.gather(*broadcasts)
        broadcasts_done = await(broadcasts_f)
        return "OK"

    ## (4) Upon receiving a message...
    async def linearized_read(self, key: str, tm: float) -> str:

        ## (0) Adjust the lamport timstamp
        ## max timestamp between the received message
        ## and this replica's timestamp
        ts = max(self.lamport_clock, tm)
        ## Increment the timestamp
        ts += 1

        ## (1) Put it in the queue (value is NULL because
        ## it's a read operation)
        msg = QueueMessage(key, "_", ts)
        hq.heappush(self.linear_queue, msg)

        ## (2) Broadcast ack (value is NULL because
        ## it's a read operation)
        broadcast_0 = self.linearized_read_ack(key, ts)
        broadcasts = [broadcast_0]
        for c in self.clients:
            broadcasts.append(c.send_read_ack(key, "_", ts))
        broadcasts_f = asyncio.gather(*broadcasts)
        broadcasts_done = await(broadcasts_f)
        
        ## (3) Wait until we receive acks from everyone else
        logger.info("DatastoreController #{} blocking now on READ....".format(self.cfg.id))
        loop = asyncio.new_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor()
        dst=[]
        loop.run_in_executor(executor, self.poll_read_acks, key, "_", tm, dst)
        return str(dst)


    ## Acknowledge an update
    async def linearized_read_ack(self, key: str, tm: float) -> str:
        msg = QueueMessage(key, "_", tm)
        dkey = str(msg)
        dval = self.linear_acks.get(dkey, 0)
        ## Bump the ack for this message
        self.linear_acks[dkey] = dval + 1
        logger.info("DatastoreController #{} bumped READ_ACK {}={}".format(
            self.cfg.id, dkey, self.linear_acks[dkey]
        ))
        return "OK"

## ----------------------------------
## Linearized consistency ends here


## ----------------------------------
## Causal consistency begins here
    
    def causal_get(self, key: str) -> str:
        # Building vector clock, increment by 1 for
        # read operation
        self.lamport_clock += 1
        tm = self.lamport_clock

        val = self.store.get(key)
        logger.info("DatastoreController: Event %d --> 'R(%s)%s" % (tm, key, val))
        return val

    async def causal_set(self, key: str, val: str) -> str:
        self.lamport_clock += 1
        tm = self.lamport_clock

        ## (1) send message to self
        broadcast_0 = self.causal_update(key, val, tm)

        ## (2) send message to others
        msg = QueueMessage(key, val, int(tm))
        logger.info("DatastoreController #{} broadcasting update {}".format(
            self.cfg.id, msg
        ))
        broadcasts = [broadcast_0]
        for c in self.clients:
            broadcasts.append(c.send_update(key, val, tm))
        broadcasts_f = asyncio.gather(*broadcasts)
        broadcasts_done = await(broadcasts_f)
        return "OK"

    ## (3) Monitor Acknowledgements
    def cc_poll_acks(self, key, val):
        head_msg = hq.heappop(self.sc_queue)
        head_msg_dkey = str(head_msg)
        while(self.causal_acks[head_msg_dkey] < self.cluster_size):
            time.sleep(0.5)
        logger.info("DatastoreController #{} processing message set {}={}".format(
            self.cfg.id, key, val
        ))
        self.store.set_(key, val)
        logger.info("DatastoreController #{} Performing write store={}".format(
            self.cfg.id, self.store.get_all()
        ))
        return "OK"

    ## (4) Upon receiving a message
    async def causal_update(self, key: str, val: str, tm: float) -> str:
        # Server updates client with current timestamp
        self.lamport_clock = max(self.lamport_clock, tm) + 1

        # Timestamp was increase when update sent to replicas (i.e. Primary -> Secondary)
        ts = self.lamport_clock

        ## (1) Put it in the queue
        msg = QueueMessage(key, val, tm)
        hq.heappush(self.causal_queue, msg)

        ## (2) Broadcast ack
        broadcast_0 = self.causal_ack_update(key, val, tm)
        broadcasts = [broadcast_0]
        for c in self.clients:
            broadcasts.append(c.send_ack_update(key, val, int(tm)))
        broadcasts_f = asyncio.gather(*broadcasts)
        broadcasts_done = await(broadcasts_f)

        ## (3) Wait until we receive acks from everyone else
        logger.info("DatastoreController #{} blocking now....".format(self.cfg.id))
        loop = asyncio.new_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor()
        loop.run_in_executor(executor, self.cc_poll_acks, key, val)
        return "OK"

    ## Acknowledge an update
    async def causal_ack_update(self, key: str, val: str, tm: float) -> None:
        msg = QueueMessage(key, val, tm)
        dkey = str(msg)
        dval = self.causal_acks.get(dkey, 0)
        ## Bump the ack for this message
        self.causal_acks[dkey] = dval + 1
        logger.info("DatastoreController #{} bumped ack {}={}".format(
            self.cfg.id, dkey, self.causal_acks[dkey]
        ))

    ## ----------------------------------
    ## Causal consistency ends here


## -----------------------------------------------------------------------------

## Various Datastore backends.
class Backend(enum.Enum):
    MEMORY = 1
    REDIS = 2

## A low-level store that stores the key-value pairs; it doesn't know
## anything about consistency models. It can store things in memory,
## or use Redis.
class Datastore:
    def __init__(self, id: int, backend: Backend):
        self.id = id
        self.backend = backend
        if self.backend == Backend.MEMORY:
            self.store = {}
        else:
            raise NotImplementedError(self.backend)

    def close(self):
        if self.backend == Backend.MEMORY:
            return
        else:
            raise NotImplementedError(self.backend)

    def get(self, key: str) -> str:
        logger.info("Datastore #{} get: {} in {}".format(self.id, key, self.backend))
        if self.backend == Backend.MEMORY:
            return self.get_in_memory(key)
        else:
            raise NotImplementedError(self.backend)

    def get_all(self):
        if self.backend == Backend.MEMORY:
            return self.store
        else:
            raise NotImplementedError(self.backend)

    def set_(self, key: str, val: str) -> None:
        logger.info("Datastore #{} set: {}={} in {}".format(self.id, key, val, self.backend))
        if self.backend == Backend.MEMORY:
            self.set_in_memory(key, val)
        else:
            raise NotImplementedError(self.backend)

    def get_in_memory(self, key: str) -> str:
        return self.store.get(key, "__NOTFOUND__")

    def set_in_memory(self, key: str, val: str) -> None:
        self.store[key] = val

## -----------------------------------------------------------------------------

## The end-user client class for the datastore controller.
class Client:
    def __init__(self, host: str, port: str):
        self.server_host = host
        self.server_port = port
        self.ctx  = zmq.Context()
        self.sock = self.ctx.socket(zmq.DEALER)
        connstr   = "tcp://{}:{}".format(self.server_host,self.server_port)
        self.sock.connect(connstr)

    def get(self, key: str) -> str:
        msg = "GET {}".format(key)
        self.sock.send(msg.encode("ASCII"))
        # print("waiting for a reply...\n")
        rep = self.sock.recv(copy=True)
        return rep.decode("ASCII")

    def set_(self, key: str, val: str) -> str:
        msg = "SET {} {}".format(key, val)
        self.sock.send(msg.encode("ASCII"))
        rep = self.sock.recv(copy=True)
        return rep.decode("ASCII")

    def close_conn(self):
        self.sock.close()
        self.ctx.term()


## The client class for a replica to communicate with others
class ReplicaClient(Client):
    def __init__(self, id: int, host: str, port: str):
        self.id = id
        super().__init__(host, port)

    async def send_update(self, key: str, val: str, tm : int) -> str:
        logger.info("ReplicaClient #{} sending update to: {}:{}".format(
            self.id, self.server_host, self.server_port
        ))
        msg = "UPDATE {} {} {}".format(key, val, tm)
        self.sock.send(msg.encode("ASCII"))
        logger.info("ReplicaClient #{} sending update to: {}:{} done.".format(
            self.id, self.server_host, self.server_port
        ))
        logger.info("ReplicaClient #{} Waiting for reply to an update from {}:{}".format(
            self.id, self.server_host, self.server_port
        ))
        # rep = self.sock.recv(copy=True)
        # logger.info("ReplicaClient #{} {}:{} returned {} as a reply to update".format(
        #     self.id, self.server_host, self.server_port, rep
        # ))
        # return rep.decode("ASCII")
        return "OK"

    async def send_ack_update(self, key: str, val: str, tm : int) -> str:
        logger.info("ReplicaClient #{} sending ack to: {}:{}".format(
            self.id, self.server_host, self.server_port
        ))
        msg = "ACK_UPDATE {} {} {}".format(key, val, tm)
        self.sock.send(msg.encode("ASCII"))
        logger.info("ReplicaClient #{} sending ack to: {}:{} done.".format(
            self.id, self.server_host, self.server_port
        ))
        # logger.info("ReplicaClient #{} Waiting for reply to an ack_update from {}:{}".format(
        #     self.id, self.server_host, self.server_port
        # ))
        # rep = self.sock.recv(copy=True)
        # logger.info("ReplicaClient #{} {}:{} returned {} as a reply to ack_update".format(
        #     self.id, self.server_host, self.server_port, rep
        # ))
        # return rep.decode("ASCII")
        return "OK"

    async def send_read(self, key: str, val: str, tm: int) -> str:
        logger.info("ReplicaClient #{} sending READ to: {}:{}".format(
            self.id, self.server_host, self.server_port
        ))
        msg = "READ {} {} {}".format(key, val, tm)
        self.sock.send(msg.encode("ASCII"))
        logger.info("ReplicaClient #{} sending READ_UPDATE to: {}:{} done.".format(
            self.id, self.server_host, self.server_port
        ))
        logger.info("ReplicaClient #{} Waiting for reply to an READ_UPDATE from {}:{}".format(
            self.id, self.server_host, self.server_port
        ))
        # rep = self.sock.recv(copy=True)
        # logger.info("ReplicaClient #{} {}:{} returned {} as a reply to update".format(
        #     self.id, self.server_host, self.server_port, rep
        # ))
        # return rep.decode("ASCII")
        return "OK"

    async def send_read_ack(self, key: str, val: str, tm: int) -> str:
        logger.info("ReplicaClient #{} sending READ_ACK to: {}:{}".format(
            self.id, self.server_host, self.server_port
        ))
        msg = "READ_ACK {} {} {}".format(key, val, tm)
        self.sock.send(msg.encode("ASCII"))
        logger.info("ReplicaClient #{} sending READ_ACK to: {}:{} done.".format(
            self.id, self.server_host, self.server_port
        ))
        # logger.info("ReplicaClient #{} Waiting for reply to an ack_update from {}:{}".format(
        #     self.id, self.server_host, self.server_port
        # ))
        # rep = self.sock.recv(copy=True)
        # logger.info("ReplicaClient #{} {}:{} returned {} as a reply to ack_update".format(
        #     self.id, self.server_host, self.server_port, rep
        # ))
        # return rep.decode("ASCII")
        return "OK"

## -----------------------------------------------------------------------------
## Some more code for sequential consistency. This class is required because
## objects inserted in a heap have to have an ordering relation defined on them.

## Renaming the class as QueueMessage since can be used by multiple models
## So, I am being lazy and saving myself from typing QueueMessage :)
class QueueMessage:
    def __init__(self, key: str, val: str, tm: int):
        self.key = key
        self.val = val
        self.tm  = tm

    def __str__(self):
        return "{}-{}-{}".format(self.key,self.val,self.tm)

    def __lt__(self, other):
        return self.tm < other.tm

    def __le__(self, other):
        return self.tm <= other.tm

    def __gt__(self, other):
        return self.tm > other.tm

    def __ge__(self, other):
        return self.tm >= other.tm

    def __eq__(self, other):
        return self.tm == other.tm

    def __ne__(self, other):
        return self.tm != other.tm
