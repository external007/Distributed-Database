#!/usr/bin/env python3.7

from utilities import *

## -----------------------------------------------------------------------------
## Default test objects
keys = ["x", "y", "z"]
values = ["a", "b", "c"]

# Can extend ids to as many as we need
# This can be scaled further by dedicating server resources
ids = [6001, 6002, 6003]
events = []

random.seed(42)


class RandomEvent:
    """
    An object preloaded with a random id, set, and get function
    """

    def __init__(self, id, set_obj: (str, str), get_obj: str):
        self.id = id
        self.set_obj = set_obj
        self.get_obj = get_obj

    def make_set(self, key=random.choice(keys), val=random.choice(values)):
        """
        :param key: Calls random key by default with replacement
        :param val: Calls random val by default with replacement
        """
        self.set_obj = (key, val)

    def make_get(self, key=random.choice(values)):
        """
        :param key: Calls random key by default with replacement
        """
        self.get_obj = key


def run_event(RandomEvent, action):
    """
    Run consistency model tests here from RandomEvent with either random or chosen parameters
    i.e. RandomEvent.make_set("hello", "world")
    :param RandomEvent: Defaulted object which user can choose an action either make_set or make_get
    :param action: Either "SET" or "GET" to populate array events
    :return: String that represents process
    """
    ## INITIALIZING CONFIGS

    # TODO Either use old simple config setup or Utilities
    cfg = default_dc_config()
    cfg.port = 6002
    client = Client(cfg.host, str(cfg.port))


    e = []
    if action == "SET":

        # Need actual server set call from here
        set_obj = RandomEvent.set_obj
        e.append("W(%s)%s" % (set_obj[0], set_obj[1]))
        rep = client.set_(set_obj[0], set_obj[1])
        client.close_conn()

    elif action == "GET":

        # Need actual server get call from here
        get_obj = RandomEvent.get_obj

        # Need to update this with client call
        res = client.get(get_obj)
        e.append("R(%s)%s" % (get_obj, res))
    else:
        print("Action must be either 'SET' or 'GET'")
        exit(1)
    events.append(e)

    print_event_list = True
    print_event_timeline = True
    if print_event_list:
        for e in range(len(events)):
            print("Event %d --> %s" % (e + 1, str(events[e])))
    if print_event_timeline:
        timeline = []
        timeline_lens = []
        for p in range(Utilities.num_replicas): # Either hard-set range or use Utilities
            process_event_log = []
            for e in events:
                if e[0] == p:
                    process_event_log.append(" " + e[1])
                else:
                    process_event_log.append(" " * 6)
            timeline.append("".join(process_event_log))
            timeline_lens.append(len(timeline[p]))
        border_str = "-" * (max(timeline_lens) + 4)
        for p in range(Utilities.num_replicas):
            print(border_str)
            print("P%d:%s" % (p + 1, timeline[p]))
        print(border_str)


## -----------------------------------------------------------------------------

if __name__ == "__main__":

    opts = argparse.ArgumentParser()
    opts.add_argument("--process_type", required=True,
       help="valid server types - sc_servers/linearized_servers/eventualized_servers/causal_servers\nvalid client types - sc_tests/linearized_tests/eventualized_tests/causal_tests")
    opts.add_argument("--num_replicas", required=True,
       help="Number of server replicas")

    args = vars(opts.parse_args())

    select = args['process_type']
    num_replicas = int(args['num_replicas'])

    ## Sequential configuration --------------------------------------------------
    if select == "sc_tests":
        utilobj = Utilities(num_replicas, "sequential")
        clntobj = SpawnClient("sequential", utilobj.ports)

    elif select == "sc_servers":
        utilobj = Utilities(num_replicas, "sequential")
        servobj = SpawnReplicas(num_replicas, utilobj.ports, utilobj.consistency_model)

    ## Linearized configuration --------------------------------------------------
    elif select == "linearized_servers":
        utilobj = Utilities(num_replicas, "linearized")
        servobj = SpawnReplicas(num_replicas, utilobj.ports, utilobj.consistency_model)

    elif select == "linearized_tests":
        utilobj = Utilities(num_replicas, "linearized")
        clntobj = SpawnClient("linearized", utilobj.ports)

    ## Eventual configuration --------------------------------------------------
    elif select == "eventualized_servers":
        utilobj = Utilities(num_replicas, "eventual")
        servobj = SpawnReplicas(num_replicas, utilobj.ports, utilobj.consistency_model)

    elif select == "eventualized_tests":
        utilobj = Utilities(num_replicas, "eventual")
        TestsForEC(utilobj.ports).test()

    ## Causal configuration --------------------------------------------------
    elif select == "causal_servers":
        if select == "causal_servers":
            utilobj = Utilities(num_replicas, "causal")
            test_port = utilobj.GetPrimary()
            print(test_port)
            #servobj = SpawnReplicas(num_replicas, utilobj.ports, utilobj.consistency_model)
        if select == "causal_tests":
            utilobj = Utilities(num_replicas, "causal")
            clntobj = SpawnClient("causal", utilobj.ports)
    else:
        print("Unknown op: {}".format(select))
        exit(1)
