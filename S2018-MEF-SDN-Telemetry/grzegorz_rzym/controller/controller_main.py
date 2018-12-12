import logging, os, json, time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from operator import attrgetter

from ryu.base.app_manager import RyuApp
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.dpset import DPSet
from ryu.app.wsgi import WSGIApplication
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types

from consts import API_PORT, STATS_INTERVAL, START_DATE
from api_main import StatsRestApi


class Stats(RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    _CONTEXTS = {
        'dpset': DPSet,
        'wsgi': WSGIApplication,
    }

    def __init__(self, *args, **kwargs):
        super(Stats, self).__init__(*args, **kwargs)
        # logger settings
        self.logger.setLevel(logging.INFO)
        self.logger.info('Application %s initialized', __name__)

        # routing
        self.mac_to_port = {}

        # dpset instance
        self.dpset = kwargs['dpset']

        # setup wsgi
        wsgi = kwargs['wsgi']
        wsgi.register(StatsRestApi, {
            StatsRestApi.controller_instance_name: self,
            StatsRestApi.dpset_instance_name: self.dpset
        })

        # init scheduler
        self.interval = STATS_INTERVAL
        self.sched = BackgroundScheduler()
        self.sched.start()
        logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
        logging.getLogger('apscheduler.scheduler').propagate = False

        # init place for PNDA watchdog
        try:
            os.mkdir('out')
        except OSError:
            pass

        # stats logs to file
        try:
            os.mkdir('stats')
        except OSError:
            pass

    def set_interval(self, interval):
        self.interval = interval
        self.change_sched_interval(interval)

    def change_sched_interval(self, interval):
        self.logger.debug("Rescheduling stat request to %i seconds", interval)
        for s in self.sched.get_jobs():
            self.logger.debug('rescheduling job %s', s.id)
            it = IntervalTrigger(seconds=interval)
            self.sched.reschedule_job(s.id, trigger=it)

    def send_flow_stats_request(self, datapath):
        # https://osrg.github.io/ryu-book/en/html/traffic_monitor.html
        self.logger.debug('Sending flow stat request to sw: %016x%i', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPFlowStatsRequest(datapath)

        datapath.send_msg(req)
        with open('stats/flow_stats_req.txt', 'a') as file:
            file.write(str(int(time.time())) + ' ' +str(ofproto.OFP_FLOW_STATS_SIZE) + ' ' +str(req) + '\n')

    def send_port_stats_request(self, datapath):
        self.logger.debug('Sending flow stat request to sw: %016x%i', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)
        with open('stats/port_stats_req.txt', 'a') as file:
            file.write(str(int(time.time())) + ' ' +str(ofproto.OFP_PORT_STATS_SIZE) + ' '+ str(req) + '\n')

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        flow_stats = []
        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match['in_port'],
                                             flow.match['eth_dst'])):
            flow_stats.append('table_id=%s '
                         'duration_sec=%d duration_nsec=%d '
                         'priority=%d '
                         'idle_timeout=%d hard_timeout=%d flags=0x%04x '
                         'cookie=%d packet_count=%d byte_count=%d '
                         'match=%s instructions=%s' %
                         (stat.table_id,
                          stat.duration_sec, stat.duration_nsec,
                          stat.priority,
                          stat.idle_timeout, stat.hard_timeout, stat.flags,
                          stat.cookie, stat.packet_count, stat.byte_count,
                          stat.match, stat.instructions))
            data = {
                "origin": "flow_stats",
                "timestamp": time.time(),
                "switch_id": ev.msg.datapath.id,
                "duration_sec": stat.duration_sec,
                "duration_nsec": stat.duration_nsec,
                "src_mac": stat.match['eth_src'],
                "dst_mac": stat.match['eth_dst'],
                "byte_count": stat.byte_count,
                "packet_count": stat.packet_count,
                "in_port": stat.match['in_port']
            }

            with open('out/flow_stats.out', 'a') as file:
                file.write(json.dumps(data) + '\n')

        self.logger.debug('FlowStats for switch %i: %s', ev.msg.datapath.id, flow_stats)
        with open('stats/flow_stats_reply.txt', 'a') as file:
            file.write(str(int(time.time())) + ' ' +str(ev.msg.msg_len) + '\n')

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        body = ev.msg.body
        port_stats = []
        for stat in sorted(body, key=attrgetter('port_no')):
            port_stats.append("port_no=%d "
                              "rx_packets=%d "
                              "rx_bytes=%d "
                              "rx_errors=%d "
                              "tx_packets=%d "
                              "tx_bytes=%d "
                              "tx_errors=%d " %
                              (stat.port_no,
                               stat.rx_packets, stat.rx_bytes, stat.rx_errors,
                               stat.tx_packets, stat.tx_bytes, stat.tx_errors)
                              )
            data = {
                "origin": "port_stats",
                "timestamp": time.time(),
                "switch_id": ev.msg.datapath.id,
                "port_no": stat.port_no,
                "rx_packets": stat.rx_packets,
                "rx_bytes": stat.rx_bytes,
                "rx_errors": stat.rx_errors,
                "tx_packets": stat.tx_packets,
                "tx_bytes": stat.tx_bytes,
                "tx_errors": stat.tx_errors
            }

            with open('out/port_stats.out', 'a') as file:
                file.write(json.dumps(data) + '\n')
        self.logger.debug('PortStats for switch %i: %s', ev.msg.datapath.id, port_stats)
        with open('stats/port_stats_reply.txt', 'a') as file:
            file.write(str(int(time.time())) + ' ' + str(ev.msg.msg_len) + '\n')

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # install table-miss flow entry
        #
        # We specify NO BUFFER to max_len of the output action due to
        # OVS bug. At this moment, if we specify a lesser number, e.g.,
        # 128, OVS will send Packet-In with invalid buffer_id and
        # truncated packet data. In that case, we cannot output packets
        # correctly.  The bug has been fixed in OVS v2.1.0.
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions, 0)

        # Add job to scheduler
        self.logger.debug('Starting scheduler for dp =%i', ev.msg.datapath.id)
        # ## flow stats
        self.sched.add_job(self.send_flow_stats_request, 'interval', seconds=STATS_INTERVAL, start_date=START_DATE,
                           args=[ev.msg.datapath])
        ## port stats
        self.sched.add_job(self.send_port_stats_request, 'interval', seconds=STATS_INTERVAL, start_date=START_DATE,
                           args=[ev.msg.datapath])

    def add_flow(self, datapath, priority, match, actions, idle_timeout, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst,idle_timeout=idle_timeout)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst,idle_timeout=idle_timeout)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return
        dst = eth.dst
        src = eth.src

        # self.logger.info("packet in dpid = %s src = %s dst = %s in_port = %s", dpid, src, dst, in_port)

        # learn a mac address to avoid FLOOD next time.
        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        # install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)

            # verify if we have a valid buffer_id, if yes avoid to send both
            # flow_mod & packet_out
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, 1, match, actions, 3, msg.buffer_id)
                return
            else:
                self.add_flow(datapath, 1, match, actions, 3)
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)


def run_controller(args=None):
    """
    Application startup function.
    Passes required parameters to Ryu core modules and ensures that link discovery is enabled.
    :param args:
    """
    from ryu.cmd.manager import main

    # add --observe-links parameter to enable topology discovery
    if not args:
        args = [__file__] + ['--observe-links', '--wsapi-port', str(API_PORT)]
    else:
        args = [__file__] + ['--observe-links', '--wsapi-port', str(API_PORT)] + args[1:]

    main(args)



if __name__ == '__main__':
    run_controller()
