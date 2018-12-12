# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time, os, json
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ipv4
from ryu.lib.packet import tcp
from ryu.lib.packet import udp

def PacketLog(features_events = [], table_id = 1, *args):
    class PacketLogger(app_manager.RyuApp):
        OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

        def __init__(self, *args, **kwargs):
            super(PacketLogger, self).__init__(*args, **kwargs)
            self.table_id = table_id
            self.periodStart = time.time()

        def start(self):
            super(PacketLogger, self).start()
            for ev in features_events:
                self.switch_features_handler(ev)
            
            try:
                os.mkdir('out')
            except OSError:
                pass

        @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
        def switch_features_handler(self, ev):
            self.add_logger_flow(ev.msg.datapath)

        def add_logger_flow(self, dp):
            ofproto = dp.ofproto
            parser = dp.ofproto_parser

            match = parser.OFPMatch(**dict(zip(args[::2], args[1::2])))
            actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                              ofproto.OFPCML_NO_BUFFER)]
            self.add_flow(dp, 0, match, actions)

        def add_flow(self, datapath, priority, match, actions, hard_timeout=0, buffer_id=None):
            ofproto = datapath.ofproto
            parser = datapath.ofproto_parser

            #new-22.1
            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions),
                    parser.OFPInstructionGotoTable(self.table_id + 1)]

            if buffer_id:
                mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, table_id=self.table_id,
                                        priority=priority, match=match, hard_timeout=hard_timeout,
                                        instructions=inst)
            else:
                mod = parser.OFPFlowMod(datapath=datapath, priority=priority, hard_timeout=hard_timeout, table_id=self.table_id,
                                        match=match, instructions=inst)
            datapath.send_msg(mod)

        @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
        def _packet_in_handler(self, ev):
            msg = ev.msg
            pkt = packet.Packet(msg.data)
            dp = msg.datapath

            ip = pkt.get_protocol(ipv4.ipv4)
            if ip is None or ip.proto not in [6, 17]: # if the packet is not tcp nor udp
                return

            if ip.proto == 6: # packet is tcp
                proto = pkt.get_protocol(tcp.tcp)
                protoName = 'tcp'
            else: # packet is udp
                proto = pkt.get_protocol(udp.udp)
                protoName = 'udp'
            
            data = {
                "origin": "packet_logger",
                "timestamp": time.time(),
                "switch_id": dp.id,
                "period_start": self.periodStart,
                "ip_src": ip.src,
                "ip_dst": ip.dst,
                "port_src": proto.src_port,
                "port_dst": proto.dst_port,
                "protocol": protoName,
                "ingress_port": msg.match['in_port']
            }

            with open('out/packet_log.out', 'a') as file:
                file.write(json.dumps(data) + '\n')

    return PacketLogger
