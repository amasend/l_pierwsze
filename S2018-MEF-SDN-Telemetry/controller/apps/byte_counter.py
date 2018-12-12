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
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types

def ByteCount(features_events = [], table_id = 1, interval = 5):
    class ByteCounter(app_manager.RyuApp):
        OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

        def __init__(self, *args, **kwargs):
            super(ByteCounter, self).__init__(*args, **kwargs)
            self.interval = interval
            self.table_id = table_id

        def start(self):
            super(ByteCounter, self).start()
            for ev in features_events:
                self.switch_features_handler(ev)
            
            try:
                os.mkdir('out')
            except OSError:
                pass

        @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
        def switch_features_handler(self, ev):
            self.add_counter_flow(ev.msg.datapath)

        def add_counter_flow(self, dp):
            ofproto = dp.ofproto
            parser = dp.ofproto_parser

            match = parser.OFPMatch()
            actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                              ofproto.OFPCML_NO_BUFFER)]
            self.add_flow(dp, 0, match, actions, self.interval)

        def add_flow(self, datapath, priority, match, actions, hard_timeout=0, buffer_id=None):
            ofproto = datapath.ofproto
            parser = datapath.ofproto_parser

            #new-22.1
            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions),
                    parser.OFPInstructionGotoTable(self.table_id + 1)]

            if buffer_id:
                mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, table_id=self.table_id,
                                        priority=priority, match=match, hard_timeout=hard_timeout,
                                        instructions=inst, flags=ofproto.OFPFF_SEND_FLOW_REM)
            else:
                mod = parser.OFPFlowMod(datapath=datapath, priority=priority, hard_timeout=hard_timeout, table_id=self.table_id,
                                        match=match, instructions=inst, flags=ofproto.OFPFF_SEND_FLOW_REM)
            datapath.send_msg(mod)

        @set_ev_cls(ofp_event.EventOFPFlowRemoved, MAIN_DISPATCHER)
        def flow_removed_handler(self, ev):
            msg = ev.msg
            if (msg.table_id != self.table_id):
                return

            dp = msg.datapath
            ofp = dp.ofproto

            self.add_counter_flow(dp)
            
            data = {
                "origin": "byte_counter",
                "timestamp": time.time(),
                "switch_id": dp.id,
                "duration": msg.duration_sec,
                "byte_count": msg.byte_count
            }

            with open('out/byte_count.out', 'a') as file:
                file.write(json.dumps(data) + '\n')

    return ByteCounter
