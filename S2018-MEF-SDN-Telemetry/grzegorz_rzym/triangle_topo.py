from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.node import (OVSSwitch, RemoteController)

c = RemoteController('c', ip='192.168.30.2', port=6633)

class MyTopo(Topo):
    def build(self):
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')

        self.addLink(s1, s2)
#        self.addLink(s1, s3)
        self.addLink(s2, s3)

        h1 = self.addHost('generator')
        h3 = self.addHost('server')
        h2 = self.addHost('attacker')

        self.addLink(h1, s1)
        self.addLink(h2, s2)
        self.addLink(h3, s3)

def main():
    topo = MyTopo()
    net = Mininet(topo = topo, switch = OVSSwitch, controller = c, autoSetMacs = True)
    net.start()
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    main()

