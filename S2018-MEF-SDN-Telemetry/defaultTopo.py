from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.topo import Topo
from mininet.node import (OVSSwitch, RemoteController)

class MyTopo(Topo):
    def build(self):
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')

        self.addLink(s1, s3)
        self.addLink(s1, s4)
        self.addLink(s2, s3)

        h1 = self.addHost('laptop')
        h3 = self.addHost('printer')
        h2 = self.addHost('pc')

        self.addLink(h1, s1)
        self.addLink(h2, s4)
        self.addLink(h3, s2)

def main():
    topo = MyTopo()
    net = Mininet(topo = topo, switch = OVSSwitch, controller = RemoteController, autoSetMacs = True)
    net.start()
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    main()

