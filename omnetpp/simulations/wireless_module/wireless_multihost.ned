package nsb_wireless_dev.simulations.wireless_module;

import inet.networklayer.configurator.ipv4.Ipv4NetworkConfigurator;
import inet.node.contract.INetworkNode;
import inet.physicallayer.wireless.common.contract.packetlevel.IRadioMedium;
import inet.visualizer.contract.IIntegratedVisualizer;

network WirelessNetwork
{
    parameters:
        @display("bgb=650,500;bgg=100,1,grey95");
        @figure[title](type=label; pos=0,-1; anchor=sw; color=darkblue);

        @figure[rcvdPkText](type=indicatorText; pos=380,20; anchor=w; font=,18; textFormat="packets received: %g"; initialValue=0);

    submodules:
        visualizer: <default(firstAvailableOrEmpty("IntegratedCanvasVisualizer"))> like IIntegratedVisualizer if typename != "" {
            @display("p=580,125");
        }
        configurator: Ipv4NetworkConfigurator {
            @display("p=580,200");
        }
        radioMedium: <default("UnitDiskRadioMedium")> like IRadioMedium {
            @display("p=580,275");
        }
        host0: <default("WirelessHost")> like INetworkNode {
            @display("p=100,100");
        }
        host1: <default("WirelessHost")> like INetworkNode {
            @display("p=250,100");
        }
        host2: <default("WirelessHost")> like INetworkNode {
            @display("p=400,100");
        }
        host3: <default("WirelessHost")> like INetworkNode {
            @display("p=550,100");
        }
        host4: <default("WirelessHost")> like INetworkNode {
            @display("p=100,250");
        }
        host5: <default("WirelessHost")> like INetworkNode {
            @display("p=250,250");
        }
        host6: <default("WirelessHost")> like INetworkNode {
            @display("p=400,250");
        }
        host7: <default("WirelessHost")> like INetworkNode {
            @display("p=550,250");
        }
        host8: <default("WirelessHost")> like INetworkNode {
            @display("p=100,400");
        }
        host9: <default("WirelessHost")> like INetworkNode {
            @display("p=250,400");
        }
        host10: <default("WirelessHost")> like INetworkNode {
            @display("p=400,400");
        }
        host11: <default("WirelessHost")> like INetworkNode {
            @display("p=550,400");
        }
        host12: <default("WirelessHost")> like INetworkNode {
            @display("p=100,550");
        }
        host13: <default("WirelessHost")> like INetworkNode {
            @display("p=250,550");
        }
        host14: <default("WirelessHost")> like INetworkNode {
            @display("p=400,550");
        }
}