//
// SPDX-License-Identifier: LGPL-3.0-or-later
//


package nsb_wireless_dev.simulations.wireless_module;

import inet.networklayer.configurator.ipv4.Ipv4NetworkConfigurator;
import inet.node.ethernet.Eth100M;
import inet.node.ethernet.Eth10M;
import inet.node.ethernet.EthernetSwitch;
import inet.node.inet.Router;
import inet.node.inet.StandardHost;
import inet.visualizer.canvas.integrated.IntegratedCanvasVisualizer;


network wiredNetwork
{
    @display("bgb=1610.5125,874.96497");
    submodules:
        configurator: Ipv4NetworkConfigurator {
            @display("p=100,100");
        }
        host0: StandardHost {
            @display("p=268.32748,417.6225");
        }
        host1: StandardHost {
            @display("p=227.9775,542.7075");
        }
        visualizer: IntegratedCanvasVisualizer {
            @display("p=98.8575,209.81999");
        }
    connections:
        host0.ethg++ <--> Eth100M <--> host1.ethg++;
        
}

