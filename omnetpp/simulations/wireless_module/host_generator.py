"""
host_generator.py

This script generates a list of submodules to be used within a wireless 
network on OMNET++/INET. The submodules are generated based on the 
inputted number of nodes. The script also generates a .ned file that 
can be used directly with OMNET++/INET.
"""

header = """package nsb_wireless_dev.simulations.wireless_module;

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
        }"""

footer = """
}"""

# Generate the submodules for the wireless network.
def generate_submodules(num_nodes, spacing=150, top_left=(100,100)):
    import math
    # Based on number of nodes, generate a square-like grid.
    # Calculate the columns.
    num_cols = math.ceil(num_nodes ** 0.5)
    # Calculate the number of rows.
    num_rows = math.ceil(num_nodes / num_cols)
    # Create a list of positions given the number of rows and columns, top_left, and spacing.
    positions = []
    for i in range(num_rows):
        for j in range(num_cols):
            positions.append((top_left[0] + spacing * j, top_left[1] + spacing * i))
    print(positions)
    # Generate the submodules.
    submodules = ""
    for i in range(num_nodes):
        submodules += """
        host%d: <default("WirelessHost")> like INetworkNode {
            @display("p=%d,%d");
        }""" % (i, positions[i][0], positions[i][1])
        
    return submodules

if __name__ == "__main__":
    # Use argparse to get the number of nodes and filename.
    import argparse
    parser = argparse.ArgumentParser(description="Generate a wireless network with the specified number of nodes.")
    parser.add_argument("-n", "--num_nodes", type=int, help="The number of nodes to generate.", required=True)
    parser.add_argument("-o", "--filename", type=str, help="The name of the file to write to.", required=True)
    args = parser.parse_args()
    # Generate the submodules.
    submodules = generate_submodules(args.num_nodes)
    # Write the file.
    with open(args.filename, "w") as f:
        f.write(header + submodules + footer)
    # Notify that the file has been written.
    print("File written to %s." % args.filename)