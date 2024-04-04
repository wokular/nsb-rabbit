import ipaddress

# Create a function that generates an alias map for a network of num_nodes nodes.
def generate_aliasmap(num_nodes, subnet="10.0.0.0/24", outputfile=None):
    # Generate IP addresses based on subnet, but only the first num_nodes.
    ip_addresses = [str(ip) for ip in ipaddress.IPv4Network(subnet).hosts()][:num_nodes]
    # Create a list of IP pairs.
    pairs = []
    for ip in ip_addresses:
        pairs.append(f"{ip}:{ip}")
    # Join them with new lines.
    aliasmap = "\n".join(pairs)
    # Set output file to default if not specified.
    if outputfile is None:
        outputfile = f"{num_nodes}_nodes_aliasmap.txt"
    # Write to file.
    with open(outputfile, "w") as f:
        f.write(aliasmap)

if __name__ == "__main__":
    # Generate alias maps for 5, 10, 15, 20, and 25.
    for num_nodes in [5, 10, 15, 20, 25]:
        generate_aliasmap(num_nodes)