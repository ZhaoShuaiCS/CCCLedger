from hostnames import *
fw = open("./ifconfig.txt", "w+")
for ip in hostip_machines:
    fw.write(ip + "\n")
