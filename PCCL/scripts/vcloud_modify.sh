#!/bin/bash
#
USERNAME=wangjunkai
HOSTS="$1"
IDENTITY="~/aws.pem"

for HOSTNAME in ${HOSTS}; do
# 	ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "
#  echo '* soft nofile 50000' | sudo tee --append /etc/security/limits.conf
#  "
	ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "
  echo mas150590 | sudo -S sed -i '/-A INPUT -p tcp -m state --state NEW -m tcp --dport 10000:99999 -j ACCEPT/d' /etc/iptables/rules.v4 
  echo mas150590 | sudo -S sed -i 's/:OUTPUT ACCEPT [8:1168]/:OUTPUT ACCEPT [463:49013]/g' /etc/iptables/rules.v4
  echo mas150590 | sudo -S sed -i '/-A INPUT -j REJECT --reject-with icmp-host-prohibited/d' /etc/iptables/rules.v4
  rm -rf resdb
  mkdir resdb
  cd resdb
  mkdir results
  echo ${HOSTNAME}
  "
  # ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "
  # sudo iptables-restore < /etc/iptables/rules.v4
  # mkdir resdb
  # cd resdb
  # mkdir results
  # sudo sed -i 's/#MaxSessions 10/MaxSessions 200/g' /etc/ssh/sshd_config &
  # "
  # ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "
  # sudo sed -i 's/#MaxSessions 10/MaxSessions 200/g' /etc/ssh/sshd_config &
  # echo ${HOSTNAME}
  # "
  # sudo sed -i 's/#MaxSessions 10/MaxSessions 200/g' /etc/ssh/sshd_config &
  # echo ${HOSTNAME}
  # ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME} "
  # sudo iptables-restore < /etc/iptables/rules.v4
  # "
#  echo '* hard nofile 90000' | sudo tee --append /etc/security/limits.conf
#  "
done

  # sudo apt-get update
  # sudo apt-get upgrade
  # sudo apt-get install gcc -y
  # sudo apt-get install g++ -y
  # sudo apt-get install cmake -y

sudo sed -i '/-A INPUT -p tcp -m state --state NEW -m tcp --dport 10000:99999 -j ACCEPT/d' /etc/iptables/rules.v4 
sudo sed -i 's/:OUTPUT ACCEPT [8:1168]/:OUTPUT ACCEPT [463:49013]/g' /etc/iptables/rules.v4
sudo sed -i '/-A INPUT -j REJECT --reject-with icmp-host-prohibited/d' /etc/iptables/rules.v4
sudo sed -i 's/#MaxSessions 10/MaxSessions 200/g' /etc/ssh/sshd_config &
echo ${HOSTNAME}