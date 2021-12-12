# Tuesday 2017-07-04

Attacker: **205.174.165.73**

Victim: **205.174.165.68**, (Local IP: **192.168.10.50**)

#### NAT Process on Firewall:

Attack: 205.174.165.73 -> 205.174.165.80 (Valid IP of the Firewall) -> 172.16.0.1 -> 192.168.10.50

Reply: 192.168.10.50 -> 172.16.0.1 -> 205.174.165.80 -> 205.174.165.73

```bash
$ capinfos -T -r -a -e Tuesday-WorkingHours.pcap
Tuesday-WorkingHours.pcap       2017-07-04 13:53:32.364079      2017-07-04 22:00:31.076755
```

## FTP-Patator 

Attack time: 9:20 – 10:20 a.m.

Command for extraction of a smaller pcap consisting of 30 minutes (considering time shift **+6 hours** due to different time zones):

```bash
$ editcap -F libpcap -A "2017-07-04 15:05:00" -B "2017-07-04 15:35:00" Tuesday-WorkingHours.pcap tuesday_ftp.pcap
```

## SSH-Patator 

Attack time: 14:00 – 15:00 p.m.

Command for extraction of a smaller pcap:

```bash
$ editcap -F libpcap -A "2017-07-04 19:45:00" -B "2017-07-04 20:15:00" Tuesday-WorkingHours.pcap tuesday_ssh.pcap
```