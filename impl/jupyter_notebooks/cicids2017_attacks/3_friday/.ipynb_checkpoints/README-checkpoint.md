## Friday 2017-07-07

```bash
$ capinfos -T -r -a -e Friday-WorkingHours.pcap
Friday-WorkingHours.pcap        2017-07-07 13:59:39.599128      2017-07-07 22:02:41.169108
```

## Botnet ARES 

Attack time: 10:02 a.m. – 11:02 a.m.

## Port Scan

Attack time: 

Firewall Rule on (**13:55 – 13:57**, **13:58 – 14:00**, **14:01 – 14:04**, **14:05 – 14:07**, **14:08 - 14:10**, **14:11 – 14:13**, **14:14 – 14:16**, 14:17 – 14:19, 14:20 – 14:21, 14:22 – 14:24, 14:33 – 14:33, 14:35 - 14:35)

Firewall rules off (sS 14:51-14:53, sT 14:54-14:56, sF 14:57-14:59, sX 15:00-15:02, sN 15:03-15:05, sP 15:06-15:07, sV 15:08-15:10, sU 15:11-15:12, sO 15:13-15:15, sA 15:16-15:18, sW 15:19-15:21, sR 15:22-15:24, sL 15:25-15:25, sI 15:26-15:27, b 15:28-15:29)

Attacker: **205.174.165.73**

Victim: **205.174.165.68**, (Local IP: **192.168.10.50**)

#### NAT Process on Firewall:

Attacker: 205.174.165.73 -> 205.174.165.80 (Valid IP of the Firewall) -> 172.16.0.1

Command for extraction of a smaller pcap consisting of 30 minutes (considering time shift **+6 hours** due to different time zones):

```bash
$ editcap -F libpcap -A "2017-07-07 18:35:00" -B "2017-07-07 19:05:00" Friday-WorkingHours.pcap friday_port_scan.pcap
```

## DDoS LOIT

Attack time: 15:56 – 16:16 p.m.

Attackers: **205.174.165.69**, **205.174.165.70** **205.174.165.71**

Victim: **205.174.165.68**, (Local IP: **192.168.10.50**)

#### NAT Process on Firewall:

Attackers: 205.174.165.69, 70, 71 -> 205.174.165.80 (Valid IP of the Firewall) -> 172.16.0.1

Command for extraction of a smaller pcap:

```bash
$ editcap -F libpcap -A "2017-07-07 20:55:00" -B "2017-07-07 21:25:00" Friday-WorkingHours.pcap friday_ddos.pcap
```

*(Only one attacker extracted this way => treated as DoS.)*