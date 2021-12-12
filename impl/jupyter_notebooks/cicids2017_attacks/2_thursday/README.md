## Thursday 2017-07-06

Attacker: **205.174.165.73**

Victim: **205.174.165.68**, (Local IP: **192.168.10.50**)

#### NAT Process on Firewall:

Attack: 205.174.165.73 -> 205.174.165.80 (Valid IP of the Firewall) -> 172.16.0.1 -> 192.168.10.50

Reply: 192.168.10.50 -> 172.16.0.1 -> 205.174.165.80 -> 205.174.165.73

```bash
$ capinfos -T -r -a -e Thursday-WorkingHours.pcap
Thursday-WorkingHours.pcap      2017-07-06 13:58:58.492265      2017-07-06 22:04:44.364012
```

## Web Attack – Brute Force

Attack time: 9:20 – 10 a.m.

Command for extraction of a smaller pcap consisting of 30 minutes (considering time shift **+6 hours** due to different time zones):

```bash
$ editcap -F libpcap -A "2017-07-06 14:30:00" -B "2017-07-06 15:30:00" Thursday-WorkingHours.pcap thursday_brute_force.pcap
```

## Web Attack – XSS

Attack time: 10:15 – 10:35 a.m

Command for extraction of a smaller pcap:

```bash
$ editcap -F libpcap -A "2017-07-06 14:00:00" -B "2017-07-06 14:30:00" Thursday-WorkingHours.pcap thursday_xss.pcap
```

## Web Attack – Sql Injection

Attack time: 10:40 – 10:42 a.m.

Command for extraction of a smaller pcap:

```bash
$ editcap -F libpcap -A "2017-07-06 15:35:00" -B "2017-07-06 16:05:00" Thursday-WorkingHours.pcap thursday_sql_injection.pcap
```