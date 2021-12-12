## CIC-IDS 2017 attacks

Iterate over paths:

| DIR_PATH | PCAP_NAME | STATUS |
|---|---|---|
| /home/sramkova/diploma_thesis_data/cicids2017/attacks/0_tuesday/ftp_patator/ | tuesday_ftp.pcap | :heavy_check_mark: |
| /home/sramkova/diploma_thesis_data/cicids2017/attacks/0_tuesday/ssh_patator/ | tuesday_ssh.pcap | :heavy_check_mark: |
| /home/sramkova/diploma_thesis_data/cicids2017/attacks/1_wednesday/dos/ | wednesday_dos_hulk.pcap | :heavy_check_mark: |
| /home/sramkova/diploma_thesis_data/cicids2017/attacks/2_thursday/web_attacks_brute_force/ | thursday_brute_force.pcap | :x: |
| /home/sramkova/diploma_thesis_data/cicids2017/attacks/2_thursday/web_attacks_sql_injection/ | thursday_sql_injection.pcap | :x: |
| /home/sramkova/diploma_thesis_data/cicids2017/attacks/3_friday/port_scan/ | friday_port_scan.pcap | :x: |
| /home/sramkova/diploma_thesis_data/cicids2017/attacks/3_friday/ddos/ |  friday_ddos.pcap| :x: |

Command for graph data generation:

```bash
$ cd /home/sramkova/dev/granef/
$ python3 granef.py -d {DIR_PATH} -i {PCAP_NAME} -o extraction -t run
$ python3 granef.py -d {DIR_PATH} -o transformation -t run
$ python3 granef.py -d {DIR_PATH} -o indexing -t run
$ python3 granef.py -d {DIR_PATH} -o handling -t run
```

On `http://127.0.0.1:8080` at `https://play.dgraph.io/?local` check time:

```
{ 
	getMinMaxTime(func: eq(host.ip, "192.168.10.50")) @normalize { 
		name : host.ip 
		
		host.originated {
			orig_time as connection.ts
		}
		
		host.responded {
			resp_time as connection.ts
		}
			
		min_orig : min(val(orig_time)) 
		max_orig : max(val(orig_time)) 
		
		min_resp : min(val(resp_time)) 
		max_resp : max(val(resp_time)) 
	} 
}
```

Check victim:

```
{ 
	getConns(func: eq(host.ip, "192.168.10.50")) { 
		name : host.ip 
		
		host.originated {
			~host.responded {
				host.ip
			}
			expand(Connection)
		}
		
		host.responded {
            ~host.originated {
            	host.ip
            }
			expand(Connection)
		}
	} 
}
```

