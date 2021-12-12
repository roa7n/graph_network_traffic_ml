## CIC-IDS 2017 attacks

Command for graph data generation (takes a PCAP file as first input):

```bash
$ cd granef/
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

