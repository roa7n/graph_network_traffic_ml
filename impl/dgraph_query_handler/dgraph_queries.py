#! /usr/bin/env python3
# -*- coding: utf-8 -*-


def handle_query(client, query_body: str, query_header: str = '', variables: dict = None):
    """
    General function to process a Dgraph query. Result is provided as a JSON response or
    extended by graph data according to desired query type.
    """
    try:
        query_result = client.query(query_header + query_body, variables)
        return query_result
    except Exception as e:
        print('Exception thrown: ' + str(e))
    return None


def query_get_host_ips(client):
    query_body = """{
      queryHosts(func: type(Host)) { 
        host.ip 
      } 
    }"""
    return handle_query(client, query_body=query_body)


def generate_connections_simple_query(direction):
    reverse_direction = 'responded' if direction == 'originated' else 'originated'
    return f"""{{
      queryHost{direction.capitalize()}(func: eq(host.ip, $ip)) {{ 
        originated_ip : host.ip 
        host.{direction} (offset: $offset, first: $first) {{
          uid
          connection.uid
          connection.conn_state
          connection.duration
          connection.orig_bytes
          connection.orig_ip_bytes
          connection.orig_p
          connection.orig_pkts
          connection.proto
          connection.resp_bytes
          connection.resp_ip_bytes
          connection.resp_p
          connection.resp_pkts
          connection.service
          connection.ts
          
          connection.produced {{
            expand(_all_)
            type: dgraph.type
            
            files.fuid {{
              expand(File)
            }}
          }}

          ~host.{reverse_direction} {{
            responded_ip : host.ip
          }}
        }}
      }}
    }}"""


def query_host_originated_connections_simple(client, ip: str, offset: str, first: str):
    query_header = 'query queryHostOriginated($ip: string, $offset: string, $first: string)'
    query_body = generate_connections_simple_query('originated')
    variables_dict = {'$ip': ip, '$offset': offset, '$first': first}

    return handle_query(client, query_body=query_body, query_header=query_header, variables=variables_dict)


def query_host_responded_connections_simple(client, ip: str, offset: str, first: str):
    query_header = 'query queryHostResponded($ip: string, $offset: string, $first: string)'
    query_body = generate_connections_simple_query('responded')
    variables_dict = {'$ip': ip, '$offset': offset, '$first': first}

    return handle_query(client, query_body=query_body, query_header=query_header, variables=variables_dict)


def generate_neighbourhood_query(first_direction, second_direction):
    reverse_direction = 'responded' if first_direction == 'originated' else 'originated'
    return f"""{{
      queryConnNeighbourhood(func: uid($uid)) @cascade {{
        connection.ts
        ~host.{first_direction} {{
          {first_direction}_ip : host.ip
          host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end)) {{
            uid
            connection.uid
            connection.conn_state
            connection.duration
            connection.orig_bytes
            connection.orig_ip_bytes
            connection.orig_p
            connection.orig_pkts
            connection.proto
            connection.resp_bytes
            connection.resp_ip_bytes
            connection.resp_p
            connection.resp_pkts
            connection.service
            connection.ts

            ~host.{reverse_direction} {{
              {reverse_direction}_ip : host.ip
            }}

          }}
        }}
      }}
    }}"""


def generate_neighbourhood_num_mean_query(first_direction, second_direction):
    reverse_direction = 'responded' if first_direction == 'originated' else 'originated'
    return f"""{{
      queryAverageNeighbourhood(func: uid($uid)) @cascade {{
        connection.ts
        ~host.{first_direction} {{
          {first_direction}_ip : host.ip
          host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end)) @normalize {{
            uids as math(1)
            duration as connection.duration
            orig_bytes as connection.orig_bytes
            orig_ip_bytes as connection.orig_ip_bytes
            #orig_p as connection.orig_p
            orig_pkts as connection.orig_pkts
            resp_bytes as connection.resp_bytes
            resp_ip_bytes as connection.resp_ip_bytes
            #resp_p as connection.resp_p
            resp_pkts as connection.resp_pkts
            ts as connection.ts

            ~host.{reverse_direction} {{
              {reverse_direction}_ip : host.ip
            }}

          }}

          avg_duration: avg(val(duration))
          avg_orig_bytes: avg(val(orig_bytes))
          avg_orig_ip_bytes: avg(val(orig_ip_bytes))
          #avg_orig_p: avg(val(orig_p))
          avg_orig_pkts: avg(val(orig_pkts))
          avg_resp_bytes: avg(val(resp_bytes))
          avg_resp_ip_bytes: avg(val(resp_ip_bytes))
          #avg_resp_p: avg(val(resp_p))
          avg_resp_pkts: avg(val(resp_pkts))
          min_ts: min(val(ts))
          max_ts: max(val(ts))
          count_all: sum(val(uids))

        }}
      }}
    }}"""


def generate_neighbourhood_cat_counts_query(first_direction, second_direction):
    return f"""{{
      queryNeighbourhoodConnstateCount(func: uid($uid)) @cascade @normalize {{
        connection.ts
        ~host.{first_direction} {{
          {first_direction}_ip : host.ip
          host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end)) @groupby(connection.conn_state) {{
            count(uid)
          }}
        }}
      }}

      queryNeighbourhoodProtoCount(func: uid($uid)) @cascade @normalize {{
        connection.ts
        ~host.{first_direction} {{
          {first_direction}_ip : host.ip
          host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end)) @groupby(connection.proto) {{
            count(uid)
          }}
        }}
      }}

      queryNeighbourhoodServiceCount(func: uid($uid)) @cascade @normalize {{
        connection.ts
        ~host.{first_direction} {{
          {first_direction}_ip : host.ip
          host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end)) @groupby(connection.service) {{
            count(uid)
          }}
        }}
      }}
    }}"""


def generate_neighbourhood_port_counts_query(first_direction, second_direction):
    return f"""{{
          queryNeighbourhoodPortOrigCount(func: uid($uid)) @cascade @normalize {{
            connection.ts
            ~host.{first_direction} {{
              {first_direction}_ip : host.ip
              host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end)) @groupby(connection.orig_p) {{
                count(uid)
              }}
            }}
          }}

          queryNeighbourhoodPortRespCount(func: uid($uid)) @cascade @normalize {{
            connection.ts
            ~host.{first_direction} {{
              {first_direction}_ip : host.ip
              host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end)) @groupby(connection.resp_p) {{
                count(uid)
              }}
            }}
          }}
        }}"""


def generate_protocol_filter(orig_protocol):
    return f'AND eq(connection.proto, "{orig_protocol}")'


def generate_service_filter(orig_service):
    return f'AND eq(connection.service, "{orig_service}")'


def generate_conn_state_filter(orig_conn_state):
    """
    TODO: docs

    S0: Connection attempt seen, no reply.                                                                        [S0]
    S1: Connection established, not terminated.                                                                   [S1]
    SF: Normal establishment and termination. Note that this is the same symbol as for state S1. For S1
        there will not be any byte counts in the summary, while for SF there will be.                             [SF]
    REJ: Connection attempt rejected.                                                                            [REJ]
    S2: Connection established and close attempt by originator seen (but no reply from responder).                [S2]
    S3: Connection established and close attempt by responder seen (but no reply from originator).                [S3]
    RSTO: Connection established, originator aborted (sent a RST).                         [RST0, RSTR, RSTOS0, RSTRH]
    RSTR: Responder sent a RST.
    RSTOS0: Originator sent a SYN followed by a RST, we never saw a SYN-ACK from the responder.
    RSTRH: Responder sent a SYN ACK followed by a RST, we never saw a SYN from the (purported) originator.
    SH: Originator sent a SYN followed by a FIN, we never saw a SYN ACK from the responder (hence the
        connection was  “half” open).                                                                       [SH, SHR]
    SHR: Responder sent a SYN ACK followed by a FIN, we never saw a SYN from the originator.
    OTH: No SYN seen, just midstream traffic (one example of this is a “partial connection” that was not        [OTH]
        later closed).

    :param orig_conn_state:
    :return:
    """
    if orig_conn_state == 'RST0' or orig_conn_state == 'RSTR' or orig_conn_state == 'RSTOS0' \
            or orig_conn_state == 'RSTRH':
        f'AND (eq(connection.conn_state, "RST0") OR eq(connection.conn_state, "RSTR") ' \
        f'OR eq(connection.conn_state, "RSTOS0") OR eq(connection.conn_state, "RSTRH"))'
    elif orig_conn_state == 'SH' or orig_conn_state == 'SHR':
        return f'AND (eq(connection.conn_state, "SH") OR eq(connection.conn_state, "SHR"))'
    return f'AND eq(connection.conn_state, "{orig_conn_state}")'


def generate_duration_filter(orig_duration):
    # based on constants from data_exploration.ipynb
    if orig_duration <= 0.0:
        return 'AND le(connection.duration, 0.000001)'
    elif orig_duration <= 0.0001:
        return 'AND ge(connection.duration, 0.000001) AND le(connection.duration, 0.001)'
    elif orig_duration <= 0.009:
        return 'AND ge(connection.duration, 0.001) AND le(connection.duration, 0.05)'
    elif orig_duration <= 0.5:
        return 'AND ge(connection.duration, 0.05) AND le(connection.duration, 1.5)'
    elif orig_duration <= 5:
        return 'AND ge(connection.duration, 1.5) AND le(connection.duration, 10)'
    elif orig_duration <= 15:
        return 'AND ge(connection.duration, 10) AND le(connection.duration, 20)'
    elif orig_duration <= 30:
        return 'AND ge(connection.duration, 20) AND le(connection.duration, 40)'
    elif orig_duration <= 50:
        return 'AND ge(connection.duration, 40) AND le(connection.duration, 60)'
    elif orig_duration <= 75:
        return 'AND ge(connection.duration, 60) AND le(connection.duration, 90)'
    elif orig_duration <= 100:
        return 'AND ge(connection.duration, 75) AND le(connection.duration, 110)'
    return 'AND ge(connection.duration, 100)'


def generate_pkts_filter(pkts_str, orig_pkts):
    if orig_pkts <= 1:
        return f'AND eq(connection.{pkts_str}, {orig_pkts})'
    elif orig_pkts <= 5:
        return f'AND gt(connection.{pkts_str}, 1) AND le(connection.{pkts_str}, 10)'
    elif orig_pkts <= 30:
        return f'AND ge(connection.{pkts_str}, {orig_pkts - 5}) AND le(connection.{pkts_str}, {orig_pkts + 5})'
    return f'AND ge(connection.{pkts_str}, 30)'


def generate_bytes_filter(bytes_str, orig_bytes):
    if orig_bytes == 0:
        return f'AND eq(connection.{bytes_str}, 0)'
    elif orig_bytes <= 50:
        return f'AND gt(connection.{bytes_str}, 0) AND le(connection.{bytes_str}, 100)'
    elif orig_bytes <= 1450:
        # (e.g. for 150 generates interval: from 100 to 200)
        return f'AND ge(connection.{bytes_str}, {orig_bytes - 50}) AND le(connection.{bytes_str}, {orig_bytes + 50})'
    elif orig_bytes <= 35000:
        return f'AND ge(connection.{bytes_str}, {orig_bytes - 500}) AND le(connection.{bytes_str}, {orig_bytes + 500})'
    return f'AND ge(connection.{bytes_str}, {orig_bytes - 1000})'


def generate_ip_bytes_filter(bytes_str, orig_bytes):
    # TODO?
    return f'AND ge(connection.{bytes_str}, {orig_bytes - 50}) AND le(connection.{bytes_str}, {orig_bytes + 50})'


def generate_neighbourhood_similar_count_query(first_direction, second_direction, orig_attributes):
    # TODO: DEFINE SIMILARITY HERE!

    # categorical attributes filters
    protocol_filter = generate_protocol_filter(orig_attributes['connection.proto'])
    service_filter = generate_service_filter(orig_attributes['connection.service'])
    conn_state_filter = generate_conn_state_filter(orig_attributes['connection.conn_state'])

    # numerical attributes filters
    duration_filter = generate_duration_filter(orig_attributes['connection.duration'])
    orig_pkts_filter = generate_pkts_filter('orig_pkts', orig_attributes['connection.orig_pkts'])
    resp_pkts_filter = generate_pkts_filter('resp_pkts', orig_attributes['connection.resp_pkts'])
    orig_bytes_filter = generate_bytes_filter('orig_bytes', orig_attributes['connection.orig_bytes'])
    resp_bytes_filter = generate_bytes_filter('resp_bytes', orig_attributes['connection.resp_bytes'])
    orig_ip_bytes_filter= generate_ip_bytes_filter('orig_ip_bytes', orig_attributes['connection.orig_ip_bytes'])
    resp_ip_bytes_filter = generate_ip_bytes_filter('resp_ip_bytes', orig_attributes['connection.resp_ip_bytes'])

    # numerical in interval (duration, resp_bytes) if low num smaller window, if larger, larger window

    reverse_direction = 'responded' if first_direction == 'originated' else 'originated'
    return f"""{{
          querySimilarNeighbourhoodCount(func: uid($uid)) @cascade @normalize {{
            connection.ts
            ~host.{first_direction} {{
              #{first_direction}_ip : host.ip
              host.{second_direction} @filter(between(connection.ts, $ts_start, $ts_end) 
                                            {protocol_filter}
                                            {service_filter}
                                            {conn_state_filter}
                                            {duration_filter}
                                            {orig_pkts_filter}
                                            {resp_pkts_filter}
                                            {orig_bytes_filter}
                                            {resp_bytes_filter}
                                            {orig_ip_bytes_filter} 
                                            {resp_ip_bytes_filter}) @normalize {{
                uids as math(1)

                #~host.{reverse_direction} {{
                  #{reverse_direction}_ip : host.ip
                #}}

              }}

              count_similar: sum(val(uids))
            }}
          }}
        }}"""


def query_neighbourhood(client, first_direction: str, second_direction: str, uid: str, ts_start: str,
                                   ts_end: str):
    query_header = 'query queryConnNeighbourhood($uid: string, $ts_start: string, $ts_end: string)'
    query_body = generate_neighbourhood_query(first_direction, second_direction)
    variables_dict = {'$uid': uid, '$ts_start': ts_start, '$ts_end': ts_end}

    return handle_query(client, query_body=query_body, query_header=query_header, variables=variables_dict)


def query_neighbourhood_mean(client, first_direction: str, second_direction: str, uid: str,
                                            ts_start: str, ts_end: str):
    query_header = 'query queryAverageNeighbourhood($uid: string, $ts_start: string, $ts_end: string)'
    query_body = generate_neighbourhood_num_mean_query(first_direction, second_direction)
    variables_dict = {'$uid': uid, '$ts_start': ts_start, '$ts_end': ts_end}

    return handle_query(client, query_body=query_body, query_header=query_header, variables=variables_dict)


def query_neighbourhood_counts(client, first_direction: str, second_direction: str, uid: str, ts_start: str,
                                          ts_end: str):
    query_header = 'query queryNeighbourhoodConnstateCount($uid: string, $ts_start: string, $ts_end: string)'
    query_body = generate_neighbourhood_cat_counts_query(first_direction, second_direction)
    variables_dict = {'$uid': uid, '$ts_start': ts_start, '$ts_end': ts_end}

    return handle_query(client, query_body=query_body, query_header=query_header, variables=variables_dict)


def query_neighbourhood_port_counts(client, first_direction: str, second_direction: str, uid: str, ts_start: str,
                                    ts_end: str):
    query_header = 'query queryNeighbourhoodPortOrigCount($uid: string, $ts_start: string, $ts_end: string)'
    query_body = generate_neighbourhood_port_counts_query(first_direction, second_direction)
    variables_dict = {'$uid': uid, '$ts_start': ts_start, '$ts_end': ts_end}

    return handle_query(client, query_body=query_body, query_header=query_header, variables=variables_dict)


def query_neighbourhood_similar_counts(client, first_direction: str, second_direction: str, uid: str, ts_start: str,
                                       ts_end: str, orig_attributes: dict):
    query_header = 'query querySimilarNeighbourhoodCount($uid: string, $ts_start: string, $ts_end: string)'
    query_body = generate_neighbourhood_similar_count_query(first_direction, second_direction, orig_attributes)
    variables_dict = {'$uid': uid, '$ts_start': ts_start, '$ts_end': ts_end}

    return handle_query(client, query_body=query_body, query_header=query_header, variables=variables_dict)
