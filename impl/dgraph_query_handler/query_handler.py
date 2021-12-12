#! /usr/bin/env python3
# -*- coding: utf-8 -*-


"""
Connects to running Dgraph database on <dgraph_ip:dgraph_port>.

2 modes:
  'IPs mode' is used to get all host IPs in network. They are saved to a new file (one line contains one host IP).
  It has to be used beforehand, because the output file is used as input to the next mode.
      Usage: $ python3 query_handler.py -im -ou host_ips

  'CONNECTIONS mode' is used to get connections of all hosts whose IPs are in input file. The result JSON is then
  flattened and the result for each host is saved to a separate CSV file.
      Usage: $ python3 query_handler.py -cm --ips_csv host_ips.csv

Usage: $ python3 query_handler.py <-im|-cm> -ip <dgraph_ip> -p <dgraph_port> -a <amount_on_page> -of <output_file>
         -od <output_directory> --ips_csv <output_of_ips_mode>
"""

import sys
import argparse
import orjson as json
import dateutil.parser
import datetime
import multiprocessing
import pandas_funcs
import pandas as pd
import dgraph_queries as queries
from dgraph_client import DgraphClient


COMMON_PORTS_MAPPER = {
    21: 'resp_21_count',
    22: 'resp_22_count',
    53: 'resp_53_count',
    80: 'resp_80_count',
    123: 'resp_123_count',
    443: 'resp_443_count',
    3389: 'resp_3389_count',
}


TIME_WINDOW_HOURS = 0
TIME_WINDOW_MINUTES = 5
TIME_WINDOW_SECONDS = 0


def generate_empty_cat_count_dictionaries():
    proto_dict = {'tcp': 0, 'udp': 0, 'icmp': 0}
    service_dict = {'NaN': 0, 'ssl': 0, 'dns': 0, 'ntp': 0, 'http': 0, 'ssh': 0, 'dhcp' : 0,
                    'krb_tcp': 0, 'dce_rpc': 0, 'smtp': 0, 'imap': 0, 'ssl,imap': 0, 'socks': 0,
                    'pop3': 0}
    conn_state_dict = {'S0': 0, 'SF': 0, 'RSTO': 0, 'RSTR': 0, 'OTH': 0, 'S1': 0, 'S3': 0, 'SHR': 0,
                       'S2': 0, 'RSTRH': 0, 'REJ': 0, 'SH': 0, 'RSTOS0': 0}
    return conn_state_dict, proto_dict, service_dict


def generate_empty_port_count_dictionary(first_direction, second_direction):
    prefix = 'orig_' if first_direction == 'originated' else 'resp_'
    prefix2 = 'orig' if second_direction == 'originated' else 'resp'
    return {prefix + prefix2 + '_orig_p_well_known_count': 0,
            prefix + prefix2 + '_orig_p_reg_or_dyn_count': 0,
            prefix + prefix2 + '_resp_p_21_count': 0,
            prefix + prefix2 + '_resp_p_22_count': 0,
            prefix + prefix2 + '_resp_p_53_count': 0,
            prefix + prefix2 + '_resp_p_80_count': 0,
            prefix + prefix2 + '_resp_p_123_count': 0,
            prefix + prefix2 + '_resp_p_443_count': 0,
            prefix + prefix2 + '_resp_p_3389_count': 0,
            prefix + prefix2 + '_resp_p_well_known_count': 0,
            prefix + prefix2 + '_resp_p_reg_count': 0,
            prefix + prefix2 + '_resp_p_dyn_count': 0}


def extract_mean_values(dgraph_client, first_direction, second_direction, uid, time_start, time_end):
    prefix = 'orig_' if first_direction == 'originated' else 'resp_'
    prefix2 = 'orig' if second_direction == 'originated' else 'resp'
    neighbourhood_averages = queries.query_neighbourhood_mean(dgraph_client, first_direction, second_direction, uid,
                                                              str(time_start), str(time_end))

    if neighbourhood_averages:
        neighbourhood_json = json.loads(neighbourhood_averages)
        avg_json = neighbourhood_json['queryAverageNeighbourhood']

        if avg_json:
            average_over_all_conns = avg_json[0][f'~host.{first_direction}'][0]
            avg_duration = average_over_all_conns['avg_duration']
            avg_orig_bytes = average_over_all_conns['avg_orig_bytes']
            avg_orig_ip_bytes = average_over_all_conns['avg_orig_ip_bytes']
            avg_orig_pkts = average_over_all_conns['avg_orig_pkts']
            avg_resp_bytes = average_over_all_conns['avg_resp_bytes']
            avg_resp_ip_bytes = average_over_all_conns['avg_resp_ip_bytes']
            avg_resp_pkts = average_over_all_conns['avg_resp_pkts']
            # TODO: Dgraph does not know how to do avg over time
            min_ts = average_over_all_conns['min_ts']
            max_ts = average_over_all_conns['max_ts']
            count_all = average_over_all_conns['count_all']

            return {prefix + prefix2 + '_total': count_all,
                    prefix + prefix2 + '_connection.time_min': min_ts,
                    prefix + prefix2 + '_connection.time_max': max_ts,
                    prefix + prefix2 + '_connection.duration_mean': avg_duration,
                    prefix + prefix2 + '_connection.orig_bytes_mean': avg_orig_bytes,
                    prefix + prefix2 + '_connection.orig_ip_bytes_mean': avg_orig_ip_bytes,
                    prefix + prefix2 + '_connection.orig_pkts_mean': avg_orig_pkts,
                    prefix + prefix2 + '_connection.resp_bytes_mean': avg_resp_bytes,
                    prefix + prefix2 + '_connection.resp_ip_bytes_mean': avg_resp_ip_bytes,
                    prefix + prefix2 + '_connection.resp_pkts_mean': avg_resp_pkts
                    }

    return {prefix + prefix2 + '_total': 0,
            prefix + prefix2 + '_connection.time_min': 0,
            prefix + prefix2 + '_connection.time_max': 0,
            prefix + prefix2 + '_connection.duration_mean': 0,
            prefix + prefix2 + '_connection.orig_bytes_mean': 0,
            prefix + prefix2 + '_connection.orig_ip_bytes_mean': 0,
            prefix + prefix2 + '_connection.orig_pkts_mean': 0,
            prefix + prefix2 + '_connection.resp_bytes_mean': 0,
            prefix + prefix2 + '_connection.resp_ip_bytes_mean': 0,
            prefix + prefix2 + '_connection.resp_pkts_mean': 0
            }


def save_result_counts(query_json, query_name, value_name, count_dictionary):
    query_result = query_json[query_name]
    query_result = query_result[0] if len(query_result) >= 1 else ''
    if '@groupby' in query_result:
        query_result = query_result['@groupby']

        for value_count in query_result:
            category_name = value_count[value_name]
            val = value_count['count']
            count_dictionary[category_name] = val


def extract_cat_counts(dgraph_client, first_direction, second_direction, uid, time_start, time_end):
    prefix = 'orig_' if first_direction == 'originated' else 'resp_'
    prefix2 = 'orig' if second_direction == 'originated' else 'resp'

    neighbourhood_counts = queries.query_neighbourhood_counts(dgraph_client, first_direction, second_direction, uid,
                                                              str(time_start), str(time_end))

    if neighbourhood_counts:
        neighbourhood_json = json.loads(neighbourhood_counts)
        conn_state_dict, proto_dict, service_dict = generate_empty_cat_count_dictionaries()

        save_result_counts(neighbourhood_json, 'queryNeighbourhoodConnstateCount', 'connection.conn_state',
                           conn_state_dict)
        save_result_counts(neighbourhood_json, 'queryNeighbourhoodProtoCount', 'connection.proto', proto_dict)
        save_result_counts(neighbourhood_json, 'queryNeighbourhoodServiceCount', 'connection.service',
                           service_dict)

        return {prefix + prefix2 + '_proto_tcp_count': proto_dict['tcp'],
                prefix + prefix2 + '_proto_udp_count': proto_dict['udp'],
                prefix + prefix2 + '_proto_icmp_count': proto_dict['icmp'],
                prefix + prefix2 + '_connection.protocol_mode': max(proto_dict, key=proto_dict.get),
                prefix + prefix2 + '_connection.service_mode': max(service_dict, key=service_dict.get),
                prefix + prefix2 + '_connection.conn_state_mode': max(conn_state_dict, key=conn_state_dict.get)
                }

    # TODO: if all are 0, don't return max? (mode = most frequent category)

    # prefixes: <orig/resp neighbourhood>_<first_direction>_<second_direction>
    return {prefix + prefix2 + '_proto_tcp_count': 0,
            prefix + prefix2 + '_proto_udp_count': 0,
            prefix + prefix2 + '_proto_icmp_count': 0,
            prefix + prefix2 + '_connection.protocol_mode': '-',
            prefix + prefix2 + '_connection.service_mode': '-',
            prefix + prefix2 + '_connection.conn_state_mode': '-'
            }


def resp_port_cat_vals(value, first_direction, second_direction):
    prefix = 'orig_' if first_direction == 'originated' else 'resp_'
    prefix2 = 'orig_' if second_direction == 'originated' else 'resp_'
    if value in COMMON_PORTS_MAPPER.keys():
        return COMMON_PORTS_MAPPER[value]
    if value < 1024:
        return prefix + prefix2 + 'resp_p_well_known_count'
    if value < 49152:
        return prefix + prefix2 + 'resp_p_reg_count'
    return prefix + prefix2 + 'resp_p_dyn_count'


def orig_port_cat_vals(value, first_direction, second_direction):
    prefix = 'orig_' if first_direction == 'originated' else 'resp_'
    prefix2 = 'orig_' if second_direction == 'originated' else 'resp_'
    if value < 1024:
        return prefix + prefix2 + 'orig_p_well_known_count'
    return prefix + prefix2 + 'orig_p_reg_or_dyn_count'


def save_port_result_counts(query_json, query_name, value_name, count_dictionary, first_direction, second_direction,
                            mode):
    query_result = query_json[query_name]
    query_result = query_result[0] if len(query_result) >= 1 else ''
    if '@groupby' in query_result:
        query_result = query_result['@groupby']
        for value_count in query_result:
            category_name = value_count[value_name]
            real_category_name = orig_port_cat_vals(category_name,
                                                    first_direction,
                                                    second_direction) if mode == 'orig' else \
                resp_port_cat_vals(category_name,
                                   first_direction,
                                   second_direction)
            val = value_count['count']
            count_dictionary[real_category_name] = val


def extract_port_cat_counts(dgraph_client, first_direction, second_direction, uid, time_start, time_end):
    prefix = 'orig_' if first_direction == 'originated' else 'resp_'
    prefix2 = 'orig_' if second_direction == 'originated' else 'resp_'
    neighbourhood_port_counts = queries.query_neighbourhood_port_counts(dgraph_client, first_direction,
                                                                        second_direction, uid, str(time_start),
                                                                        str(time_end))

    if neighbourhood_port_counts:
        neighbourhood_json = json.loads(neighbourhood_port_counts)

        orig_port_dict = generate_empty_port_count_dictionary(first_direction, second_direction)
        resp_port_dict = generate_empty_port_count_dictionary(first_direction, second_direction)

        save_port_result_counts(neighbourhood_json, 'queryNeighbourhoodPortOrigCount', 'connection.orig_p',
                                orig_port_dict, first_direction, second_direction, 'orig')
        save_port_result_counts(neighbourhood_json, 'queryNeighbourhoodPortRespCount', 'connection.resp_p',
                                resp_port_dict, first_direction, second_direction, 'resp')

        return join_dicts('', [orig_port_dict, resp_port_dict])

    return {prefix + prefix2 + 'orig_p_well_known_count': 0,
            prefix + prefix2 + 'orig_p_reg_or_dyn_count': 0,
            prefix + prefix2 + 'resp_21_count': 0,
            prefix + prefix2 + 'resp_22_count': 0,
            prefix + prefix2 + 'resp_53_count': 0,
            prefix + prefix2 + 'resp_80_count': 0,
            prefix + prefix2 + 'resp_123_count': 0,
            prefix + prefix2 + 'resp_443_count': 0,
            prefix + prefix2 + 'resp_3389_count': 0,
            prefix + prefix2 + 'resp_p_well_known_count': 0,
            prefix + prefix2 + 'resp_p_reg_count': 0,
            prefix + prefix2 + 'resp_p_dyn_count': 0
            }


def extract_similar_count(dgraph_client, first_direction, second_direction, uid, time_start, time_end, orig_attributes):
    prefix = 'orig_' if first_direction == 'originated' else 'resp_'
    prefix2 = 'orig_' if second_direction == 'originated' else 'resp_'
    neighbourhood_similar_counts = queries.query_neighbourhood_similar_counts(dgraph_client, first_direction,
                                                                              second_direction, uid, str(time_start),
                                                                              str(time_end), orig_attributes)

    if neighbourhood_similar_counts:
        neighbourhood_json = json.loads(neighbourhood_similar_counts)
        neighbourhood_json = neighbourhood_json['querySimilarNeighbourhoodCount']
        if len(neighbourhood_json) >= 1:
            return {prefix + prefix2 + 'similar_count': neighbourhood_json[0]['count_similar']}

    return {prefix + prefix2 + 'similar_count': 0}


def generate_time_interval(dgraph_time_str, hours, minutes, seconds):
    # time is in the RFC3339 format
    date_time = dateutil.parser.isoparse(dgraph_time_str)
    start_time = date_time - datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    end_time = date_time + datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return start_time.isoformat().replace("+00:00", "Z"), end_time.isoformat().replace("+00:00", "Z")


def compute_time_neighbourhood(conn_uid, cur_time, direction, orig_attributes):
    reverse_direction = 'responded' if direction == 'originated' else 'originated'
    start_time, end_time = generate_time_interval(cur_time, TIME_WINDOW_HOURS, TIME_WINDOW_MINUTES, TIME_WINDOW_SECONDS)

    # a dictionary will be returned (thought of as one row in resulting csv file (df))
    neighbourhood_dict = {}

    neighbourhood_avgs_orig = extract_mean_values(dgraph_client, direction, direction, conn_uid, start_time, end_time)
    neighbourhood_cnts_orig = extract_cat_counts(dgraph_client, direction, direction, conn_uid, start_time, end_time)
    neighbourhood_port_cat_orig = extract_port_cat_counts(dgraph_client, direction, direction, conn_uid, start_time,
                                                          end_time)
    neighbourhood_cnt_similar_orig = extract_similar_count(dgraph_client, direction, direction, conn_uid, start_time,
                                                           end_time, orig_attributes)
    neighbourhood_avgs_rev = extract_mean_values(dgraph_client, direction, reverse_direction, conn_uid, start_time,
                                                 end_time)
    neighbourhood_cnts_rev = extract_cat_counts(dgraph_client, direction, reverse_direction, conn_uid, start_time,
                                                end_time)
    neighbourhood_port_cat_resp = extract_port_cat_counts(dgraph_client, direction, reverse_direction, conn_uid,
                                                          start_time, end_time)
    neighbourhood_cnt_similar_resp = extract_similar_count(dgraph_client, direction, reverse_direction, conn_uid,
                                                           start_time, end_time, orig_attributes)

    neighbourhood_dict.update(neighbourhood_avgs_orig)
    neighbourhood_dict.update(neighbourhood_cnts_orig)
    neighbourhood_dict.update(neighbourhood_port_cat_orig)
    neighbourhood_dict.update(neighbourhood_cnt_similar_orig)
    neighbourhood_dict.update(neighbourhood_avgs_rev)
    neighbourhood_dict.update(neighbourhood_cnts_rev)
    neighbourhood_dict.update(neighbourhood_port_cat_resp)
    neighbourhood_dict.update(neighbourhood_cnt_similar_resp)

    return neighbourhood_dict


def result_is_valid(result, mode):
    if mode == 'originated':
        query_name = 'queryHostOriginated'
        edge_direction = 'host.originated'
    else:
        query_name = 'queryHostResponded'
        edge_direction = 'host.responded'

    json_result = json.loads(result)
    if query_name in json_result:
        json_level_1 = json_result[query_name]

        # helper variable for storing for how many hosts there are no connection returner
        no_result_counter = len(json_level_1)
        for json_level_2 in json_level_1:
            if edge_direction not in json_level_2:
                no_result_counter -= 1

        # if at least for one host there were some connections returned, we still want to keep paginating,
        # otherwise we already got everything and we can end it
        if no_result_counter > 0:
            return True
    return False


def get_next_result(ip_var, offset_var, first_var, mode):
    if mode == 'originated':
        return queries.query_host_originated_connections(dgraph_client, ip_var, offset_var, first_var)
    return queries.query_host_responded_connections(dgraph_client, ip_var, offset_var, first_var)


def join_dicts(prefix, dict_list):
    final_dict = {}
    for dict in dict_list:
        for key in dict:
            final_dict[prefix + key] = dict[key]
    return final_dict


def output_conns_csv(output_path, host_ip, direction_str, hosts_dfs):
    file_name = output_path + '-' + direction_str + '-' + str(host_ip) + '.csv' if direction_str \
        else output_path + '-' + str(host_ip) + '.csv'
    all_hosts_connections = pandas_funcs.concat_multiple_dfs(hosts_dfs)
    all_hosts_connections.to_csv(file_name, index=False, header=True)
    print('Successfully wrote to file ' + file_name + '.')


def compute_and_write_host_neighbourhood(host_ip):
    print('\n[{}]: Computing neighbourhood for connections of originator {:15}'.format(
        datetime.datetime.now().strftime("%H:%M:%S"), host_ip))

    # pagination vars:
    page_counter = 0
    page_step = args.amount_on_page

    host_ip = host_ip.strip()
    print('##############\n' + host_ip + '\n##############')
    result = queries.query_host_originated_connections_simple(dgraph_client, str(host_ip), str(page_counter),
                                                              str(page_step))
    hosts_dfs = []

    if result:
        while result_is_valid(result, 'originated'):
            print('Result for IP ' + host_ip + ' and first ' + str(page_step) + ' with offset ' +
                  str(page_counter) + ' is valid.')

            # get all returned originated connections and compute neighbourhood for each:
            result_json = json.loads(result)
            connections = result_json['queryHostOriginated'][0]['host.originated']

            df_result = pd.DataFrame()
            for connection in connections:
                conn_uid = connection['uid']
                conn_ts = connection['connection.ts']

                originator_neighbourhood = compute_time_neighbourhood(conn_uid, conn_ts, 'originated', connection)
                responder_neighbourhood = compute_time_neighbourhood(conn_uid, conn_ts, 'responded', connection)

                # concat neighbourhoods with original connection:
                connection.update({'originated_ip': host_ip,
                                   'responded_ip': connection['~host.responded'][0]['responded_ip']})
                connection.pop('~host.responded', None)
                connection.update(originator_neighbourhood)
                connection.update(responder_neighbourhood)

                row_df = pd.DataFrame([connection])
                df_result = pd.concat([df_result, row_df], axis=0, ignore_index=True)

            # convert result in page range to CSV in order to write to one output file:
            # write result to a separate file:
            # csv_result = json_to_csv_mode(result)
            hosts_dfs.append(df_result)

            # get connections for subsequent page range:
            page_counter += page_step
            result = queries.query_host_originated_connections_simple(dgraph_client, str(host_ip), str(page_counter),
                                                                      str(page_step))

        print('Result for IP ' + host_ip + ' and first ' + str(page_step) + ' with offset ' + str(page_counter)
              + ' is NOT valid.')

    # write to one final CSV file:
    if len(hosts_dfs) > 0:
        output_conns_csv(output_path, host_ip, '', hosts_dfs)

        # head -n1 test.txt | tr , '\n' # print out only csv header - each col on new line
    else:
        print('No result returned for IP ' + host_ip + '.')


def get_host_connections(host_ip, mode):
    if mode == 'originated':
        query_func = queries.query_host_originated_connections_simple
    else:
        query_func = queries.query_host_responded_connections_simple

    page_counter = 0
    page_step = args.amount_on_page

    host_ip = host_ip.strip()
    print('\n##############\n' + host_ip + '\n (' + mode + ')' + '\n##############')
    result = query_func(dgraph_client, str(host_ip), str(page_counter), str(page_step))

    hosts_dfs = []

    if result:
        while result_is_valid(result, mode):
            print('Result for IP ' + host_ip + ' and first ' + str(page_step) + ' with offset ' + str(page_counter) +
                  ' is valid.')

            # convert result in page range to CSV in order to write to one output file:
            # write result to a separate file:
            csv_result = pandas_funcs.convert_json_to_csv_conns(result, mode)
            hosts_dfs.append(csv_result)

            # get connections for subsequent page range:
            page_counter += page_step
            result = query_func(dgraph_client, str(host_ip), str(page_counter), str(page_step))

        print('Result for IP ' + host_ip + ' and first ' + str(page_step) + ' with offset ' + str(page_counter)
              + ' is NOT valid.')

        # write to one CSV file:
        if len(hosts_dfs) > 0:
            output_conns_csv(output_path, host_ip, mode[0], hosts_dfs)
    else:
        print('Something went wrong with trying to get the connections result of ' + str(host_ip) + ' from Dgraph.')


def define_arguments():
    """
        Add arguments to ArgumentParser (argparse) module instance.

        :return: Parsed arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('-ip', '--dgraph_ip', help='Dgraph server IP address', type=str, default='127.0.0.1')
    parser.add_argument('-p', '--dgraph_port', help='Dgraph server port', type=int, default=9080)

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('-im', '--ips_mode', help='Host IPs will be stored to a CSV file.', action='store_true')
    mode.add_argument('-nm', '--neighbourhood_mode', help='CSV result of query with neighbourhood will be stored.',
                      action='store_true')
    mode.add_argument('-cm', '--connections_mode', help='CSV result of query will be stored.', action='store_true')

    parser.add_argument('-a', '--amount_on_page', help='Query variable: "first" query pagination value', type=int,
                        default=10000)
    parser.add_argument('-of', '--output_file', help='Output JSON/CSV file name (without ".json"/".csv")', type=str,
                        default='output')
    parser.add_argument('-od', '--output_directory', help='Output directory absolute path', type=str,
                        default='/home/sramkova/dev/storage/ml/')

    parser.add_argument('--ips_csv', help='Path to CSV file with host IPs', type=str, required='--connections_mode' in
                        sys.argv or '-cm' in sys.argv or 'neighbourhood_mode' in sys.argv or 'nm' in sys.argv)

    return parser.parse_args()


if __name__ == '__main__':
    args = define_arguments()
    start_time = datetime.datetime.now()
    print('\n ========   S T A R T E D   [{}]\n'.format(start_time.strftime("%H:%M:%S")))

    # initialize Dgraph client:
    dgraph_client = DgraphClient()
    dgraph_client.connect(ip=args.dgraph_ip, port=args.dgraph_port)

    output_path = args.output_directory + '/' + args.output_file
    print('Output file path (name) is "' + output_path + '".')

    if args.ips_mode:
        # output only IPs from dataset:
        ips_json = queries.query_get_host_ips(dgraph_client)
        if ips_json:
            output_file_name = output_path + '.csv'
            ips_csv = pandas_funcs.convert_json_to_csv_ips(ips_json)
            ips_csv.to_csv(output_file_name, index=False, header=False)
            print('Successfully wrote to file ' + output_file_name + '.')
        else:
            print('Something went wrong with trying to get the result from Dgraph.')

    elif args.neighbourhood_mode:
        # output connections of all hosts from input IPs file and their neighbourhoods:
        ips_file = open(args.ips_csv, 'r')

        with multiprocessing.Pool(processes=32) as pool:
            host_ips_list = [host_ip for host_ip in ips_file]
            pool.map(compute_and_write_host_neighbourhood, host_ips_list)
    else:
        # output connections of all hosts from input IPs file:
        ips_file = open(args.ips_csv, 'r')

        for host_ip in ips_file:
            get_host_connections(host_ip, 'originated')
            get_host_connections(host_ip, 'responded')

    finished_time = datetime.datetime.now()
    print('\n ========   F I N I S H E D   [{}]\n'.format(finished_time.strftime("%H:%M:%S")))
    print('Total time: {}\n'.format(finished_time - start_time))

