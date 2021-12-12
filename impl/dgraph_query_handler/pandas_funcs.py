#! /usr/bin/env python3
# -*- coding: utf-8 -*-


import orjson as json
import pandas as pd
from itertools import groupby


def concat_multiple_dfs(dfs):
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.append(dfs[i])
        print('Concatenating df ' + str(i) + '.')
    return df


def convert_json_to_csv_ips(json_input):
    df = json.loads(json_input)
    json_result = df['queryHosts']
    return pd.json_normalize(json_result)


def get_app_data_value(app_data, value_string):
    return app_data[value_string] if value_string in app_data else None


def canonicalize_dict(x):
    return sorted(x.items(), key=lambda x: hash(x[0]))


def unique_and_count(lst):
    grouper = groupby(sorted(map(canonicalize_dict, lst)))
    return [dict(k + [("count", len(list(g)))]) for k, g in grouper]


def convert_json_to_csv_conns(json_input, mode):
    # set JSON query objects name according to mode (reflects same strings as used in the query definition)
    if mode == 'originated':
        query_name = 'queryHostOriginated'
        conns_edge_direction = 'host.originated'
        reverse_edge_direction = '~host.responded'

    else:
        query_name = 'queryHostResponded'
        conns_edge_direction = 'host.responded'
        reverse_edge_direction = '~host.originated'

    df = json.loads(json_input)
    loaded_host_ip = pd.json_normalize(df[query_name])
    loaded_host_originated = pd.json_normalize(data=df[query_name], record_path=conns_edge_direction)
    loaded_host_originated = loaded_host_originated.drop(reverse_edge_direction, 1)
    loaded_host_responded = pd.json_normalize(data=df[query_name], record_path=[conns_edge_direction,
                                                                                reverse_edge_direction])
    joined1 = pd.concat([loaded_host_originated, loaded_host_responded], axis=1)
    n_repeat = joined1.shape[0]
    new_df = loaded_host_ip['originated_ip'].to_frame()
    new_df = new_df.loc[new_df.index.repeat(n_repeat)].reset_index(drop=True)
    final = pd.concat([new_df, joined1], axis=1)

    # app data counts from connection.produced:
    final['dns_count'] = 0
    final['ssh_count'] = 0
    final['http_count'] = 0
    final['ssl_count'] = 0
    final['files_count'] = 0

    app_data_names = ['dns', 'ssh', 'http', 'ssl', 'files']

    # app data concrete attribute values for similarity computation:
    final['dns_qtype'] = ''
    final['dns_rcode'] = ''
    final['ssh_auth_attempts'] = ''
    final['ssh_host_key'] = ''
    final['http_method'] = ''
    final['http_status_code'] = ''
    final['http_user_agent'] = ''
    final['ssl_version'] = ''
    final['ssl_cipher'] = ''
    final['ssl_curve'] = ''
    final['ssl_validation_status'] = ''
    final['files_source'] = ''
    final['file_md5'] = ''

    # app data in one column (for dev purposes right now):
    final['dns_dicts'] = ''
    final['ssh_dicts'] = ''
    final['http_dicts'] = ''
    final['ssl_dicts'] = ''
    final['files_dicts'] = ''

    if 'connection.produced' in final:
        # https://stackoverflow.com/questions/23330654/update-a-dataframe-in-pandas-while-iterating-row-by-row
        for i, row in final.iterrows():
            if isinstance(row['connection.produced'], list):
                dns_qtypes = set()
                dns_rcodes = set()
                ssh_auth_attempts = set()
                ssh_host_keys = set()
                http_methods = set()
                http_status_codes = set()
                http_user_agents = set()
                ssl_versions = set()
                ssl_ciphers = set()
                ssl_curves = set()
                ssl_validation_status = set()
                files_sources = set()
                file_md5s = set()

                dns_dict = []
                ssh_dict = []
                http_dict = []
                ssl_dict = []
                files_dict = []

                for app_data in row['connection.produced']:
                    app_data_name = str(app_data['type'][0]).lower()

                    if app_data_name in app_data_names:
                        final.at[i, app_data_name + '_count'] += 1

                        # values of keys in specified app data:

                    if app_data_name == 'dns':
                        dns_qtypes.add(get_app_data_value(app_data, 'dns.qtype'))
                        dns_rcodes.add(get_app_data_value(app_data, 'dns.rcode'))
                        # TODO: query substring, AA, (TC has only false), RD, RA, Z?

                        temp_dict = {'dns.qtype': get_app_data_value(app_data, 'dns.qtype'),
                                     'dns.rcode': get_app_data_value(app_data, 'dns.rcode')}
                        dns_dict.append(temp_dict)

                    elif app_data_name == 'ssh':
                        ssh_auth_attempts.add(get_app_data_value(app_data, 'ssh.auth_attempts'))
                        ssh_host_keys.add(get_app_data_value(app_data, 'ssh.host_key'))

                        temp_dict = {'ssh.auth_attempts': get_app_data_value(app_data, 'ssh.auth_attempts'),
                                     'ssh.host_key': get_app_data_value(app_data, 'ssh.host_key')}
                        ssh_dict.append(temp_dict)
                    elif app_data_name == 'http':
                        http_methods.add(get_app_data_value(app_data, 'http.method'))
                        http_status_codes.add(get_app_data_value(app_data, 'http.status_code'))
                        http_user_agents.add(get_app_data_value(app_data, 'http.user_agent'))
                        # TODO: trans_depth (interval), _body_len, resp_mime_types?

                        temp_dict = {'http.method': get_app_data_value(app_data, 'http.method'),
                                     'http.status_code': get_app_data_value(app_data, 'http.status_code'),
                                     'http.user_agent': get_app_data_value(app_data, 'http.user_agent')}
                        http_dict.append(temp_dict)
                    elif app_data_name == 'ssl':
                        ssl_versions.add(get_app_data_value(app_data, 'ssl.version'))
                        ssl_ciphers.add(get_app_data_value(app_data, 'ssl.cipher'))
                        ssl_curves.add(get_app_data_value(app_data, 'ssl.curve'))
                        ssl_validation_status.add(get_app_data_value(app_data, 'ssl.validation_status'))
                        # TODO: server_name substr, cipher redo (TLS_{sth from here}_..)?

                        temp_dict = {'ssl.version': get_app_data_value(app_data, 'ssl.version'),
                                     'ssl.cipher': get_app_data_value(app_data, 'ssl.cipher'),
                                     'ssl.curve': get_app_data_value(app_data, 'ssl.curve'),
                                     'ssl.validation_status': get_app_data_value(app_data, 'ssl.validation_status')}
                        ssl_dict.append(temp_dict)
                    elif app_data_name == 'files':
                        files_sources.add(get_app_data_value(app_data, 'files.source'))
                        # TODO: mime_type, local_orig, is_orig, timedout?

                        files_list = app_data['files.fuid']
                        for file in files_list:
                            file_md5s.add(file['file.md5'])

                        temp_dict = {'files.source': get_app_data_value(app_data, 'files.source'),
                                     'file.md5s': file_md5s}
                        files_dict.append(temp_dict)

                final.at[i, 'dns_qtype'] = list(dns_qtypes)
                final.at[i, 'dns_rcode'] = list(dns_rcodes)
                final.at[i, 'ssh_auth_attempts'] = list(ssh_auth_attempts)
                final.at[i, 'ssh_host_key'] = list(ssh_host_keys)
                final.at[i, 'http_method'] = list(http_methods)
                final.at[i, 'http_status_code'] = list(http_status_codes)
                final.at[i, 'http_user_agent'] = list(http_user_agents)
                final.at[i, 'ssl_version'] = list(ssl_versions)
                final.at[i, 'ssl_cipher'] = list(ssl_ciphers)
                final.at[i, 'ssl_curve'] = list(ssl_curves)
                final.at[i, 'ssl_validation_status'] = list(ssl_validation_status)
                final.at[i, 'files_source'] = list(files_sources)
                final.at[i, 'file_md5'] = list(file_md5s)

                dns_dict = unique_and_count(dns_dict)
                final.at[i, 'dns_dicts'] = dns_dict
                ssh_dict = unique_and_count(ssh_dict)
                final.at[i, 'ssh_dicts'] = ssh_dict
                http_dict = unique_and_count(http_dict)
                final.at[i, 'http_dicts'] = http_dict
                ssl_dict = unique_and_count(ssl_dict)
                final.at[i, 'ssl_dicts'] = ssl_dict
                files_dict = unique_and_count(files_dict)
                final.at[i, 'files_dicts'] = files_dict
            else:
                final.at[i, 'dns_qtype'] = []
                final.at[i, 'dns_rcode'] = []
                final.at[i, 'ssh_auth_attempts'] = []
                final.at[i, 'ssh_host_key'] = []
                final.at[i, 'http_method'] = []
                final.at[i, 'http_status_code'] = []
                final.at[i, 'http_user_agent'] = []
                final.at[i, 'ssl_version'] = []
                final.at[i, 'ssl_cipher'] = []
                final.at[i, 'ssl_curve'] = []
                final.at[i, 'ssl_validation_status'] = []
                final.at[i, 'files_source'] = []
                final.at[i, 'file_md5'] = []

                final.at[i, 'dns_dicts'] = []
                final.at[i, 'ssh_dicts'] = []
                final.at[i, 'http_dicts'] = []
                final.at[i, 'ssl_dicts'] = []
                final.at[i, 'files_dicts'] = []

        final = final.drop('connection.produced', 1)

    return final

