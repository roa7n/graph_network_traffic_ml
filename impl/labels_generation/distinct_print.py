#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import socket


def add_count(dictionary, dict_key):
    """
        Counts the number of occurrences of dict_key string in input file using helper dictionary.

        :param dictionary: Input dictionary
        :param dict_key: Input dictionary key (string)
    """
    if dict_key:
        if dict_key not in dictionary:
            dictionary[dict_key] = 1
        else:
            dictionary[dict_key] += 1


def print_sorted_dict(dictionary):
    """
        Prints dictionary keys and values sorted by values in descending order.

        :param dictionary: Input dictionary
    """
    sorted_dictionary = {k: v for k, v in sorted(dictionary.items(), key=lambda item: item[1], reverse=True)}
    for dict_key in sorted_dictionary.keys():
        print('{}: [{}]'.format(dict_key, sorted_dictionary[dict_key]))


def define_arguments():
    """
        Add arguments to ArgumentParser (argparse) module instance.

        :return: Parsed arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('-in', '--input_file', help='Input file path', type=str,
                        default='/home/ubuntu/denca-devel/labels/test/fast.log')
    parser.add_argument('-ou', '--output_file', help='Output labels file path', type=str,
                        default='/home/ubuntu/denca-devel/labels/labels.csv')
    parser.add_argument('-m', '--mode', help='Input file format mode', choices=['log', 'json'],
                        default='log')
    parser.add_argument('-g', '--generate', help='Generates output label helper file', action='store_true')

    return parser.parse_args()


if __name__ == '__main__':
    args = define_arguments()

    input_path = args.input_file
    print('Input file path is "' + input_path + '".\n')
    input_file = open(input_path, 'r')

    mode = args.mode
    generate = args.generate
    output_path = args.output_file

    if input_path.endswith(mode):
        distinct_counter = {}
        distinct_counter_event_types = {}

        if generate:
            lines_dict = {}
            labels_file = open(output_path, 'w')

        for line in input_file.readlines():

            if mode == 'log':
                # log output from either Snort or Suricata
                # file contains lines in format:
                # time [**] [.] alert text [**] [alert class] [priority] {type} IP originator -> IP responder
                line_splitted = line.split('[')
                alert = line_splitted[2]
                add_count(distinct_counter, alert)

                if generate:
                    clean_sth = [substr.strip() for substr in line_splitted]

                    # get position of IP addresses:
                    # (sometimes on fourth, sometimes on fifth index)
                    if len(clean_sth) - 1 >= 5:
                        ips = clean_sth[5]
                    else:
                        ips = clean_sth[4]

                    # extract only only IPv4 (without ports):
                    ips = ips.split('}')
                    ips = ips[1].split('->')
                    for i in range(len(ips)):
                        ip = ips[i].strip()
                        if ':' in ip:
                            splitted_ip = ip.split(':')
                            ips[i] = splitted_ip[0]

                    try:
                        socket.inet_aton(ips[0])
                        socket.inet_aton(ips[1])
                        add_count(lines_dict, '{},{},{},{}\n'.format(clean_sth[0],  # .split('.')[0] + '.0',  # time
                                                                     clean_sth[2].split(']')[1].strip()
                                                                     .replace(',', ' '),  # alert text
                                                                     ips[0],  # IP originator
                                                                     ips[1]))  # IP responder
                    except socket.error:
                        pass
                        #print('Not an IPv4 address.')

            else:
                # JSON output from Suricata
                line_json = json.loads(line)

                alert_json = line_json['alert'] if 'alert' in line_json else ''
                if alert_json:
                    signature = alert_json['signature'] if 'signature' in alert_json else ''
                    category = alert_json['category'] if 'category' in alert_json else ''
                    event = signature + ' ' + category
                    add_count(distinct_counter, event)

                event_type = line_json['event_type'] if 'event_type' in line_json else ''
                add_count(distinct_counter_event_types, event_type)

        if generate:
            for csv_line in lines_dict.keys():
                labels_file.write(csv_line)
            print('Wrote labels to file ' + output_path + '.')
            labels_file.close()

        print('ALERTS:')
        print_sorted_dict(distinct_counter)

        if mode == 'json':
            print('\nEVENT TYPES:')
            print_sorted_dict(distinct_counter_event_types)

    else:
        print('Input file is not in expected format "' + mode + '".')

    print('\nDONE')
