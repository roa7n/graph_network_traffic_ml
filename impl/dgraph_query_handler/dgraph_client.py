#! /usr/bin/env python3
# -*- coding: utf-8 -*-


import pydgraph  # official communication module for Dgraph database


class DgraphClient:
    """
    The main Dgraph client class allowing to connect to the database and perform queries.

    :ivar client_stub: PyDgraph client to store connection details
    :ivar dgraph: initialized PyDgraph client object
    """

    def __init__(self):
        self.client_stub = None
        self.dgraph = None

    def connect(self, ip: str, port: int):
        """
        Establish connection to Dgraph database server.

        :param ip: IP address of the Dgraph server.
        :param port: Port of the Dgraph server.
        :raises: ConnectionError if connection was not established.
        """
        # destroy previous Dgraph connection:
        if self.client_stub:
            self.client_stub.close()

        # initialize Dgraph server connection (set GRPC with maximum values):
        self.client_stub = pydgraph.DgraphClientStub('{0}:{1}'.format(ip, port), options=[
            ('grpc.max_send_message_length', 1024 * 1024 * 1024),
            ('grpc.max_receive_message_length', 1024 * 1024 * 1024)
        ])
        self.dgraph = pydgraph.DgraphClient(self.client_stub)

    def query(self, query: str, variables: dict = None) -> str:
        """
        Perform query using established Dgraph connection.

        :param query: query string to perform
        :param variables: dictionary with variable values
        :return: obtained response as a JSON string
        :raises: RuntimeError if database is not connected or the transaction fails
        """
        # check if the database connection is initialized:
        if not self.dgraph:
            raise RuntimeError('Dgraph database is not connected.')

        try:
            txn = self.dgraph.txn(read_only=True)
            result = txn.query(query, variables)
        except Exception as e:
            raise RuntimeError('Dgraph query failed: ' + str(e))
        finally:
            txn.discard()

        return result.json


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
