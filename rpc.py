#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Copyright 2014 Bernhard Haslhofer

    Bitcoin RPC interface.

"""

import requests
import json
import time
import datetime as dt


class JSONRPCException(Exception):
    pass


class JSONRPCProxy:
    """A generic JSOWN RPC interface with keep-alive session reuse"""
    def __init__(self, url):
        self._session = requests.Session()
        self._url = url
        self._headers = {'content-type': 'application/json'}

    def _call(self, rpcMethod, *params):
        payload = json.dumps({"method": rpcMethod, "params": list(params),
                              "jsonrpc": "2.0"})
        tries = 10
        hadConnectionFailures = False
        while True:
            try:
                response = self._session.get(self._url, headers=self._headers,
                                             data=payload)
            except requests.exceptions.ConnectionError:
                tries -= 1
                if tries == 0:
                    raise JSONRPCException('Failed to connect for RPC call.')
                hadConnectionFailures = True
                print("Couldn't connect for remote procedure call.",
                      "will sleep for ten seconds and then try again...")
                time.sleep(10)
            else:
                if hadConnectionFailures:
                    print("Connected for RPC call after retry.")
                break
        if not response.status_code in (200, 500):
            raise JSONRPCException("RPC connection failure: " +
                                   str(response.status_code) + ' ' +
                                   response.reason)
        responseJSON = response.json()
        if 'error' in responseJSON and responseJSON['error'] is not None:
            raise JSONRPCException('Error in RPC call: ' +
                                   str(responseJSON['error']))
        return responseJSON['result']


class BitcoinProxy(JSONRPCProxy):
    """Proxy to Bitcoin RPC Service implementing Bitcoin client call list
    https://en.bitcoin.it/wiki/Original_Bitcoin_client/API_Calls_list"""
    def __init__(self, url):
        super().__init__(url)

    def getblock(self, hash):
        """Returns information about the block with the given hash."""
        r = self._call('getblock', hash)
        r['time'] = dt.datetime.fromtimestamp(
            int(r['time'])).strftime('%Y-%m-%d %H:%M:%S')
        r['height'] = int(r['height'])
        return r

    def getblockcount(self):
        """Returns the number of blocks in the longest block chain."""
        r = self._call('getblockcount')
        return int(r)

    def getblockhash(self, index):
        """Returns hash of block in best-block-chain at <index>"""
        r = self._call('getblockhash', index)
        return r

    def getinfo(self):
        """Returns an object containing various state info."""
        r = self._call('getinfo')
        return r

    def getrawtransaction(self, txid, verbose=1):
        """Returns raw transaction representation for given transaction id."""
        r = self._call('getrawtransaction', txid, verbose)
        return r


if __name__ == '__main__':
    bcProxy = BitcoinProxy('http://bitcoinrpc:pass@localhost:8332/')
    try:
        #response = bcProxy.getblockcount()
        bh = bcProxy.getblockhash(1)
        print(bh)
        block = bcProxy.getblock(bh)
        print(block)
        transaction = bcProxy.getrawtransaction(block['tx'][0])
        print(transaction)
    except Exception as err:
        print('Ooops...RPC Exception:', err)