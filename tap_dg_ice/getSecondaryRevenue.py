#!/usr/bin/python

from web3 import Web3
from web3.exceptions import TransactionNotFound
import backoff
from hexbytes import HexBytes

MATIC_URL = 'https://polygon-rpc.com/'
w3 = Web3(Web3.HTTPProvider(MATIC_URL))
DG_WALLET = HexBytes('0x0000000000000000000000007a61a0ed364e599ae4748d1ebe74bf236dd27b09')
ETH_MULTIPLIER = 1e18
emptyData = {'paymentTokenAmount': 0, 'paymentTokenAddress': None}

class GetRevenueException(Exception):
     pass

@backoff.on_exception(backoff.expo,
                      (TransactionNotFound),
                      max_tries=10)
def getReceipts(transaction_id):
    receipts = w3.eth.get_transaction_receipt(transaction_id)
    return receipts

def getSecondaryRevenue(transaction_id):
    receipts = getReceipts(transaction_id)
    if 'status' not in receipts:
        return emptyData
    
    logs = receipts['logs']
    secondaryRevenue = 0

    dgTransactions = []
    for l in logs:
        if 'topics' in l and len(l['topics']) >=3 and l['topics'][2] == DG_WALLET:
            dgTransactions.append(l)
    if len(dgTransactions) == 0:
        return emptyData

    paymentTokenAddress = None
    for secondaryRev in dgTransactions:
        if 'address' in secondaryRev and secondaryRev['address']:
            paymentTokenAddress = secondaryRev['address']
        additionalRevenue = int(secondaryRev['data'], base=16)
        secondaryRevenue += additionalRevenue

    secondaryRevenue = secondaryRevenue / ETH_MULTIPLIER

    return {'paymentTokenAmount': secondaryRevenue, 'paymentTokenAddress': paymentTokenAddress}

