"""Stream type classes for tap-dg-ice."""

import time
import datetime
import logging
import backoff, requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dg_ice.client import TapDgIceStream
from singer_sdk.streams import RESTStream
from tap_dg_ice.getSecondaryRevenue import getSecondaryRevenue

class IceTransferEvents(TapDgIceStream):
    """Define custom stream."""
    name = "ice_level_transfer_events"
    
    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'iceLevelTransferEvents'
    query = """
    query ($timestamp: Int!)
        {
            iceLevelTransferEvents(
                first: 1000,
                    orderBy: timestamp,
                    orderDirection: asc,
                    where:{
                        timestamp_gte: $timestamp
            }) {
                id
                oldOwner{
                    address
                }
                newOwner{
                    address
                }
                tokenAddress {
                    address
                }
                tokenId
                timestamp
            }
        }
    """

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("tokenId", th.StringType),
        th.Property("timestamp", th.IntegerType),
        th.Property("oldOwner", th.ObjectType(
            th.Property("address", th.StringType),
        )),
        th.Property("newOwner", th.ObjectType(
            th.Property("address", th.StringType),
        )),
        th.Property("tokenAddress", th.ObjectType(
            th.Property("address", th.StringType),
        ))
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert body shape variables"""
        row['timestamp'] = int(row['timestamp'])
        return row


class InitialMintingEvent(TapDgIceStream):
    """Define custom stream."""
    name = "ice_initial_minting_event"
    
    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'initialMintingEvents'
    query = """
    query ($timestamp: Int!)
        {
            initialMintingEvents(
                first: 1000,
                    orderBy: timestamp,
                    orderDirection: asc,
                    where:{
                        timestamp_gte: $timestamp
            }) {
                    id
                    tokenId
                    mintCount
                    tokenOwner {
                        id
                    }
                    timestamp
            }
        }
    """

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("tokenId", th.StringType),
        th.Property("timestamp", th.IntegerType),
        th.Property("tokenOwner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("mintCount", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert body shape variables"""
        row['timestamp'] = int(row['timestamp'])
        return row


class UpgradeItemEvent(TapDgIceStream):
    """Define custom stream."""
    name = "ice_upgrade_item_event"
    
    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'upgradeItemEvents'
    query = """
    query ($timestamp: Int!)
        {
            upgradeItemEvents(
                first: 1000,
                    orderBy: timestamp,
                    orderDirection: asc,
                    where:{
                        timestamp_gte: $timestamp
            }) {
                    id
                    itemId
                    issuedId
                    tokenOwner{
                        id
                    }
                    tokenId
                        tokenAddress{
                        address
                    }
                    requestIndex
                    timestamp
            }
        }
    """

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("itemId", th.StringType),
        th.Property("issuedId", th.StringType),
        th.Property("tokenId", th.StringType),
        th.Property("timestamp", th.IntegerType),
        th.Property("tokenOwner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("tokenAddress", th.ObjectType(
            th.Property("address", th.StringType),
        )),
        th.Property("requestIndex", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert body shape variables"""
        row['timestamp'] = int(row['timestamp'])
        return row



class UpgradeResolvedEvents(TapDgIceStream):
    """Define custom stream."""
    name = "ice_upgrade_resolved_events"
    
    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'upgradeResolvedEvents'
    query = """
    query ($timestamp: Int!)
        {
            upgradeResolvedEvents(
                first: 1000,
                    orderBy: timestamp,
                    orderDirection: asc,
                    where:{
                        timestamp_gte: $timestamp
            }) {
                    id
                    newItemId
                    newTokenId
                    tokenOwner {
                    id
                    }
                    tokenAddress {
                    address
                    }
                    timestamp
            }
        }
    """

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("newItemId", th.StringType),
        th.Property("newTokenId", th.StringType),
        th.Property("timestamp", th.IntegerType),
        th.Property("tokenOwner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("tokenAddress", th.ObjectType(
            th.Property("address", th.StringType),
        )),
    ).to_dict()


    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert body shape variables"""
        row['timestamp'] = int(row['timestamp'])
        return row


class NFTItems(TapDgIceStream):
    """Define custom stream."""
    name = "nft_items"


    primary_keys = ["id"]
    replication_key = 'createdAt'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'nftitems'
    query = """
        query ($timestamp: Int!)
        {
            nftitems(
                first: 1000,
                    orderBy: createdAt,
                    orderDirection: asc,
                    where:{
                        createdAt_gte: $timestamp
            }) {
                id
                owner {
                    id
                }
                token {
                    id
                }
                tokenId
                level
                createdAt
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert level to integer"""
        row['level'] = int(row['level'])
        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("owner", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("token", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("tokenId", th.StringType),
        th.Property("level", th.IntegerType),
        th.Property("createdAt", th.IntegerType),
    ).to_dict()


class IceUSDCPAir(TapDgIceStream):
    """Define custom stream."""
    name = "ice_usdc_pair"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["quickswap_api_url"]
    
    primary_keys = ["id"]
    replication_key = 'date'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'pairDayDatas'
    query = """
    query ($timestamp: Int!)
        {
            pairDayDatas(
                first:1000,
                orderBy: date,
                orderDirection: asc,
                where:{
                    pairAddress:"0x9e3880647c07ba13e65663de29783ecd96ec21de",
                    date_gte: $timestamp
                })
            {
                id
                date
                reserve0
                reserve1
                totalSupply
                reserveUSD
                dailyVolumeUSD
                dailyVolumeToken0
                dailyVolumeToken1
                dailyTxns
            }
        }
    """


    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Generate row id"""
        row['date'] = int(row['date'])

        return row

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("date", th.IntegerType),
        th.Property("reserve0", th.StringType),
        th.Property("reserve1", th.StringType),
        th.Property("totalSupply", th.StringType),
        th.Property("reserveUSD", th.StringType),
        th.Property("dailyVolumeUSD", th.StringType),
        th.Property("dailyVolumeToken0", th.StringType),
        th.Property("dailyVolumeToken1", th.StringType),
        th.Property("dailyTxns", th.StringType)
    ).to_dict()

class SecondaryRevenueICETransfer(TapDgIceStream):
    """Define custom stream."""
    name = "secondary_revenue_ice_transfer"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["secondary_revenue_graph_url"]
    

    def get_starting_timestamp(
        self, context: Optional[dict]
    ) -> Optional[int]:
        """Return `start_date` config, or state if using timestamp replication."""
        if self.is_timestamp_replication_key:
            replication_key_value = self.get_starting_replication_key_value(context)
            if replication_key_value:
                return replication_key_value

        if "start_updated_at" in self.config:
            return self.config["start_updated_at"]

        return 0


    primary_keys = ["id"]
    replication_key = 'timestamp'
    replication_method = "INCREMENTAL"
    is_sorted = True
    object_returned = 'transferEvents'
    query = """
    query ($timestamp: Int!)
        {
            transferEvents(
                first:1000,
                orderBy: timestamp,
                orderDirection: asc,
                where:{
                    timestamp_gte: $timestamp
                }) {
                id
                to {
                    id
                }
                from {
                    id
                }
                tokenId
                tokenAddress
                value
                contractAddress
                blockNumber
                timestamp
                isICE
            }

        }

        
    """


    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Generate row id"""
        row['timestamp'] = int(row['timestamp'])
        row['blockNumber'] = int(row['blockNumber'])

        return row



    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "transactionId": record['id'],
            "value": record['value']
        }

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("timestamp", th.IntegerType),
        th.Property("tokenId", th.StringType),
        th.Property("tokenAddress", th.StringType),
        th.Property("contractAddress", th.StringType),
        th.Property("blockNumber", th.IntegerType),
        th.Property("value", th.StringType),
        th.Property("isICE", th.BooleanType),
         th.Property("from", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("to", th.ObjectType(
            th.Property("id", th.StringType),
        )),
    ).to_dict()

class SecondaryRevenueICETransferDetails(RESTStream):
    """Define custom stream."""
    name = "secondary_revenue_ice_transfer_details"


    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["secondary_revenue_api_url"]
    path = "/ice/secondaryRevenue"

    # Child stream
    parent_stream_type = SecondaryRevenueICETransfer

    primary_keys = ["id"]
    replication_key = 'id'
    replication_method = "INCREMENTAL"
    ignore_parent_replication_keys = True


    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        row = getSecondaryRevenue(context["transactionId"])
        row = self.post_process(row, context)
        yield row

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Add hash"""
        row['id'] = context['transactionId']
        row['paymentTokenAmount'] = str(row['paymentTokenAmount'])
        return row

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {"transactionId": context['transactionId']}            


    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("contractAddress", th.StringType),
        th.Property("paymentTokenAddress", th.StringType),
        th.Property("paymentTokenAmount", th.StringType),
    ).to_dict()