"""Stream type classes for tap-dg-ice."""

import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dg_ice.client import TapDgIceStream

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
