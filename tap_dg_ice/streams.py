"""Stream type classes for tap-dg-ice."""

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
                    id
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