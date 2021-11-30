"""Stream type classes for tap-dg-ice."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dg_ice.client import TapDgIceStreamComplete

class NFTItems(TapDgIceStreamComplete):
    """Define custom stream."""
    name = "ice_nft_items"
    
    primary_keys = ["id"]
    object_returned = 'nftitems'
    query = """
        query ($offset: Int!)
        {
            nftitems(
                first: 1000,
                skip: $offset,
                orderBy: id,
                orderDirection:asc
            ) {
                id
                owner {
                    id
                }
                token {
                    id
                }
                tokenId
                level
            }
        }
    """

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert x/y to integers"""
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
    ).to_dict()
