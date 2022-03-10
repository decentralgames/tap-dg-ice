"""Stream type classes for tap-dg-ice."""

import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dg_ice.client import TapDgIceStreamByKey

class DGTokenHoldersEth(TapDgIceStreamByKey):
    """Define custom stream."""
    name = "dg_token_holders_ethereum"


    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["dg_token_eth"]
    
    primary_keys = ["id"]
    incremental_key = 'id'
    initial_key = '0x'
    object_returned = 'balances'
    query = """
    query ($key: String!)
        {
            balances(
                first: 1000,
                    orderBy: id,
                    orderDirection: asc,
                    where:{
                        id_gt: $key
            }) {
            id
            account {
                id
            }
            token {
                id
            }
            balance
        }
        }
    """

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("account", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("token", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("balance", th.StringType)
    ).to_dict()


class DGTokenHoldersPolygon(TapDgIceStreamByKey):
    """Define custom stream."""
    name = "dg_token_holders_polygon"


    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["dg_token_polygon"]
    
    primary_keys = ["id"]
    incremental_key = 'id'
    initial_key = '0x'
    object_returned = 'balances'
    query = """
    query ($key: String!)
        {
            balances(
                first: 1000,
                    orderBy: id,
                    orderDirection: asc,
                    where:{
                        id_gt: $key
            }) {
            id
            account {
                id
            }
            token {
                id
            }
            balance
        }
        }
    """

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("account", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("token", th.ObjectType(
            th.Property("id", th.StringType),
        )),
        th.Property("balance", th.StringType)
    ).to_dict()

