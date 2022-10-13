"""TapDgIce tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_dg_ice.timestamped_streams import (
    IceTransferEvents,
    InitialMintingEvent,
    UpgradeItemEvent,
    UpgradeResolvedEvents,
    NFTItems,
    SecondaryRevenueICETransfer,
)

from tap_dg_ice.complete_streams import (
    DGTokenHoldersEth,
    DGTokenHoldersPolygon,
)


STREAM_TYPES = [
    IceTransferEvents,
    InitialMintingEvent,
    UpgradeItemEvent,
    UpgradeResolvedEvents,
    NFTItems,
    DGTokenHoldersEth,
    DGTokenHoldersPolygon,
    SecondaryRevenueICETransfer,
]


class TapTapDgIce(Tap):
    """TapDgIce tap class."""
    name = "tap-dg-ice"

    config_jsonschema = th.PropertiesList(
        th.Property("start_updated_at", th.IntegerType, default=1),
        th.Property("api_url", th.StringType, default='https://api.thegraph.com/subgraphs/name/satoshi-naoki/decentralgamesice'),
        th.Property("quickswap_api_url", th.StringType, default='https://api.thegraph.com/subgraphs/name/sameepsi/quickswap06'),
        th.Property("dg_token_eth", th.StringType, default='https://api.thegraph.com/subgraphs/name/satoshi-naoki/decentral-games-ethereum'),
        th.Property("dg_token_polygon", th.StringType, default='https://api.thegraph.com/subgraphs/name/satoshi-naoki/decentral-games-polygon'),
        th.Property("secondary_revenue_graph_url", th.StringType, default='https://api.thegraph.com/subgraphs/name/tabatha-decentralgames/secondary-revenue-ice'),
    ).to_dict()


    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
