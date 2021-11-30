"""GraphQL client handling, including TapDgIceStream base class."""

import requests, backoff
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, cast

from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import GraphQLStream

RESULTS_PER_PAGE = 1000

class TapDgIceStream(GraphQLStream):
    """TapDgIce stream class."""

    is_timestamp_replication_key = True
    latest_timestamp = None
    results_count = None
    total_results_count = 0
    results_keys = set()
    dedupe = True
    onlyonerow = False

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        return headers

    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        try:
            results = resp_json["data"][self.object_returned]
            self.results_count = len(results)
            self.total_results_count += self.results_count
            for row in results:

                if self.onlyonerow == False:
                    #Update timestamp
                    if self.latest_timestamp is None or row[self.replication_key] > self.latest_timestamp:
                        self.latest_timestamp = row[self.replication_key]
                
                yield row
        except Exception as err:
            self.logger.warn(f"(stream: {self.name}) Problem with response: {resp_json}")
            raise err

    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token or self.get_starting_timestamp(partition)
        self.logger.info(f'(stream: {self.name}) Next page:{next_page_token}')

        return {
            "timestamp": int(next_page_token),
        }

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

        return None

    
    def get_next_page_token(self, response, previous_token):
        if self.results_count == 0:
            return None
        if previous_token and self.latest_timestamp == previous_token:
            return None

        return self.latest_timestamp

    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=7,
        factor=2,
    )
    def _request_with_backoff(
        self, prepared_request, context: Optional[dict]
    ) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = cast(str, prepared_request.path_url)
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        if response.status_code in [401, 403]:
            self.logger.info("Failed request for {}".format(prepared_request.url))
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )
        self.logger.debug("Response received successfully.")
        return response



class TapDgIceStreamComplete(GraphQLStream):
    """TapDgIce stream without replication key."""

    total_results_count = 0
    results_count = 0
    
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]
        
    def get_url_params(self, partition, next_page_token: Optional[th.IntegerType] = None) -> dict:
        next_page_token = next_page_token or 0
        self.logger.info(f'(stream: {self.name}) Next page:{next_page_token}')

        return {
            "offset": int(next_page_token),
        }


    def get_next_page_token(self, response, previous_token):
        if self.results_count == 0 or self.results_count < RESULTS_PER_PAGE:
            return None

        if previous_token is None:
            return RESULTS_PER_PAGE
        else:
            return previous_token + RESULTS_PER_PAGE
        

    
    def parse_response(self, response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        resp_json = response.json()
        try:
            results = resp_json["data"][self.object_returned]
            self.results_count = len(results)
            self.total_results_count += self.results_count
            for row in results:
                yield row
        except Exception as err:
            self.logger.warn(f"(stream: {self.name}) Problem with response: {resp_json}")
            raise err
    
    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=5,
        giveup=lambda e: e.response is not None and e.response.status_code >= 400,
        factor=2,
    )
    def _request_with_backoff(
        self, prepared_request, context: Optional[dict]
    ) -> requests.Response:
        response = self.requests_session.send(prepared_request)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = cast(str, prepared_request.path_url)
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        if response.status_code in [401, 403]:
            self.logger.info("Failed request for {}".format(prepared_request.url))
            self.logger.info(
                f"Reason: {response.status_code} - {str(response.content)}"
            )
            raise RuntimeError(
                "Requested resource was unauthorized, forbidden, or not found."
            )
        elif response.status_code >= 400:
            raise RuntimeError(
                f"Error making request to API: {prepared_request.url} "
                f"[{response.status_code} - {str(response.content)}]".replace(
                    "\\n", "\n"
                )
            )
        self.logger.debug("Response received successfully.")
        return response