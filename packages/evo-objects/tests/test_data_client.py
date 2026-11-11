#  Copyright © 2025 Bentley Systems, Incorporated
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio
import json
import sys
import threading
from contextlib import nullcontext
from unittest import mock
from uuid import UUID, uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.testing import assert_frame_equal
from parameterized import parameterized

from data import load_test_data
from evo.aio import AioTransport
from evo.common import APIConnector, HTTPHeaderDict, HTTPResponse, IFeedback, RequestMethod
from evo.common.exceptions import BadRequestException, EvoAPIException, RetryError, TransportError
from evo.common.io.exceptions import ChunkedIOError, DataExistsError
from evo.common.test_tools import TestWithConnector, TestWithStorage
from evo.common.utils import BackoffLinear, NoFeedback, PartialFeedback, Retry, get_header_metadata
from evo.objects.endpoints.models import DataUploadResponseBody
from evo.objects.exceptions import TableFormatError
from evo.objects.utils import KnownTableFormat, ObjectDataClient, table_formats
from evo.objects.utils._concurrency import gather_cancelling
from helpers import NoImport, UnloadModule, get_sample_table_and_bytes


class TestObjectDataClient(TestWithConnector, TestWithStorage):
    def setUp(self) -> None:
        TestWithConnector.setUp(self)
        TestWithStorage.setUp(self)
        self.data_client = ObjectDataClient(environment=self.environment, connector=self.connector, cache=self.cache)
        self.setup_universal_headers(get_header_metadata(ObjectDataClient.__module__))

    def tearDown(self) -> None:
        # Clear cache between tests to avoid cached files interfering with subsequent tests.
        self.cache.clear_cache()

    @property
    def base_path(self) -> str:
        return f"geoscience-object/orgs/{self.environment.org_id}/workspaces/{self.environment.workspace_id}"

    @parameterized.expand(
        [
            ("with_explicit_table_format", True),
            ("with_implicit_table_format", False),
        ]
    )
    def test_save_table(self, _name: str, pass_table_format: bool) -> None:
        """Test saving tabular data using pyarrow."""
        with (
            mock.patch("evo.objects.utils.table_formats.get_known_format") as mock_get_known_format,
            mock.patch("evo.common.io.upload.StorageDestination") as mock_destination,
        ):
            mock_table = mock.Mock()
            mock_get_known_format.return_value = mock_known_format = mock.Mock(spec=KnownTableFormat)
            mock_known_format.save_table.return_value = mock_table_info = {}

            if pass_table_format:
                actual_table_info = self.data_client.save_table(mock_table, table_format=mock_known_format)
            else:
                actual_table_info = self.data_client.save_table(mock_table)

        if pass_table_format:
            mock_get_known_format.assert_not_called()
        else:
            mock_get_known_format.assert_called_once_with(mock_table, table_formats=None)
        mock_known_format.save_table.assert_called_once_with(
            table=mock_table, destination=self.data_client.cache_location
        )
        mock_destination.upload_file.assert_not_called()
        self.transport.assert_no_requests()
        self.assertIs(mock_table_info, actual_table_info)

    def save_table_with_table_format_list(self) -> None:
        """Test saving tabular data using pyarrow with specified table format."""
        with (
            mock.patch("evo.objects.utils.table_formats.get_known_format") as mock_get_known_format,
            mock.patch("evo.common.io.upload.StorageDestination") as mock_destination,
        ):
            sample_table, _ = get_sample_table_and_bytes(table_formats.FLOAT_ARRAY_2, 1)

            mock_known_format = mock.Mock(wraps=table_formats.FLOAT_ARRAY_2)
            mock_known_format.save_table.return_value = mock_table_info = {}
            mock_get_known_format.return_value = mock_known_format
            actual_table_info = self.data_client.save_table(
                sample_table, table_format=[table_formats.FLOAT_ARRAY_2, mock_known_format]
            )

            mock_get_known_format.assert_called_once_with(
                table_formats=[table_formats.FLOAT_ARRAY_2, mock_known_format]
            )

        mock_known_format.save_table.assert_called_once_with(
            table=sample_table, destination=self.data_client.cache_location
        )
        mock_destination.upload_file.assert_not_called()
        self.transport.assert_no_requests()
        self.assertIs(mock_table_info, actual_table_info)

    @parameterized.expand(
        [
            ("with_explicit_table_format", True),
            ("with_implicit_table_format", False),
        ]
    )
    def test_save_dataframe(self, _name: str, pass_table_format: bool) -> None:
        """Test saving tabular data using pandas."""
        with (
            mock.patch("evo.objects.utils.table_formats.get_known_format") as mock_get_known_format,
            mock.patch("evo.common.io.upload.StorageDestination") as mock_destination,
            mock.patch("pyarrow.Table") as mock_pyarrow_table,
            mock.patch("evo.objects.utils.data._get_schema_from_dataframe", return_value=None),
        ):
            mock_pyarrow_table.from_pandas.return_value = mock_table = mock.Mock()
            mock_get_known_format.return_value = mock_known_format = mock.Mock(spec=KnownTableFormat)
            mock_known_format.save_table.return_value = mock_table_info = {}

            mock_dataframe = mock.Mock()
            if pass_table_format:
                actual_table_info = self.data_client.save_dataframe(mock_dataframe, table_format=mock_known_format)
            else:
                actual_table_info = self.data_client.save_dataframe(mock_dataframe)

        if pass_table_format:
            mock_get_known_format.assert_not_called()
        else:
            mock_get_known_format.assert_called_once_with(mock_table, table_formats=None)
        mock_known_format.save_table.assert_called_once_with(
            table=mock_table, destination=self.data_client.cache_location
        )
        mock_destination.upload_file.assert_not_called()
        self.transport.assert_no_requests()
        self.assertIs(mock_table_info, actual_table_info)

    async def test_upload_referenced_data(self) -> None:
        put_data_response = load_test_data("put_data_batch.json")[:5]
        test_pointset = {
            "name": "Test Pointset",
            "uuid": None,
            "bounding_box": {"min_x": 0.0, "max_x": 0.0, "min_y": 0.0, "max_y": 0.0, "min_z": 0.0, "max_z": 0.0},
            "coordinate_reference_system": "unspecified",
            "locations": {
                "attributes": [
                    {
                        "name": f"Test Attribute {n}",
                        "nan_description": {"values": []},
                        "values": {
                            "data": data["name"],
                            "length": 0,
                            "width": 1,
                            "data_type": "float64",
                        },
                        "attribute_type": "scalar",
                    }
                    for n, data in enumerate(put_data_response[1:])
                ],
                "coordinates": {
                    "data": put_data_response[0]["name"],
                    "length": 0,
                    "width": 3,
                    "data_type": "float64",
                },
            },
            "schema": "/objects/pointset/1.1.0/pointset.schema.json",
        }

        data_by_name = {}
        for data in put_data_response:
            data_by_name[data["name"]] = data
            if data["exists"] is False:
                # Create an empty file to simulate the data being present in the cache.
                (self.data_client.cache_location / data["name"]).touch()

        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(put_data_response)),
            mock.patch("evo.common.io.upload.StorageDestination", autospec=True) as mock_destination,
        ):

            async def _mock_upload_file_side_effect(*args, **kwargs):
                filename = kwargs["filename"]
                data = data_by_name.pop(filename.name)
                actual_upload_url = await kwargs["url_generator"]()
                self.assertEqual(data["upload_url"], actual_upload_url)
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIsInstance(kwargs["fb"], PartialFeedback)

            mock_destination.upload_file.side_effect = _mock_upload_file_side_effect
            await self.data_client.upload_referenced_data(test_pointset, mock.Mock(spec=IFeedback))

        self.assert_request_made(
            method=RequestMethod.PUT,
            path=f"{self.base_path}/data",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            body=[{"name": data["name"]} for data in put_data_response[1:]] + [{"name": put_data_response[0]["name"]}],
        )

    @parameterized.expand(
        [("referenced", ConnectionError), ("table", RuntimeError), ("table_transient", ConnectionError)]
    )
    async def test_upload_failure_preserves_exception_and_identifies_data(
        self, upload_type: str, error_type: type[Exception]
    ) -> None:
        response = load_test_data("put_data_batch.json")[:1]
        name = response[0]["name"]
        (self.data_client.cache_location / name).touch()
        error = error_type("Storage unavailable")
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(response)),
            mock.patch("evo.common.io.upload.StorageDestination", autospec=True) as destination,
            mock.patch("evo.objects.utils.data.logger") as logger,
            self.assertRaises(error_type) as cm,
        ):
            destination.upload_file.side_effect = error
            if upload_type == "referenced":
                await self.data_client.upload_referenced_data({"coordinates": {"data": name}})
            else:
                table, _ = get_sample_table_and_bytes(table_formats.FLOAT_ARRAY_2, 1)
                with mock.patch.object(self.data_client, "save_table", return_value={"data": name}):
                    await self.data_client.upload_table(
                        table, retry=Retry(logger=logger, backoff_method=BackoffLinear(0))
                    )
        self.assertIs(error, cm.exception)
        context = (
            "\n".join(getattr(cm.exception, "__notes__", []))
            if sys.version_info >= (3, 11)
            else "\n".join(call.args[0] for call in logger.error.call_args_list)
        )
        self.assertIn("While uploading", context)
        self.assertIn(name, context)

    async def test_upload_url_failure_reports_upload_stage(self) -> None:
        with (
            self.transport.set_http_response(status_code=400, content='{"title": "Bad request"}'),
            mock.patch("evo.objects.utils.data.logger") as logger,
            self.assertRaises(BadRequestException) as cm,
        ):
            await self.data_client.upload_referenced_data({"coordinates": {"data": "0" * 64}})
        context = (
            "\n".join(getattr(cm.exception, "__notes__", []))
            if sys.version_info >= (3, 11)
            else logger.error.call_args.args[0]
        )
        self.assertIn("While uploading referenced data", context)

    def test_incompatible_table_is_reported_as_preparation_not_upload(self) -> None:
        with (
            mock.patch("evo.objects.utils.data.logger") as logger,
            self.assertRaises(TableFormatError) as cm,
        ):
            self.data_client.save_table(pa.table({"value": ["text"]}), table_format=table_formats.FLOAT_ARRAY_2)
        context = (
            "\n".join(getattr(cm.exception, "__notes__", []))
            if sys.version_info >= (3, 11)
            else logger.error.call_args.args[0]
        )
        self.assertIn("While preparing an Arrow table for Evo Parquet storage", context)
        self.assertNotIn("While uploading", context)
        self.transport.assert_no_requests()

    @parameterized.expand(
        [
            ("with_explicit_table_format", True),
            ("with_implicit_table_format", False),
        ]
    )
    async def test_upload_table(self, _name: str, pass_table_format: bool) -> None:
        """Test uploading tabular data using pyarrow or pandas."""
        put_data_response = load_test_data("put_data.json")
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(put_data_response)),
            mock.patch("evo.objects.utils.table_formats.get_known_format") as mock_get_known_format,
            mock.patch("evo.common.io.upload.StorageDestination", autospec=True) as mock_destination,
        ):
            mock_table = mock.Mock()
            mock_get_known_format.return_value = mock_known_format = mock.Mock(spec=KnownTableFormat)
            mock_known_format.save_table.return_value = mock_table_info = {}
            mock_table_info["data"] = mock_data_id = put_data_response[0]["name"]

            async def _mock_upload_file_side_effect(*args, **kwargs):
                expected_filename = self.data_client.cache_location / mock_data_id
                expected_upload_url = put_data_response[0].get("upload_url")
                actual_upload_url = await kwargs["url_generator"]()
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertEqual(expected_upload_url, actual_upload_url)
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])

            mock_destination.upload_file.side_effect = _mock_upload_file_side_effect

            if pass_table_format:
                actual_table_info = await self.data_client.upload_table(mock_table, table_format=mock_known_format)
            else:
                actual_table_info = await self.data_client.upload_table(mock_table)

        if pass_table_format:
            mock_get_known_format.assert_not_called()
        else:
            mock_get_known_format.assert_called_once_with(mock_table, table_formats=None)
        mock_known_format.save_table.assert_called_once_with(
            table=mock_table, destination=self.data_client.cache_location
        )
        mock_destination.upload_file.assert_called_once()
        self.assert_request_made(
            method=RequestMethod.PUT,
            path=f"{self.base_path}/data",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            body=[{"name": mock_data_id}],
        )
        self.assertIs(mock_table_info, actual_table_info)

    @parameterized.expand(
        [
            ("with_explicit_table_format", True),
            ("with_implicit_table_format", False),
        ]
    )
    async def test_upload_dataframe(self, _name: str, pass_table_format: bool) -> None:
        """Test uploading tabular data using pyarrow or pandas."""
        put_data_response = load_test_data("put_data.json")
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(put_data_response)),
            mock.patch("evo.objects.utils.table_formats.get_known_format") as mock_get_known_format,
            mock.patch("evo.common.io.upload.StorageDestination", autospec=True) as mock_destination,
            mock.patch("pyarrow.Table") as mock_pyarrow_table,
            mock.patch("evo.objects.utils.data._get_schema_from_dataframe", return_value=None),
        ):
            mock_pyarrow_table.from_pandas.return_value = mock_table = mock.Mock()
            mock_get_known_format.return_value = mock_known_format = mock.Mock(spec=KnownTableFormat)
            mock_known_format.save_table.return_value = mock_table_info = {}
            mock_table_info["data"] = mock_data_id = put_data_response[0]["name"]

            async def _mock_upload_file_side_effect(*args, **kwargs):
                expected_filename = self.data_client.cache_location / mock_data_id
                expected_upload_url = put_data_response[0].get("upload_url")
                actual_upload_url = await kwargs["url_generator"]()
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertEqual(expected_upload_url, actual_upload_url)
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])

            mock_destination.upload_file.side_effect = _mock_upload_file_side_effect

            mock_dataframe = mock.Mock()
            if pass_table_format:
                actual_table_info = await self.data_client.upload_dataframe(
                    mock_dataframe, table_format=mock_known_format
                )
            else:
                actual_table_info = await self.data_client.upload_dataframe(mock_dataframe)

        if pass_table_format:
            mock_get_known_format.assert_not_called()
        else:
            mock_get_known_format.assert_called_once_with(mock_table, table_formats=None)
        mock_known_format.save_table.assert_called_once_with(
            table=mock_table, destination=self.data_client.cache_location
        )
        mock_destination.upload_file.assert_called_once()
        self.assert_request_made(
            method=RequestMethod.PUT,
            path=f"{self.base_path}/data",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            body=[{"name": mock_data_id}],
        )
        self.assertIs(mock_table_info, actual_table_info)

    async def test_upload_table_exists(self) -> None:
        """Test uploading tabular data using pyarrow when the table exists."""
        put_data_response = load_test_data("put_data_exists.json")
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(put_data_response)),
            mock.patch("evo.objects.utils.table_formats.get_known_format") as mock_get_known_format,
            mock.patch("evo.common.io.upload.StorageDestination", autospec=True) as mock_destination,
        ):
            mock_table = mock.Mock()
            mock_get_known_format.return_value = mock_known_format = mock.Mock(spec=KnownTableFormat)
            mock_known_format.save_table.return_value = mock_table_info = {}
            mock_table_info["data"] = mock_data_id = put_data_response[0]["name"]

            async def _mock_upload_file_side_effect(*args, **kwargs):
                expected_filename = self.data_client.cache_location / mock_data_id
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])

                with self.assertRaises(DataExistsError) as cm:
                    await kwargs["url_generator"]()
                raise cm.exception  # upload_table() should catch this exception.

            mock_destination.upload_file.side_effect = _mock_upload_file_side_effect
            actual_table_info = await self.data_client.upload_table(mock_table)

        mock_get_known_format.assert_called_once_with(mock_table, table_formats=None)
        mock_known_format.save_table.assert_called_once_with(
            table=mock_table, destination=self.data_client.cache_location
        )
        mock_destination.upload_file.assert_called_once()
        self.assert_request_made(
            method=RequestMethod.PUT,
            path=f"{self.base_path}/data",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            body=[{"name": mock_data_id}],
        )
        self.assertIs(mock_table_info, actual_table_info)

    @parameterized.expand(
        [
            ("table", "upload_table", False),
            ("nested_group", "upload_table", True),
            ("dataframe", "upload_dataframe", False),
            ("category", "upload_category_dataframe", False),
        ]
    )
    async def test_cancelled_upload_waits_for_parquet_writer(self, _name, method, nested_group) -> None:
        started = asyncio.Event()
        release = threading.Event()
        finished = threading.Event()
        loop = asyncio.get_running_loop()

        def save_table(*_args, **_kwargs):
            loop.call_soon_threadsafe(started.set)
            try:
                if not release.wait(timeout=15):
                    raise TimeoutError("The test did not release the parquet writer")
                return {"data": "saved-table"}
            finally:
                finished.set()

        with (
            mock.patch.object(self.data_client, "save_table", side_effect=save_table),
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache") as upload,
        ):

            async def run():
                data = pd.DataFrame({"value": ["sand"] if "category" in method else [1.0]})
                if method == "upload_table":
                    data = pa.Table.from_pandas(data)
                operation = getattr(self.data_client, method)(data)
                if nested_group:
                    return await gather_cancelling(operation)
                return await operation

            task = asyncio.create_task(run())
            cancelled = False
            try:
                await asyncio.wait_for(started.wait(), timeout=10)
                task.cancel()
                cancelled = True
                await asyncio.sleep(0)
                self.assertFalse(task.done(), "Cancellation returned while the parquet writer was still running")
            finally:
                release.set()
                if not cancelled:
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        self.assertTrue(task.cancelled())
        self.assertTrue(finished.is_set())
        upload.assert_not_awaited()

    async def test_parquet_serialization_runs_off_the_event_loop(self) -> None:
        threads = []
        main_thread = threading.get_ident()

        def save_table(*_args):
            threads.append(threading.get_ident())
            return {"data": "saved-table"}

        with (
            mock.patch.object(self.data_client, "save_table", side_effect=save_table),
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache") as upload,
        ):
            result = await self.data_client.upload_table(pa.table({"value": [1.0]}))

        self.assertEqual({"data": "saved-table"}, result)
        self.assertEqual(1, len(threads))
        self.assertNotEqual(main_thread, threads[0])
        upload.assert_awaited_once()

    async def test_parquet_writer_failure_is_propagated_without_uploading(self) -> None:
        with (
            mock.patch.object(self.data_client, "save_table", side_effect=RuntimeError("parquet write failed")),
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache") as upload,
        ):
            with self.assertRaisesRegex(RuntimeError, "parquet write failed"):
                await self.data_client.upload_table(pa.table({"value": [1.0]}))

        upload.assert_not_awaited()

    @parameterized.expand([("plain", False), ("category", True)])
    async def test_dataframe_conversion_does_not_block_the_event_loop(self, _name, category):
        started = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        main_thread = threading.get_ident()
        worker_threads = []
        from_pandas = pa.Table.from_pandas
        frame = pd.DataFrame({"value": pd.Categorical(["sand", "clay"]) if category else [1.0, 2.0]})

        def convert(*args, **kwargs):
            worker_threads.append(threading.get_ident())
            loop.call_soon_threadsafe(started.set)
            if not release.wait(timeout=10):
                raise TimeoutError("Test did not release conversion")
            return from_pandas(*args, **kwargs)

        method = self.data_client.upload_category_dataframe if category else self.data_client.upload_dataframe
        with (
            mock.patch("evo.objects.utils.data.pa.Table", wraps=pa.Table) as arrow,
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache") as upload,
        ):
            arrow.from_pandas.side_effect = convert
            task = asyncio.create_task(method(frame))
            try:
                await asyncio.wait_for(started.wait(), timeout=2)
                self.assertFalse(task.done())
                upload.assert_not_awaited()
                self.assertNotEqual(main_thread, worker_threads[0])
                release.set()
                result = await asyncio.wait_for(task, timeout=5)
            finally:
                release.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        infos = [result["values"], result["table"]] if category else [result]
        self.assertTrue(all((self.data_client.cache_location / info["data"]).is_file() for info in infos))

    @parameterized.expand(
        [
            ("plain", "upload_dataframe", "upload_table", False),
            ("category", "upload_category_dataframe", "upload_category_table", True),
        ]
    )
    async def test_dataframe_uploads_preserve_public_table_dispatch(self, _name, method, table_method, category):
        frame = pd.DataFrame({"value": ["sand", "clay"] if category else [1.0, 2.0]})
        feedback = mock.Mock(spec=IFeedback)
        result = {"data": "intercepted"}
        with mock.patch.object(self.data_client, table_method, return_value=result) as upload:
            actual = await getattr(self.data_client, method)(frame, fb=feedback)
        self.assertIs(result, actual)
        upload.assert_awaited_once()
        self.assertIs(feedback, upload.call_args.kwargs["fb"])
        self.assertEqual(frame["value"].tolist(), upload.call_args.args[0].column(0).to_pylist())

    async def test_category_upload_preserves_public_table_dispatch(self):
        with mock.patch.object(self.data_client, "upload_table", return_value={"data": "intercepted"}) as upload:
            result = await self.data_client.upload_category_table(pa.table({"value": ["sand", "clay"]}))
        self.assertEqual(2, upload.await_count)
        self.assertEqual(
            [table_formats.INTEGER_ARRAY_1_INT32, table_formats.LOOKUP_TABLE_INT32],
            [call.kwargs["table_format"] for call in upload.call_args_list],
        )
        self.assertEqual({"values": {"data": "intercepted"}, "table": {"data": "intercepted"}}, result)

    async def test_upload_does_not_multiply_storage_retry_budget(self):
        with (
            mock.patch.object(self.data_client, "save_table", return_value={"data": "saved"}),
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache") as upload,
        ):
            await self.data_client.upload_table(pa.table({"value": [1.0]}))
        with self.assertRaises(RetryError):
            async for attempt in upload.call_args.kwargs["retry"]:
                attempt.set_exception(ConnectionError("failed"))

    @parameterized.expand(
        [
            ("408", 408),
            ("429", 429),
            ("500", 500),
            ("502", 502),
            ("503", 503),
            ("504", 504),
            ("timeout", None),
            ("connection", False),
        ]
    )
    async def test_transient_upload_retry_preserves_order_and_cached_tables(self, _name, status):
        error = (
            ConnectionError("offline")
            if status is False
            else TimeoutError("timeout")
            if status is None
            else EvoAPIException(status, "unavailable", None, None)
        )
        attempts = {}
        completed = []
        sibling_finished = asyncio.Event()

        async def upload(context, **_kwargs):
            attempts[context.name] = attempts.get(context.name, 0) + 1
            if context.name == "first" and attempts[context.name] <= 2:
                await sibling_finished.wait()
                raise error
            completed.append(context.name)
            sibling_finished.set()

        with (
            mock.patch.object(self.data_client, "save_table", side_effect=lambda table, _: {"data": table}) as save,
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", new=upload),
        ):
            retry = Retry(logger=mock.Mock(), backoff_method=BackoffLinear(0))
            results = await asyncio.wait_for(
                gather_cancelling(
                    self.data_client.upload_table("first", retry=retry),
                    self.data_client.upload_table("second", retry=retry),
                ),
                timeout=2,
            )
        self.assertEqual(["first", "second"], [result["data"] for result in results])
        self.assertEqual(["second", "first"], completed)
        self.assertEqual({"first": 3, "second": 1}, attempts)
        self.assertEqual(2, save.call_count)

    @parameterized.expand([("url", None), ("chunk", "comp=block&"), ("commit", "comp=blocklist")])
    async def test_retry_covers_real_upload_url_chunks_and_commit(self, _name, query):
        response = load_test_data("put_data.json")
        original_request = self.transport.request
        failures = 0

        async def request(**kwargs):
            nonlocal failures
            if query is not None and query in kwargs["url"] and failures < 2:
                failures += 1
                return HTTPResponse(status=503, data=b"", reason="unavailable", headers=HTTPHeaderDict())
            return await original_request(**kwargs)

        responses = [DataUploadResponseBody(**response[0])]
        with (
            self.transport.set_http_response(status_code=201, content=""),
            mock.patch.object(self.transport, "request", side_effect=request),
            mock.patch.object(self.data_client, "save_table", wraps=self.data_client.save_table) as save,
            mock.patch(
                "evo.objects.io.DataApi.put_data_in_workspace",
                side_effect=[EvoAPIException(503, "unavailable", None, None), responses] if query is None else None,
                return_value=responses,
            ) as request_url,
            mock.patch("evo.common.utils.retry.asyncio.sleep") as sleep,
        ):
            result = await self.data_client.upload_table(pa.table({"value": [1.0]}))
        self.assertEqual(2 if query is None else 3, request_url.await_count)
        self.assertEqual(0 if query is None else 2, failures)
        self.assertEqual([mock.call(4)] if query is None else [mock.call(4), mock.call(8)], sleep.call_args_list)
        self.assertTrue((self.data_client.cache_location / result["data"]).is_file())
        save.assert_called_once()

    @parameterized.expand([("url", False), ("storage", True)])
    async def test_transport_retries_are_not_multiplied_by_table_retries(self, _name, storage):
        transport = AioTransport(
            user_agent="test", max_attempts=3, backoff_method=BackoffLinear(0), close_grace_period_ms=0
        )
        client = ObjectDataClient(
            environment=self.environment,
            connector=APIConnector(base_url=self.connector.base_url, transport=transport),
            cache=self.cache,
        )
        response = load_test_data("put_data.json")
        with (
            mock.patch("aiohttp.ClientSession.request", side_effect=ConnectionError("offline")) as request,
            mock.patch(
                "evo.objects.io.DataApi.put_data_in_workspace",
                return_value=[DataUploadResponseBody(**response[0])],
            )
            if storage
            else nullcontext(),
        ):
            async with transport:
                with self.assertRaises(ChunkedIOError if storage else TransportError) as caught:
                    await client.upload_table(
                        pa.table({"value": [1.0]}),
                        retry=Retry(logger=mock.Mock(), backoff_method=BackoffLinear(0)),
                    )
        self.assertEqual(3, request.call_count)
        if storage:
            self.assertIsInstance(caught.exception.__cause__, TransportError)

    @parameterized.expand(
        [
            ("upload_table", False),
            ("upload_dataframe", False),
            ("upload_category_table", True),
            ("upload_category_dataframe", True),
        ]
    )
    async def test_public_upload_methods_honor_retry_policy(self, method, category):
        frame = pd.DataFrame({"value": ["sand", "clay"] if category else [1.0, 2.0]})
        data = frame if "dataframe" in method else pa.table({"value": frame["value"].tolist()})
        started = 0
        all_started = asyncio.Event()

        async def fail(**_kwargs):
            nonlocal started
            started += 1
            if started == (2 if category else 1):
                all_started.set()
            await asyncio.wait_for(all_started.wait(), timeout=2)
            raise EvoAPIException(503, "unavailable", None, None)

        with mock.patch(
            "evo.objects.io.ObjectDataUpload.upload_from_cache",
            side_effect=fail,
        ) as upload:
            with self.assertRaises(EvoAPIException) as caught:
                await getattr(self.data_client, method)(
                    data, retry=Retry(logger=mock.Mock(), max_attempts=1, backoff_method=BackoffLinear(0))
                )
        self.assertIsInstance(caught.exception.__cause__, RetryError)
        self.assertEqual(1, len(caught.exception.__cause__.exceptions))
        self.assertEqual(2 if category else 1, upload.await_count)

    async def test_retry_exhaustion_is_bounded_and_preserves_errors(self):
        errors = [EvoAPIException(503, f"unavailable-{i}", None, None) for i in range(3)]
        with (
            mock.patch.object(self.data_client, "save_table", return_value={"data": "saved"}) as save,
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", side_effect=errors) as upload,
            mock.patch("evo.common.utils.retry.asyncio.sleep") as sleep,
        ):
            with self.assertRaises(EvoAPIException) as caught:
                await self.data_client.upload_table(pa.table({"value": [1.0]}))
        self.assertIs(errors[-1], caught.exception)
        self.assertEqual(tuple(errors), caught.exception.__cause__.exceptions)
        self.assertEqual([mock.call(4), mock.call(8)], sleep.call_args_list)
        self.assertEqual(3, upload.await_count)
        save.assert_called_once()

    async def test_cancellation_during_backoff_stops_retries_without_blocking_other_uploads(self):
        backing_off = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def backoff(_delay):
            backing_off.set()
            await release.wait()

        async def upload(context, **_kwargs):
            calls.append(context.name)
            if context.name == "first":
                raise EvoAPIException(503, "unavailable", None, None)

        with (
            mock.patch.object(self.data_client, "save_table", side_effect=lambda table, _: {"data": table}),
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", new=upload),
            mock.patch("evo.common.utils.retry.asyncio.sleep", side_effect=backoff),
        ):
            task = asyncio.create_task(self.data_client.upload_table("first"))
            try:
                await asyncio.wait_for(backing_off.wait(), timeout=2)
                self.assertEqual({"data": "second"}, await self.data_client.upload_table("second"))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                release.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(["first", "second"], calls)

    async def test_retry_progress_is_monotonic_and_existing_data_is_success(self):
        feedback = mock.Mock(spec=IFeedback)
        calls = 0

        async def upload(**kwargs):
            nonlocal calls
            calls += 1
            kwargs["fb"].progress(0.8 if calls == 1 else 0.2)
            if calls == 1:
                raise ConnectionError("retry")
            raise DataExistsError()

        with (
            mock.patch.object(self.data_client, "save_table", return_value={"data": "saved"}) as save,
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", side_effect=upload),
        ):
            result = await self.data_client.upload_table(
                pa.table({"value": [1.0]}),
                fb=feedback,
                retry=Retry(logger=mock.Mock(), backoff_method=BackoffLinear(0)),
            )
        progress = [call.args[0] for call in feedback.progress.call_args_list]
        self.assertEqual(sorted(progress), progress)
        self.assertEqual(1, progress[-1])
        self.assertEqual({"data": "saved"}, result)
        self.assertEqual(2, calls)
        save.assert_called_once()

    def test_client_can_be_reused_across_event_loops(self):
        async def upload(**_kwargs):
            await asyncio.sleep(0)

        async def run():
            return await asyncio.gather(*(self.data_client.upload_table(str(i)) for i in range(16)))

        with (
            mock.patch.object(self.data_client, "save_table", side_effect=lambda table, _: {"data": table}),
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", side_effect=upload),
        ):
            for _ in range(2):
                result = asyncio.run(asyncio.wait_for(run(), timeout=10))
                self.assertEqual([str(i) for i in range(16)], [info["data"] for info in result])

    @parameterized.expand(
        [
            ("same_client_cancelled", "cancelled", False),
            ("same_client_failed", "failed", False),
            ("separate_clients_cancelled", "cancelled", True),
            ("separate_clients_failed", "failed", True),
        ]
    )
    async def test_identical_uploads_do_not_share_cancellation_or_failures(self, _name, outcome, separate_clients):
        started = [asyncio.Event(), asyncio.Event()]
        release = [asyncio.Event(), asyncio.Event()]
        calls = 0
        other_client = (
            ObjectDataClient(environment=self.environment, connector=self.connector, cache=self.cache)
            if separate_clients
            else self.data_client
        )
        table = pa.table({"value": [1.0]})
        table_info = self.data_client.save_table(table)

        async def upload(**_kwargs):
            nonlocal calls
            index = calls
            calls += 1
            with (self.data_client.cache_location / table_info["data"]).open("rb") as cached:
                started[index].set()
                await release[index].wait()
                if index == 0:
                    raise RuntimeError("upload failed")
                self.assertEqual([1.0], pq.read_table(cached).column(0).to_pylist())

        with mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", side_effect=upload):
            first = asyncio.create_task(self.data_client.upload_table(table))
            tasks = [first]
            try:
                await asyncio.wait_for(started[0].wait(), timeout=10)
                second = asyncio.create_task(other_client.upload_table(table))
                tasks.append(second)
                await asyncio.wait_for(started[1].wait(), timeout=10)
                if outcome == "cancelled":
                    first.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await first
                else:
                    release[0].set()
                    with self.assertRaisesRegex(RuntimeError, "upload failed"):
                        await first
                self.assertFalse(second.done())
                release[1].set()
                result = await asyncio.wait_for(second, timeout=10)
                self.assertEqual(table_info, result)
            finally:
                for event in release:
                    event.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        self.assertEqual(2, calls)

    @parameterized.expand(
        [
            ("bad_request", EvoAPIException(400, "bad request", None, None)),
            ("unauthorized", EvoAPIException(401, "unauthorized", None, None)),
            ("forbidden", EvoAPIException(403, "forbidden", None, None)),
            ("not_found", EvoAPIException(404, "not found", None, None)),
            ("transport", TransportError("already retried")),
            ("storage_transport", RetryError("storage", [TransportError("already retried")])),
            ("storage_permanent", RetryError("storage", [EvoAPIException(401, "unauthorized", None, None)])),
            (
                "storage_mixed",
                RetryError("storage", [EvoAPIException(503, "unavailable", None, None), ValueError("invalid")]),
            ),
            ("unclassified_chunk_error", ChunkedIOError("unknown cause")),
            ("invalid_data", ValueError("invalid")),
            ("cancelled", asyncio.CancelledError()),
        ]
    )
    async def test_storage_errors_and_cancellation_propagate_unchanged(self, _name, error):
        with (
            mock.patch.object(self.data_client, "save_table", return_value={"data": "saved"}),
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", side_effect=error) as upload,
        ):
            with self.assertRaises(type(error)) as caught:
                await self.data_client.upload_table(mock.Mock())
        self.assertIs(error, caught.exception)
        upload.assert_awaited_once()

    async def test_category_values_and_lookup_upload_concurrently(self) -> None:
        started = []
        all_started = asyncio.Event()
        release = asyncio.Event()

        async def upload(upload, **_kwargs):
            started.append(upload.name)
            if len(started) == 2:
                all_started.set()
            await release.wait()

        with mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache", new=upload):
            task = asyncio.create_task(self.data_client.upload_category_table(pa.table({"category": ["sand", "clay"]})))
            try:
                await asyncio.wait_for(all_started.wait(), timeout=2)
                release.set()
                result = await asyncio.wait_for(task, timeout=2)
            finally:
                release.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        self.assertCountEqual([result["values"]["data"], result["table"]["data"]], started)

    async def test_upload_dataframe_exists(self) -> None:
        """Test uploading tabular data using pandas when the table exists."""
        put_data_response = load_test_data("put_data_exists.json")
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(put_data_response)),
            mock.patch("evo.objects.utils.table_formats.get_known_format") as mock_get_known_format,
            mock.patch("evo.common.io.upload.StorageDestination", autospec=True) as mock_destination,
            mock.patch("pyarrow.Table") as mock_pyarrow_table,
            mock.patch("evo.objects.utils.data._get_schema_from_dataframe", return_value=None),
        ):
            mock_pyarrow_table.from_pandas.return_value = mock_table = mock.Mock()
            mock_get_known_format.return_value = mock_known_format = mock.Mock(spec=KnownTableFormat)
            mock_known_format.save_table.return_value = mock_table_info = {}
            mock_table_info["data"] = mock_data_id = put_data_response[0]["name"]

            async def _mock_upload_file_side_effect(*args, **kwargs):
                expected_filename = self.data_client.cache_location / mock_data_id
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])

                with self.assertRaises(DataExistsError) as cm:
                    await kwargs["url_generator"]()
                raise cm.exception  # upload_table() should catch this exception.

            mock_destination.upload_file.side_effect = _mock_upload_file_side_effect

            mock_dataframe = mock.Mock()
            actual_table_info = await self.data_client.upload_dataframe(mock_dataframe)

        mock_get_known_format.assert_called_once_with(mock_table, table_formats=None)
        mock_known_format.save_table.assert_called_once_with(
            table=mock_table, destination=self.data_client.cache_location
        )
        mock_destination.upload_file.assert_called_once()
        self.assert_request_made(
            method=RequestMethod.PUT,
            path=f"{self.base_path}/data",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            body=[{"name": mock_data_id}],
        )
        self.assertIs(mock_table_info, actual_table_info)

    @parameterized.expand(
        [
            (
                "dictionary",
                pa.table({"Category": pa.array(["A", "B", "A", "C"], type=pa.dictionary(pa.int32(), pa.string()))}),
                pa.table(
                    {"key": pa.array([0, 1, 2], type=pa.int32()), "value": pa.array(["A", "B", "C"], type=pa.string())}
                ),
                pa.table({"Category": pa.array([0, 1, 0, 2], type=pa.int32())}),
                table_formats.INTEGER_ARRAY_1_INT32,
            ),
            (
                "string",
                pa.table({"Category": pa.array(["A", "B", "A", "C"], type=pa.string())}),
                pa.table(
                    {"key": pa.array([0, 1, 2], type=pa.int32()), "value": pa.array(["A", "B", "C"], type=pa.string())}
                ),
                pa.table({"Category": pa.array([0, 1, 0, 2], type=pa.int32())}),
                table_formats.INTEGER_ARRAY_1_INT32,
            ),
            (
                "multiple_columns",
                pa.table(
                    {
                        "Category": pa.array(["A", "B", "A", "C"], type=pa.string()),
                        "Other": pa.array(["B", "C", "B", "D"], type=pa.string()),
                    }
                ),
                pa.table(
                    {
                        "key": pa.array([0, 1, 2, 3], type=pa.int32()),
                        "value": pa.array(["A", "B", "C", "D"], type=pa.string()),
                    }
                ),
                pa.table(
                    {
                        "Category": pa.array([0, 1, 0, 2], type=pa.int32()),
                        "Other": pa.array([1, 2, 1, 3], type=pa.int32()),
                    }
                ),
                table_formats.INTEGER_ARRAY_MD_INT32,
            ),
            (
                "multiple_columns_chunked",
                pa.table(
                    {
                        "Category": pa.chunked_array(
                            [["A", "B"], ["A", "C"]], type=pa.dictionary(pa.int8(), pa.string())
                        ),
                        "Other": pa.array(["B", "C", "B", "D"], type=pa.string()),
                        "Extra": pa.chunked_array([["A"], ["B", "C"], ["A"]], type=pa.string()),
                    }
                ),
                pa.table(
                    {
                        "key": pa.array([0, 1, 2, 3], type=pa.int32()),
                        "value": pa.array(["A", "B", "C", "D"], type=pa.string()),
                    }
                ),
                pa.table(
                    {
                        "Category": pa.array([0, 1, 0, 2], type=pa.int32()),
                        "Other": pa.array([1, 2, 1, 3], type=pa.int32()),
                        "Extra": pa.array([0, 1, 2, 0], type=pa.int32()),
                    }
                ),
                table_formats.INTEGER_ARRAY_MD_INT32,
            ),
        ]
    )
    async def test_upload_category_table(
        self,
        _name: str,
        table: pa.Table,
        lookup_table: pa.Table,
        values_table: pa.Table,
        values_table_format: KnownTableFormat,
    ) -> None:
        """Test uploading categorical data using pandas."""
        with (
            mock.patch.object(self.data_client, "save_table") as save,
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache") as upload,
        ):

            def side_effect(table_to_upload, table_format=None):
                return {"data": str(uuid4()), "table": table_to_upload, "table_format": table_format}

            save.side_effect = side_effect
            category_info = await self.data_client.upload_category_table(table)

        self.assertEqual(save.call_count, 2)
        self.assertEqual(upload.await_count, 2)
        self.assertEqual(category_info["table"]["table"], lookup_table)
        self.assertEqual(category_info["table"]["table_format"], table_formats.LOOKUP_TABLE_INT32)
        self.assertEqual(category_info["values"]["table"], values_table)
        self.assertEqual(category_info["values"]["table_format"], values_table_format)

    async def test_upload_category_table_error(self) -> None:
        """Test uploading categorical data raises error for unsupported types."""
        table = pa.table({"Category": pa.array([1.0, 2.0, 1.0, 3.0], type=pa.float64())})
        with self.assertRaises(TableFormatError) as cm:
            await self.data_client.upload_category_table(table)
        assert "Category columns must be of type string" in str(cm.exception)

    @parameterized.expand(
        [
            (
                "category",
                pd.DataFrame({"Category": pd.Categorical(["A", "B", "A", "C"])}),
                pa.table(
                    {"key": pa.array([0, 1, 2], type=pa.int32()), "value": pa.array(["A", "B", "C"], type=pa.string())}
                ),
                pa.table({"Category": pa.array([0, 1, 0, 2], type=pa.int32())}),
                table_formats.INTEGER_ARRAY_1_INT32,
            ),
            (
                "string",
                pd.DataFrame({"Category": ["A", "B", "A", "C"]}),
                pa.table(
                    {"key": pa.array([0, 1, 2], type=pa.int32()), "value": pa.array(["A", "B", "C"], type=pa.string())}
                ),
                pa.table({"Category": pa.array([0, 1, 0, 2], type=pa.int32())}),
                table_formats.INTEGER_ARRAY_1_INT32,
            ),
            (
                "multiple_columns",
                pd.DataFrame({"Category": ["A", "B", "A", "C"], "Other": ["B", "C", "B", "D"]}),
                pa.table(
                    {
                        "key": pa.array([0, 1, 2, 3], type=pa.int32()),
                        "value": pa.array(["A", "B", "C", "D"], type=pa.string()),
                    }
                ),
                pa.table(
                    {
                        "Category": pa.array([0, 1, 0, 2], type=pa.int32()),
                        "Other": pa.array([1, 2, 1, 3], type=pa.int32()),
                    }
                ),
                table_formats.INTEGER_ARRAY_MD_INT32,
            ),
        ]
    )
    async def test_upload_category_dataframe(
        self,
        _name: str,
        dataframe: pd.DataFrame,
        lookup_table: pa.Table,
        values_table: pa.Table,
        values_table_format: KnownTableFormat,
    ) -> None:
        """Test uploading categorical data using pandas."""
        with (
            mock.patch.object(self.data_client, "save_table") as save,
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache") as upload,
        ):

            def side_effect(table, table_format=None):
                return {"data": str(uuid4()), "table": table, "table_format": table_format}

            save.side_effect = side_effect
            category_info = await self.data_client.upload_category_dataframe(dataframe)

        self.assertEqual(save.call_count, 2)
        self.assertEqual(upload.await_count, 2)
        self.assertEqual(category_info["table"]["table"], lookup_table)
        self.assertEqual(category_info["table"]["table_format"], table_formats.LOOKUP_TABLE_INT32)
        self.assertEqual(category_info["values"]["table"], values_table)
        self.assertEqual(category_info["values"]["table_format"], values_table_format)

    async def test_upload_dataframe_converts_large_string_via_schema(self) -> None:
        """Test that upload_dataframe converts large_string columns (from pandas string[pyarrow]) to string via schema."""
        dataframe = pd.DataFrame({"col": pd.Series(["A", "B", "C"], dtype="string[pyarrow]")})
        with (
            mock.patch.object(self.data_client, "save_table", return_value={"data": "saved"}) as save,
            mock.patch("evo.objects.io.ObjectDataUpload.upload_from_cache"),
        ):
            await self.data_client.upload_dataframe(dataframe)

        table = save.call_args.args[0]
        self.assertTrue(pa.types.is_string(table.schema.field("col").type))

    async def test_download_table(self) -> None:
        """Test downloading tabular data using pyarrow."""
        get_object_response = load_test_data("get_object.json")
        object_id = UUID(int=2)
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(get_object_response)),
            mock.patch("evo.common.io.download.HTTPSource", autospec=True) as mock_source,
        ):
            mock_table_info = {
                "data": "0000000000000000000000000000000000000000000000000000000000000001",
                "length": 1,
                "width": 3,
                "data_type": "float64",
            }
            mock_data_id: str = mock_table_info["data"]
            expected_filename = self.data_client.cache_location / mock_data_id
            sample_table, payload_bytes = get_sample_table_and_bytes(
                KnownTableFormat.from_table_info(mock_table_info), 1
            )

            async def _mock_download_file_side_effect(*args, **kwargs):
                expected_download_url = get_object_response["links"]["data"][1]["download_url"]
                actual_download_url = await kwargs["url_generator"]()
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertEqual(expected_download_url, actual_download_url)
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])
                expected_filename.write_bytes(payload_bytes)

            mock_source.download_file.side_effect = _mock_download_file_side_effect
            actual_table = await self.data_client.download_table(object_id, None, mock_table_info)

        mock_source.download_file.assert_called_once()
        self.assert_request_made(
            method=RequestMethod.GET,
            path=f"{self.base_path}/objects/{object_id}",
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
        )
        self.assertEqual(sample_table, actual_table)

    async def test_download_tables_prepares_data_once(self) -> None:
        """Test that downloading multiple tables prepares all data with one metadata request."""
        object_id = UUID(int=2)
        data_id_1 = "0000000000000000000000000000000000000000000000000000000000000001"
        data_id_2 = "0000000000000000000000000000000000000000000000000000000000000002"
        table_info_1 = {"data": data_id_1, "length": 1, "width": 1, "data_type": "float64"}
        table_info_2 = {"data": data_id_2, "length": 1, "width": 1, "data_type": "float64"}
        download_1 = mock.Mock()
        download_1.name = data_id_1
        download_2 = mock.Mock()
        download_2.name = data_id_2
        table_1 = pa.table({"value": [1.0]})
        table_2 = pa.table({"value": [2.0]})

        async def download_side_effect(_download, table_info, _fb):
            return table_1 if table_info is table_info_1 else table_2

        with (
            mock.patch.object(self.data_client, "_prepare_data_downloads", new_callable=mock.AsyncMock) as prepare,
            mock.patch.object(
                self.data_client, "_download_prepared_table", new_callable=mock.AsyncMock
            ) as download_table,
        ):
            prepare.return_value = {data_id_1: download_1, data_id_2: download_2}
            download_table.side_effect = download_side_effect
            actual_tables = await self.data_client.download_tables(
                object_id, None, [table_info_1, table_info_1, table_info_2]
            )

        prepare.assert_awaited_once_with(object_id, None, [data_id_1, data_id_2])
        self.assertEqual({data_id_1: table_1, data_id_2: table_2}, actual_tables)
        self.assertEqual(2, download_table.await_count)

    async def test_download_tables_with_no_table_infos(self) -> None:
        """Test that downloading no tables does not prepare any downloads."""
        with mock.patch.object(self.data_client, "_prepare_data_downloads", new_callable=mock.AsyncMock) as prepare:
            actual_tables = await self.data_client.download_tables(UUID(int=2), None, [])

        prepare.assert_not_awaited()
        self.assertEqual({}, actual_tables)

    async def test_download_dataframe(self) -> None:
        """Test downloading tabular data using pandas."""
        get_object_response = load_test_data("get_object.json")
        object_id = UUID(int=2)
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(get_object_response)),
            mock.patch("evo.common.io.download.HTTPSource", autospec=True) as mock_source,
        ):
            mock_table_info = {
                "data": "0000000000000000000000000000000000000000000000000000000000000001",
                "length": 1,
                "width": 3,
                "data_type": "float64",
            }
            mock_data_id: str = mock_table_info["data"]
            expected_filename = self.data_client.cache_location / mock_data_id
            sample_table, payload_bytes = get_sample_table_and_bytes(
                KnownTableFormat.from_table_info(mock_table_info), 1
            )

            async def _mock_download_file_side_effect(*args, **kwargs):
                expected_download_url = get_object_response["links"]["data"][1]["download_url"]
                actual_download_url = await kwargs["url_generator"]()
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertEqual(expected_download_url, actual_download_url)
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])
                expected_filename.write_bytes(payload_bytes)

            mock_source.download_file.side_effect = _mock_download_file_side_effect
            actual_dataframe = await self.data_client.download_dataframe(object_id, None, mock_table_info)

        mock_source.download_file.assert_called_once()
        self.assert_request_made(
            method=RequestMethod.GET,
            path=f"{self.base_path}/objects/{object_id}",
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
        )
        assert_frame_equal(sample_table.to_pandas(), actual_dataframe)

    async def test_download_dataframe_optional(self) -> None:
        """Test download dataframe is not available if pandas is not installed."""
        with UnloadModule("evo.objects.utils.data"), NoImport("pandas"):
            from evo.objects.utils.data import ObjectDataClient

            client = ObjectDataClient(environment=self.environment, connector=self.connector, cache=self.cache)
            self.assertFalse(
                any(
                    (
                        hasattr(ObjectDataClient, "download_dataframe"),
                        hasattr(client, "download_dataframe"),
                    )
                ),
                "download_dataframe should not be available if pandas is missing",
            )

    async def test_download_table_already_downloaded(self) -> None:
        """Test downloading tabular data using pyarrow or pandas when the table is already downloaded."""
        get_object_response = load_test_data("get_object.json")
        object_id = UUID(int=2)
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(get_object_response)),
            mock.patch("evo.common.io.download.HTTPSource", autospec=True) as mock_source,
        ):
            mock_table_info = {
                "data": "0000000000000000000000000000000000000000000000000000000000000001",
                "length": 1,
                "width": 3,
                "data_type": "float64",
            }
            mock_data_id: str = mock_table_info["data"]
            expected_filename = self.data_client.cache_location / mock_data_id
            sample_table, payload_bytes = get_sample_table_and_bytes(
                KnownTableFormat.from_table_info(mock_table_info), 1
            )

            async def _mock_download_file_side_effect(*args, **kwargs):
                expected_download_url = get_object_response["links"]["data"][1]["download_url"]
                actual_download_url = await kwargs["url_generator"]()
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertEqual(expected_download_url, actual_download_url)
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])

            mock_source.download_file.side_effect = _mock_download_file_side_effect

            expected_filename.write_bytes(payload_bytes)
            actual_table = await self.data_client.download_table(object_id, None, mock_table_info)

        mock_source.download_file.assert_not_called()
        # the object metadata is still requested to get the initial download URL and check permissions.
        self.assert_request_made(
            method=RequestMethod.GET,
            path=f"{self.base_path}/objects/{object_id}",
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
        )
        self.transport.request.assert_called_once()  # Ensure no other requests were made.
        self.assertEqual(sample_table, actual_table)

    async def test_download_dataframe_already_downloaded(self) -> None:
        """Test downloading tabular data using pandas when the table is already downloaded."""
        get_object_response = load_test_data("get_object.json")
        object_id = UUID(int=2)
        with (
            self.transport.set_http_response(status_code=200, content=json.dumps(get_object_response)),
            mock.patch("evo.common.io.download.HTTPSource", autospec=True) as mock_source,
        ):
            mock_table_info = {
                "data": "0000000000000000000000000000000000000000000000000000000000000001",
                "length": 1,
                "width": 3,
                "data_type": "float64",
            }
            mock_data_id: str = mock_table_info["data"]
            expected_filename = self.data_client.cache_location / mock_data_id
            sample_table, payload_bytes = get_sample_table_and_bytes(
                KnownTableFormat.from_table_info(mock_table_info), 1
            )

            async def _mock_download_file_side_effect(*args, **kwargs):
                expected_download_url = get_object_response["links"]["data"][1]["download_url"]
                actual_download_url = await kwargs["url_generator"]()
                self.assertEqual(expected_filename, kwargs["filename"])
                self.assertEqual(expected_download_url, actual_download_url)
                self.assertIs(self.transport, kwargs["transport"])
                self.assertIs(NoFeedback, kwargs["fb"])

            mock_source.download_file.side_effect = _mock_download_file_side_effect

            expected_filename.write_bytes(payload_bytes)
            actual_dataframe = await self.data_client.download_dataframe(object_id, None, mock_table_info)

        mock_source.download_file.assert_not_called()
        # the object metadata is still requested to get the initial download URL and check permissions.
        self.assert_request_made(
            method=RequestMethod.GET,
            path=f"{self.base_path}/objects/{object_id}",
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
        )
        self.transport.request.assert_called_once()  # Ensure no other requests were made.
        assert_frame_equal(sample_table.to_pandas(), actual_dataframe)
