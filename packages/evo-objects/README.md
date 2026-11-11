<p align="center"><a href="https://seequent.com" target="_blank"><picture><source media="(prefers-color-scheme: dark)" srcset="https://developer.seequent.com/img/seequent-logo-dark.svg" alt="Seequent logo" width="400" /><img src="https://developer.seequent.com/img/seequent-logo.svg" alt="Seequent logo" width="400" /></picture></a></p>
<p align="center">
    <a href="https://pypi.org/project/evo-objects/"><img alt="PyPI - Version" src="https://img.shields.io/pypi/v/evo-objects" /></a>
    <a href="https://github.com/SeequentEvo/evo-python-sdk/actions/workflows/run-all-tests.yaml"><img src="https://github.com/SeequentEvo/evo-python-sdk/actions/workflows/run-all-tests.yaml/badge.svg" alt="" /></a>
</p>
<p align="center">
    <a href="https://developer.seequent.com/" target="_blank">Seequent Developer Portal</a>
    &bull; <a href="https://community.seequent.com/group/19-evo" target="_blank">Seequent Community</a>
    &bull; <a href="https://seequent.com" target="_blank">Seequent website</a>
</p>

# Evo Object API Client

The Geoscience Object API is Seequent's next-generation cloud offering for geoscience
data, empowering our users to build responsive modern workflows. 

## Pre-requisites

* Python ^3.10
* An [application registered in Bentley](https://developer.bentley.com/register/?product=seequent-evo)

## Installation

```shell
pip install evo-objects
```

## Usage

See [the evo-sdk-common documentation](https://github.com/SeequentEvo/evo-python-sdk/blob/main/packages/evo-sdk-common/README.md)
for information on how to authenticate, then select the organisation, hub and workspace that you would like to use.

### Interacting with the Geoscience Object API

To get up and running quickly with the Evo Objects SDK, start by configuring your
[environment and API connector](https://github.com/SeequentEvo/evo-python-sdk/blob/main/packages/evo-sdk-common/docs/quickstart.md).

```python
from evo.objects import ObjectAPIClient

service_client = ObjectAPIClient(environment, connector)
service_health = await service_client.get_service_health()
service_health.raise_for_status()

# The data client is an optional utility that provides helpers for uploading and downloading
# parquet data via pyarrow.Table objects.
data_client = service_client.get_data_client(manager.cache)
...
```

Listing objects is simple, just call the `ObjectAPIClient.list_objects()` method.

```python
offset = 0
while True:
    page = await service_client.list_objects(offset=offset, limit=10)
    if offset == 0:
        print(f"Found {page.total} object{'' if page.total == 1 else 's'}")
    for object in page:
        print(f"{object.path}: <{object.schema_id}> ({object.id})")

    if page.is_last:
        break
    else:
        offset = page.next_offset
```

You can also get a list of all objects. Internally, this recursively calls the `list_objects()` method until all objects are fetched.

Check out the other methods on the `ObjectAPIClient` for more details on how to upload and download objects, or get object versions.

### Typed downhole collections

`DownholeCollection` provides a DataFrame-based API for creating and reading downhole objects. A collection can contain
distance tables, interval tables, or both. `hole_index` is an integer key in the object's `hole_id` lookup table, not a
row position or a pandas categorical code. The creation helper uses dense zero-based keys in sorted hole-ID order unless
an explicit `hole_indices` mapping is supplied. Persisted objects may use arbitrary integer keys. Collection holes may
contain a subset or repeated chunks.

```python
import pandas as pd

from evo.objects.typed import DownholeCollection, DownholeCollectionData
from evo.objects.typed.downhole_collection import IntervalCollection
from evo.objects.utils.downhole import hole_chunks_from_ids

intervals = pd.DataFrame({"from": [0.0], "to": [1.5], "lithology": ["sandstone"]})
collections = [
    IntervalCollection(
        name="geology",
        holes=hole_chunks_from_ids(pd.Series(["DH-01"])),
        table=intervals,
        unit="m",  # Collection coordinate units are set explicitly here.
    )
]

# Build DownholeCollectionData with matching path, location-hole chunks, and collar properties,
# then create it with: await DownholeCollection.create(context, data)
```

Use `await dhc.location.to_dataframe()` and `await dhc.location.path_to_dataframe()` to read collars and paths.
Distance and interval tables provide `to_dataframe()` and `to_dataframe_by_hole()`. Before reading a large object, call
`await dhc.prefetch_collections("geology")` to warm only the requested collection data (and location data by default).
Attribute descriptions for ordinary attributes round-trip through `DataFrame.attrs["attribute_descriptions"]`.
Collection coordinate units come only from `DistanceCollection.unit` or `IntervalCollection.unit`; unit metadata on the
distance or `from` column is ignored.

### Upload concurrency

Async uploads perform DataFrame/Arrow conversion and Parquet saving in the event loop's default thread pool.
Independent pairs, such as category values/lookup files and a typed table/its attributes, run concurrently.
Attribute columns, schema lists, and downhole collections are processed sequentially to avoid unrestricted file
fan-out. Results preserve input order, and table/attribute replacement is applied only after the required uploads
and conflict checks succeed. A cancelled operation waits for active preparation workers to finish.

There is no SDK-wide upload semaphore or custom thread pool. HTTP connections are limited by
`AioTransport(num_pools=...)` (default: 4; 0 disables the connection limit), separately from the thread pool.
Connection limits do not bound queued tasks or buffered file data; applications scheduling many direct uploads
concurrently should control their own file-level fan-out. Do not modify input DataFrames or clear their cache while
uploads are running.

Table, DataFrame, and category uploads retry transient HTTP failures (408, 429, 500, 502, 503, 504), raw connection
failures, and timeouts. The default is three total attempts with exponential backoff (4 then 8 seconds). Each retry
reuses the completed cached Parquet file and restarts that file's upload, not the whole collection; this also covers
upload-URL acquisition, failed chunks, and block-list commits. Progress does not move backwards across attempts.
Permanent failures, cancellation, and already-exhausted transport connection retries are not retried.
After exhaustion the last error is raised with the retry history chained as its cause, so persistent failures
still fail the operation rather than silently skipping data.

Pass `retry=Retry(logger=..., max_attempts=..., backoff_method=...)` to any of these upload methods to override the
policy. A category's values and lookup each have their own attempt budget. To avoid multiplying retry budgets, the
storage layer runs once per table attempt; transport-level connection retries remain separate and unchanged.

DataFrame uploads continue to dispatch through `upload_table` or `upload_category_table`, so those overrides still
intercept uploads. Typed table replacement stages data through `_upload` and commits through `_apply_uploaded_table`;
subclasses customizing `DataTable.from_dataframe` must move commit-time derived-metadata updates to
`_apply_uploaded_table` for them to run when the table is updated together with attributes. That hook must perform
fallible validation before changing live state.

To configure HTTP connections, pass a transport to your context using your existing authorizer, cache, and environment:

```python
from evo.aio import AioTransport
from evo.common import StaticContext

transport = AioTransport(user_agent="my-app", num_pools=8)
context = StaticContext(
    transport=transport, authorizer=authorizer, cache=cache,
    hub_url=environment.hub_url, org_id=environment.org_id, workspace_id=environment.workspace_id,
)
```

`ServiceManager` and `ServiceManagerWidget` also accept a transport in their constructors.
The `ServiceManagerWidget.with_auth_code(...)` convenience factory currently uses the default connection limit.

## Contributing

For instructions on contributing to the development of this library, please refer to the [evo-python-sdk documentation](https://github.com/seequentevo/evo-python-sdk).

## License

The Python SDK for Evo is open source and licensed under the [Apache 2.0 license.](./LICENSE.md).

Copyright © 2025 Bentley Systems, Incorporated.

Licensed under the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
