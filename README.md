# Wikipedia data processing

Personal project to get familiar with, apply and evalutate Elixir libraries for Data Engineering tasks.

The data source is [Wikipedia's recent changes API](https://www.mediawiki.org/wiki/API:RecentChanges).

### Topology

```mermaid
flowchart LR
    A[Extractor]
    B[Storage]
    C[Flow producer]
    D[Flow stream]
    E[Broadcaster]
    F("Encoder")
    B --> F
    A --> E --> |Raw| B
    E --> C
    C --> D
    D --> |Aggregated| B
```

### Commands

Manually capture data to local storage
```elixir
iex> {:ok, pid} = Extraction.Storage.GenServer.start_link(%{storage: Extraction.Storage.S3, formatter: Extraction.Storage.Formatter.Json})
iex> Extraction.Sse.stream_sse("https://stream.wikimedia.org/v2/stream/recentchange", &Extraction.Sse.process_sse/2)
```

Run livebook
```bash
docker run --network wikipedia_data_processing_network -p 8080:8080 -p 8081:8081 --pull always -u $(id -u):$(id -g) -v $(pwd)/data_exploration:/data ghcr.io/livebook-dev/livebook
```

Garage
```
node_id=$(docker exec -ti garage /garage status | awk 'END {print $1}')
docker exec -it garage /garage layout assign -z dc1 -c 1G $node_id
docker exec -it garage /garage layout apply --version 1
docker exec -it garage /garage bucket create testbucket
docker exec -it garage /garage key create testbucket_key
docker exec -it garage /garage bucket allow --read --write --owner testbucket --key testbucket_key
```


### Known issues

Connection gets dropped reliably every 15 minutes until Req runs out of retries.
```elixir
23:46:31.845 [warning] retry: got exception, will retry in 1000ms, 3 attempts left

23:46:31.847 [warning] ** (Req.TransportError) socket closed

00:01:33.768 [warning] retry: got exception, will retry in 2000ms, 2 attempts left

00:01:33.768 [warning] ** (Req.TransportError) socket closed

00:16:36.754 [warning] retry: got exception, will retry in 4000ms, 1 attempt left

00:16:36.754 [warning] ** (Req.TransportError) socket closed
** (Req.TransportError) socket closed
    (req 0.5.10) lib/req.ex:1121: Req.request!/2
    iex:1: (file)
```