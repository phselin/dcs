# DCS — Distributed Coordination Service

A Raft-based replicated key-value store with TTL leases, written from scratch in Go. It provides the
primitives behind real coordination services like etcd/ZooKeeper — consensus, linearizable reads,
compare-and-swap, and leases — and ships with a browser UI for spinning up a local cluster and
poking at it (killing nodes, wiping state, watching elections happen in real time).

## Architecture

```mermaid
flowchart TD
    UI[Browser UI] -- REST --> MGMT["Management server :7000<br/>spawns / kills / wipes nodes"]

    MGMT --> N1
    MGMT --> N2
    MGMT --> N3

    subgraph Cluster
        N1["node1<br/>HTTP :8001 · Raft :9001<br/>(leader)"]
        N2["node2<br/>HTTP :8002 · Raft :9002<br/>(follower)"]
        N3["node3<br/>HTTP :8003 · Raft :9003<br/>(follower)"]

        N1 <-- "RequestVote /<br/>AppendEntries" --> N2
        N2 <-- "RequestVote /<br/>AppendEntries" --> N3
        N1 <-- "RequestVote /<br/>AppendEntries" --> N3
    end

    N1 --> D1[("data/node1<br/>term · vote · log")]
    N2 --> D2[("data/node2<br/>term · vote · log")]
    N3 --> D3[("data/node3<br/>term · vote · log")]
```

Within a node, requests flow down through an HTTP façade, the Raft engine, and into the replicated
state machine:

```mermaid
flowchart TD
    A["HTTP API (httpapi/)<br/>/keys · /lease · /status · /dump<br/>forwards non-leader writes to the leader"]
    B["Raft Node (raft/*.go)<br/>election timer · log replication<br/>commit index · leadership<br/>confirmationfor linearizable reads"]
    C["KV Store + Lease Manager (statemachine.go)<br/>GET / PUT / DELETE / CAS/<br/>TTL leases that auto-expire<br/>and cascade-delete keys"]
    D[("Disk persistence (./data/&lt;id&gt;)<br/>term, voted-for and<br/>log entries survive restarts")]

    A --> B -- "committed entries" --> C --> D
```

## Design

- **Raft consensus from scratch** (`raft/`) — leader election with randomized timeouts, log
  replication with AppendEntries/heartbeats, and commit-index advancement, all driven over Go's
  `net/rpc`.
- **Disk persistence** (`raft/persist.go`) — term, vote, and log are flushed to disk per node so a
  restarted node recovers its state instead of starting fresh.
- **Linearizable reads** — the leader confirms it still holds leadership (a quorum check) before
  serving a read, so followers never see stale data through a partitioned "leader".
- **Replicated KV store with leases** (`raft/statemachine.go`) — supports `GET`/`PUT`/`DELETE` and
  atomic `CAS` (compare-and-swap), plus etcd-style TTL leases that auto-expire and cascade-delete
  their attached keys — the building blocks for distributed locks and leader election.
- **HTTP API** (`httpapi/`) — a REST layer per node that proposes commands to the Raft log and
  transparently forwards writes to the current leader.
- **Cluster simulator UI** (`frontend/`) — spins up 1–10 nodes, shows live state (term, log length,
  commit index, current leader), and lets you kill/restart/wipe individual nodes to watch the
  cluster recover.

## Usage

1. `go run .` — starts the management server on `localhost:7000` (one process hosts all simulated
   nodes; each node also gets its own Raft port `900x` and HTTP port `800x`).
2. Open `frontend/index.html` in a browser, enter a node count (1–10), and click **Start**.
3. Use the UI to issue KV/lease operations against any node — writes are forwarded to the leader
   automatically. Required fields for each operation are listed next to its button.
4. **Kill** / **Restart** simulate node crashes and recovery; **Wipe** clears a node's persisted
   state (written under `/data`).
5. `Ctrl+C` shuts down every node and removes all persisted state. Restarting the cluster requires
   re-running `go run .` and refreshing the browser.
