# Distributed Coordination Service

## [AI Log](https://github.students.cs.ubc.ca/CPSC416-2025W-T2/pranavl/blob/main/ai/LOG.md)

## [Design/Reflection (incomplete)](https://github.students.cs.ubc.ca/CPSC416-2025W-T2/pranavl/blob/main/capstone/design.md)

## Usage

- `go run .` will start a server running on localhost:7000 and is use to manage nodes 
- Open index.html in browser and input the number of nodes
- The UI should be enough to understand how to use the system
- wipe cleans the persisted node state (ui doesn't show a message but you can take a look inside /data directory)
- Request buttons have required fields listed next to them
- Refreshing the browser requires running `go run .` again
- SIGINT will destroy all nodes and their state
