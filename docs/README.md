# Documentation

How the ouroboros ring works and how to use it. The contract itself is
[`spec/`](../spec/README.md) and [`vectors/`](../vectors/); these pages explain
it and the reference implementations, and defer to the spec wherever the two
could disagree.

| Read | For |
|---|---|
| [Architecture](architecture.md) | the participants, the memory layout, how an event travels, and where the code lives |
| [Attach and admission](attach-and-admission.md) | the handshake, who decides admission, the runtime listener and grant files, local attach, reaping |
| [Routing and flow control](routing-and-flow-control.md) | the pump, routing by pattern, correlation, backpressure, overflow and the control reserve, the tap |
| [Module calls](module-calls.md) | the AMOD envelope, the module runtime and the module caller, and how call outcomes are decided |
| [Arena payloads](arena-payloads.md) | payloads too large for a slot: leases, producing next to the host, reading from any process |
| [Capture](capture.md) | recording the tap stream, the capture format, and reading it back |
| [Implementations and conformance](implementations-and-conformance.md) | what C, Go and Rust each provide, the evidence that holds them to the contract, and adding a language |
| [Versions and releases](versions-and-releases.md) | release tags, the contract's own version numbers, and how consumers pin a release |

New to the ring: read the architecture page, then whichever of the others
matches what you are building. Implementing the ring in another language: read
the [spec](../spec/README.md) first, then
[implementations and conformance](implementations-and-conformance.md).

Diagrams are [Mermaid](https://mermaid.js.org/), which GitHub renders in place.
