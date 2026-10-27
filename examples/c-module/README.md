# External C module template

Copy this directory into a module's own repository. It consumes the installed
`ouroboros-ring` package and links only `ouroboros-ring-client`; it does not link
the host, routing, arena or capture code.

The host's module launcher supplies the local attach socket and an opaque
two-part principal identity. The host authenticates that identity and binds the
module's declared publish/subscribe/serve kinds in its admission hook before
granting any mappings. The attach socket is closed immediately after the
handshake; events then use the module's shared-memory queue pair.

The example supplies only a stage map and handler. The ring's module runtime
owns the attach loop, module envelope, correlation, heartbeats, deadlines,
cancellation and shutdown. Production handlers should call
`ouro_module_invocation_cancelled()` between bounded units of expensive work.
