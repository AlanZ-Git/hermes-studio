# Candidate chat defaults

New chats use Hermes when no valid saved agent preference exists or browser storage
is unavailable. Explicit saved choices (including Ekko and coding agents) remain
unchanged. Picker order for group chats and workflows is not a default policy.
Existing sessions keep their persisted engine; there is no Ekko-to-Hermes migration.

The independent Ekko engine is disabled unless the server is started with exactly:

    HERMES_WEB_UI_ENABLE_EKKO_AGENT=1

This single process-level opt-in gates both startup setup/MCP injection and lazy
setup used by chat, group chat, workflows, configuration, memory and skill APIs.
Without it, Studio does not initialize Ekko configuration, memory/conversation DBs
or skill trees. Calls that require Ekko fail with the opt-in instruction, rather
than silently initializing the engine or changing an existing session to Hermes.
The source package, shared model/log adapters and build-time assets remain present.
Low-level library fixtures supplied their own setup are separate from Studio's
managed singleton.

This change does not change workflow scheduling, LAN discovery, Hermes native
loops, Cron/Kanban, or existing Hermes personal/group chat and approval wiring.
It is a source candidate default, not a production configuration or deployment.
