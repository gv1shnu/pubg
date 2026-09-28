# Version policy

`VERSION` is the product version. Current version: **0.1.0-dev.2**, a locally tested development prerelease, not a stable release. Use Semantic Versioning: patch for compatible corrections, minor for new compatible features, major for breaking application interfaces after 1.0. Before 1.0, document every breaking change explicitly.

Release workflow: update VERSION, Maven project version and CHANGELOG together; run runtime acceptance checks; record evidence; only then create an authorized annotated tag `vX.Y.Z`. Never imply tests passed through a version label. The owner authorized development commits and pushes to dev on 2026-09-28. Do not tag a stable release or push changes to main without a separate instruction.

Event contract versioning is separate: `schema_version=1` and `*.v1` topics. Breaking wire changes require v2 schemas/topics and an explicit migration. SQL migrations are numbered and repeatable; subsequent incompatible changes get a new migration file instead of silently changing a deployed contract. Raw source event identity remains immutable across archive replay.

Container base images and direct dependencies are pinned. Transitive Python dependencies are resolver-selected within pinned top-level packages; this is not a complete supply-chain lock. Capture resolved package lists and image digests in runtime acceptance evidence before release.
