# BrowserQuest Online Changelog

## v0.21.0 - 2026-03-13
- Extended progression balancing for dungeon 41+ (loot quality, enemy scaling, shop tier continuity).
- Added visible in-game version chip (`v0.21.0`) for cache/version verification.
- Added handedness HUD toggle persistence (left/right status panel).
- Added anti-clone protections for guest migration:
  - strict `guest_id` required for guest API flows,
  - retired guest identities after Google claim,
  - automatic fresh guest reset if retired guest is reused.
- Rebuilt Wiki architecture into dedicated subpages:
  - `/wiki`
  - `/wiki/progression`
  - `/wiki/items`
  - `/wiki/enemies`
  - `/wiki/formulas`
  - `/wiki/changelog`
- Added API version endpoint: `/api/game/version`.
- Expanded sitemap entries for all wiki pages.

## v0.20.0 - 2026-03-13
- Initial groundwork for 41+ content expansion.
- Added early version chip and changelog link reference.
- Added first pass of handedness UI toggle.
