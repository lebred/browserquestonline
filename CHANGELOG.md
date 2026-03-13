# BrowserQuest Online Changelog

## v0.21.3 - 2026-03-13
- Wiki mobile portrait overhaul:
  - table rows now render as stacked cards on small screens,
  - each cell shows its own header label,
  - no horizontal clipping/truncation for dense data tables.
- Updated wiki section versions to `v0.21.3` for cache validation.

## v0.21.2 - 2026-03-13
- Removed top version chip from game header.
- Removed left-handed/right-handed menu toggle.
- Mobile portrait: status panel now stretches full width at top for better readability.
- Version remains visible in Journal logs and via `/api/game/version`.

## v0.21.1 - 2026-03-13
- Added a distinct `26-30` gear progression tier with new items:
  - `Lame ombrale` (weapon)
  - `Aegis ombrale` (shield)
- Updated tier loot/shop pools so floors `26-30` are no longer a flat continuation of `21-25`.
- Improved wiki mobile rendering for vertical screens:
  - fixed table layout,
  - aggressive text wrapping,
  - tighter mobile typography/padding for readability.

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
