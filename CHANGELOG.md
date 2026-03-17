# BrowserQuest Online Changelog

## v0.23.0 - 2026-03-16
- Added two advanced enemy archetypes:
  - `Sentinel` (fast laser caster, floor 46+)
  - `Hydra` (triple-shot apex caster, floor 56+)
- Hardened UI text rendering:
  - escaped chat messages,
  - escaped leaderboard names,
  - escaped journal lines,
  - sanitized chat input on the server before storage.
- Updated wiki/blog/sitemap content to reflect the level-60 route and the new deep-floor enemy bands.
- Added root URL and `lastmod` entries to the sitemap.

## v0.22.0 - 2026-03-16
- Extended the BrowserQuest item ladder, shop tiers, and loot pools all the way through floors `76-80`, giving the game a real level-60 progression route plus one stretch endgame tier.
- Added seven new late-game gear lines: `Zenith`, `Gravitation`, `Seraphic`, `Cataclysm`, `Eclipse`, `Firmament`, and `Last Sky`.
- Rebuilt the wiki home, progression, items, enemies, and formulas pages so documentation now matches the live progression scope instead of effectively stopping at `41+`.

## v0.21.7 - 2026-03-14
- Doubled rarity drop odds (Magic/Rare/Epic) across all floor brackets.
- Added `SEO` metadata on the main game page (`description`, `canonical`, OpenGraph, Twitter card).
- Added secret admin fallback route under `/wiki/ops-bq-7f4k2` to avoid root-route redirect behavior on some Nginx setups.
- Updated wiki formulas/progression/items/enemies/changelog pages to match the new drop rates and version.

## v0.21.6 - 2026-03-14
- Added `Sell sidegrades` bulk action beside `Sell downgrades` in inventory.
- Added Archon ranged projectile behavior in combat simulation.
- Kept enemy and player combat loops aligned so ranged casters (dragon + archon) share range gating logic.

## v0.21.5 - 2026-03-14
- Gave Archon enemies their own distinct visual design (no longer slime-like).
- Journal kill lines now include enemy name plus XP and gold reward.
- Removed the inventory `Drop` button to simplify combat/item UI.
- Added a secret admin analytics panel route protected by Google login email check (`matduke@gmail.com` by default).

## v0.21.4 - 2026-03-13
- Fixed legacy shared guest identity issue (`guest:guest`) that could clone progress on leaderboard.
- Added server-side normalization for bad legacy guest IDs (`guest/default/null/undefined`) to per-device stable IDs.
- Added automatic purge/exclusion for old `guest:guest` artifacts in profiles/saves/presence/daily stats/leaderboard.

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
