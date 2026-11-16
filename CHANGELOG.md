# Changelog

## [1.21.1](https://github.com/firebolt-db/firebolt-go-sdk/compare/v1.21.0...v1.21.1) (2026-11-16)


### Bug Fixes

* send queries as text/plain instead of application/json (FB-5787) ([#241](https://github.com/firebolt-db/firebolt-go-sdk/issues/241)) ([eb4df58](https://github.com/firebolt-db/firebolt-go-sdk/commit/eb4df58a0c3d2783f43e54617df0bd126b5d7ebb))

## [1.21.0](https://github.com/firebolt-db/firebolt-go-sdk/compare/v1.20.1...v1.21.0) (2026-08-18)


### Features

* support ARRAY(STRUCT(...)) in batch inserts ([#207](https://github.com/firebolt-db/firebolt-go-sdk/issues/207)) ([2618ca0](https://github.com/firebolt-db/firebolt-go-sdk/commit/2618ca080ac9f43738b4e37ca4fc886d5a5351e6))
* support JSON columns in batch inserts ([#206](https://github.com/firebolt-db/firebolt-go-sdk/issues/206)) ([a317035](https://github.com/firebolt-db/firebolt-go-sdk/commit/a31703591892c66832f710653c2d1fcb397f4d4d))


### Bug Fixes

* propagate ignored errors across SDK ([#209](https://github.com/firebolt-db/firebolt-go-sdk/issues/209)) ([35fa19f](https://github.com/firebolt-db/firebolt-go-sdk/commit/35fa19fb51fb404ec85dbf3221a3ceb40d695a6a))
