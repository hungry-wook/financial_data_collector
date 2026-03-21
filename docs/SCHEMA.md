# Schema

## ?? ???
- `instruments`: ?? ???
- `instrument_delisting_snapshot`: KIND ?? ???
- `collection_runs`: ?? ?? ??
- `trading_calendar`: ??? ???
- `daily_market_data`: ?? ?? ?? ???
- `benchmark_index_data`: ???? ??
- `data_quality_issues`: ?? ??
- `price_adjustment_factors`: ??? ?? ????

## ?? ??
### `instruments`
- `instrument_id`
- `external_code`
- `market_code`
- `instrument_name`
- `listing_date`
- `delisting_date`
- `listed_shares`

### `daily_market_data`
- `instrument_id`
- `trade_date`
- `open`, `high`, `low`, `close`, `volume`
- `turnover_value`
- `market_value`
- `listed_shares`
- `base_price`
- `is_trade_halted`
- `record_status`

### `benchmark_index_data`
- `index_code`
- `index_name`
- `trade_date`
- `open`, `high`, `low`, `close`, `volume`
- `turnover_value`
- `market_cap`
- `record_status`

### `price_adjustment_factors`
- `instrument_id`
- `trade_date`
- `as_of_date`
- `factor`
- `cumulative_factor`

## ?? ?
- `instrument_daily_v1`: ?? ??? ?? ?? ?? ??
- `benchmark_daily_v1`: ???? ?? ??? ?
- `trading_calendar_v1`: ??? ??? ?
