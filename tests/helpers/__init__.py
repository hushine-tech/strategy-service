"""Test helpers shared across strategy-service tests.

All wallet fixtures should be obtained through ``wallet_fixtures`` to keep
test construction aligned with the production path (proto canonical state →
``build_wallet_from_account`` → ``BinanceWalletRuntime``). Direct manual
runtime construction is intentionally avoided after Phase C2b cleanup.
"""
