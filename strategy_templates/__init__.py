"""Strategy templates used for real mode=2 testnet reconciliation runs.

与 ``tests/strategies/`` 的"测试替身"不同:这里是真实 mode=2 smoke 时会挂到
账号上的参考实现。代码通过 ``scripts/seed_*_strategy.py`` 读取本目录的 .py
文件并写入 ``strategies`` 表,之后通过 ``POST /api/accounts/{id}/strategies``
挂载 + 激活。
"""
