# Tick Lists for Hikyuu -> DolphinDB

这些清单文件用于 `alphahome-ddb import-hikyuu-5min` 的 `--codes-file` 参数。

## 文件命名（脚本默认只用 *_all）

- `sh_all.txt` / `sz_all.txt` / `bj_all.txt`：按市场全量
- `{market}_{digit}.txt`：按代码首位数字分组（例如 `sh_6.txt`）
- `all.txt`：三市场汇总

`import_all_hikyuu_to_ddb.ps1` 默认只会运行一次导入：
- `all.txt`（全市场）或
- `{market}_all.txt`（指定单市场）

数字分组文件主要用于你手动分批/断点续跑（不是默认路径）。

## 重新导入（会删除 DolphinDB 数据）

如之前误重复导入导致 DolphinDB 存在重复行，最简单的处理方式是删除并重建库表后再全量导入：

```powershell
./scripts/import_all_hikyuu_to_ddb.ps1 -ResetDb -InitTable
```

## 生成/更新

当 `E:/stock/{sh,sz,bj}_5min.h5` 更新后，建议重新生成清单：

```powershell
python scripts/generate_hikyuu_5min_tickers.py --output-dir scripts/tickers
```
