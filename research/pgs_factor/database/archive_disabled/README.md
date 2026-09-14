# Disabled legacy PGS database artifacts

This directory is a source-only archive. Its writer and DDL target the retired
`pgs_factors` storage schema and must never be imported or executed. Production
P/G data belongs to `factors`; `pgs_factors` is compatibility-view-only.

The active compatibility stub one directory above fails closed. All production
writes must go through `alphahome.factors.FactorCoordinator`.
