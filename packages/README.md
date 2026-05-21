# Packages

This directory contains standalone packages incubated inside the AlphaHome
repository.

`tinydata` is intentionally structured as an independent Python package. Its
runtime code must not import `alphahome` or depend on AlphaDB, the GUI, or
AlphaHome task registration.
