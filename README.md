# zig-dataframe
Proof of concept for type-checked DataFrames in Zig

The goal isn't to create a useful library, but to experiment with incorporating (relatively) strict type systems with DataFrame-style objects. It is useful to show which kinds of operations are easy to support (slicing, `concat`, `pivot`, etc.) and which ones are *hard* (`transpose`).
