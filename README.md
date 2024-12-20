# ARCHIVED

This project has become `lnx-fs` to some extent.

# Yorick

Yorick is a basic blob storage system for writing blocks of and having stable identifiers to retrieve the block.

Yorick is effectively a glorified WAL which performs automatic compaction and re-structuring of data on disk
in order to group blocks which are a part of the same 'group' together.


### Features

- Multiple IO backends to select (BufferedIO, DirectIO)
- Non-blocking async interface.
- Automatic relocating of data on disk to aid with IO caching.


### What Yorick is not

- Not an atomic key-value store.
- Not a standard WAL.
- Not optimised for small chunks of data (Think Kilobytes+ vs Bytes)


### Performance

TODO!
