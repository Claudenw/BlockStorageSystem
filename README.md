# Block Storage System

A simple block storage system that reads/writes objects or SpanBuffers from a 
file.

Basic structure is that there is a block that starts at position 0 that comprises
the list of deleted records.

As records are written to the system they reuse the deleted records or extend the file as necessary.

As records are deleted the free list grows.


