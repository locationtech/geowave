---
layout: docs
title: Welcome
---

# Welcome

## What is GeoWave
GeoWave is a library for storage, index, and search of multi-dimensional data on top of a sorted key-value datastore.   GeoWave includes specific tailored implementations that have advanced support for OGC spatial types (up to 3 dimensions), and both bounded and unbounded temporal values.  Both single and ranged values are supported on all axes.  GeoWave’s geospatial support is built on top of the GeoTools extensibility model, so it plugins natively to GeoServer, uDig, and any other GeoTools compatible project – and can ingest GeoTools compatible data sources.   GeoWave comes out of the box with an Accumulo implementation.

## Intent behind GeoWave

### Pluggable Backend
GeoWave is intended to be a multidimensional indexing layer that can be added on top of any sorted key-value store.  Accumulo was chosen as the target architecture – but HBase would be a relatively straightforward swap – and any datastore which allows prefix based range scans should be trivial extensions.

### Modular Design
The architecture itself is designed to be extremely extensible – with most of the functionality units defined by interfaces, with default implementations of these interfaces to cover most use cases. It is expected that the out of the box functionality should satisfy 90% of use cases – at least that is the intent – but the modular architecture allows for easy feature extension as well as integration into other platforms.   

### Self-Describing Data
GeoWave also targets keeping data configuration, format, and other information needed to manipulate data in the database itself.  This allows software to programmatically interrogate all the data stored in a single or set of GeoWave instances without needing bits of configuration from clients, application servers, or other external stores.
