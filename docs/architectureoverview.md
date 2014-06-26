---
layout: docs
title: Architecture Overview
---

# Architecture Overview
![Architecture Overview 1](/docs/figures/overview1.png)

At the core of the GeoWave architecture concept is getting data in, and pulling data out – or Ingest and Query.  There are also two types of data persisted in the system – feature data, and metadata.   Feature data is the actual set of attributes and geometries that are stored for later retrieval.  Metadata describes how the data is persisted in the database.  The intent is to store the information needed for data discovery and retrieval in the database – so an existing data store isn’t tied to a bit of configuration on a particular external server or client – but instead is “self-describing.”   