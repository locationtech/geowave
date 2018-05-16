
# GeoWave's Theia Commandline Utility

This module complements GeoWave commandline tools with direct access to Theia public imagery.  To use, ensure the module is on the classpath for your geowave commandline tools and then you should have `geowave theia` options available to you.  `analyze` and `download` are completely separate from storage within GeoWave. The ingest routines wrap download with the additional step of ingesting into GeoWave.  If you want to ingest data that you have already downloaded just use `--retainimages`.  `ingestraster` and `ingestvector` are fairly self-explanatory and `ingest` just wraps both in a single command so for all of the scenes and bands you have ingested into your grid coverage (raster) layer, you will have the vector layers of `scenes` and `bands` with associated metadata. 
For all of the commands, the scenes and bands can be filtered using a CQL expression.  The list of the scene attributes that the CQL expression can be applied towards is this: shape (Geometry), location (String), productIdentifier (String), productType (String), collection (String), platform (String), processingLevel (String), startDate (Date), quicklook (String), thumbnail (String), bands (String), resolution (int), cloudCover (int), snowCover (int), waterCover (int), orbitNumber (int), relativeOrbitNumber (int) and the feature ID is entityId for the scene.  Additionally attributes of the individual bands can be used such as band (String).  Using SPI (with a class matching the `TheiaBandConverterSpi` interface provided on the classpath), a developer can even provide the raster ingest utility with a converter which will run through custom conversion code prior to GeoWave ingest to massage the data in any way.

## Examples
Here is an example of cropping the visible bands over Paris (pre-computing and ingesting an image pyramid and band intensity histograms as well).  The resulting coverage name is `paris_visible` and can be added directly to geoserver as a layer (you likely want to make sure the GeoServer style is applying the red, green, and blue bands from Theia to the correct RGB rendered tiles).
```
theia ingestraster --cql "BBOX(shape,2.08679,48.658291,2.63791,49.04694) AND (band='B2') AND (band='B3') AND (band='B4')" --userident ? --password ? --pyramid --retainimages --crop --histogram --coverage paris_visible <my datastore> <my index> 
```
And here's an example of ingesting both the raster and associated vector (scene and band metadata) data into GeoWave for two bands, intersecting a bounding box over Navarra (Spain). The `navarra_mosaic_${band}` template will be create two resulting coverages, which can be added as two layers in GeoServer.  You could choose the stylization conforming to the band combination that you like.  Also, the `bands` and `scenes` vector layer can be added to geoserver.
```
theia ingest --startDate "2018-01-28" --endDate "2018-01-30" --cql "BBOX(shape,-1.8274,42.3253,-1.6256,42.4735) AND (band='B2') AND (band='B4')" --userident ? --password ? --retainimages --vectorstore <my vector datastore> --vectorindex <my index1>,<my index2> --pyramid --coverage navarra_mosaic_${band} <my raster datastore> <my index3>
```

## Usage
The following is the commandline usage help listing the set of available commands and options:

```
Usage: geowave theia [options]
  Commands:
    analyze
      Print out basic aggregate statistics for available Theia imagery.

    download
      Download Theia imagery to a local directory.

    ingest
      Ingest routine for locally downloading Theia imagery and ingesting it into GeoWave's raster store and in parallel ingesting the scene metadata into GeoWave's vector store.  These two stores can actually be the same or they can be different.

    ingestraster
      Ingest routine for locally downloading Theia imagery and ingesting it into GeoWave.

    ingestvector
      Ingest routine for searching Theia scenes that match certain criteria and ingesting the scene and band metadata into GeoWave's vector store.
```

```
Usage: geowave theia analyze [options]
  Options:
    --collection
       Product collection to fetch within Theia collections ('SENTINEL2').
       Default: SENTINEL2
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       location (String), productIdentifier (String), productType (String), 
       collection (String), platform (String), processingLevel (String), 
       startDate (Date), quicklook (String), thumbnail (String), bands (String), 
       resolution (int), cloudCover (int), snowCover (int), waterCover (int), 
       orbitNumber (int), relativeOrbitNumber (int) and the feature ID is 
       entityId for the scene.  Additionally attributes of the individuals 
       band can be used such as band (String).
       Default: <empty string>
    -f, --enddate
       Optional end Date filter.
       Default: <null>
    --location
       Product location, 100 km Grid Square ID of the Military Grid Reference
       System (EX: 'T30TWM').
       Default: <empty string>
    --orbitnumber
       Optional Orbit Number filter.
       Default: 0
    --platform
       Satellite ('SENTINEL2A','SENTINEL2B').
       Default: <empty string>
    --relativeorbitnumber
       Optional Relative Orbit Number filter.
       Default: 0
    -s, --startdate
       Optional start Date filter.
       Default: <null>
    -ws, --workspaceDir
       A local directory to write temporary files needed for Theia ingest.
       Default is <TEMP_DIR>/theia
       Default: theia
```

```
Usage: geowave theia ingestraster [options] <storename> <comma delimited index/group list>
  Options:
    --collection
       Product collection to fetch within Theia collections ('SENTINEL2').
       Default: SENTINEL2
    --converter
       Prior to ingesting an image, this converter will be used to massage the
       data.  The default is not to convert the data.
    --coverage
       The name to give to each unique coverage.  Freemarker templating can be
       used for variable substitution based on the same attributes used for
       filtering.  The default coverage name is '${entityId}_${band}'.
       If ${band} is unused in the coverage name, all bands will be merged 
       together into the same coverage.
       Default: ${entityId}_${band}
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       location (String), productIdentifier (String), productType (String), 
       collection (String), platform (String), processingLevel (String), 
       startDate (Date), quicklook (String), thumbnail (String), bands (String), 
       resolution (int), cloudCover (int), snowCover (int), waterCover (int), 
       orbitNumber (int), relativeOrbitNumber (int) and the feature ID is 
       entityId for the scene.  Additionally attributes of the individuals 
       band can be used such as band (String).
       Default: <empty string>
    --crop
       Use the spatial constraint provided in CQL to crop the image.  If no
       spatial constraint is provided, this will not have an effect.
       Default: false
    -f, --enddate
       Optional end Date filter.
       Default: <null>
    --histogram
       An option to store the histogram of the values of the coverage so that
       histogram equalization will be performed.
       Default: false
    --location
       Product location, 100 km Grid Square ID of the Military Grid Reference
       System (EX: 'T30TWM').
       Default: <empty string>
    --orbitnumber
       Optional Orbit Number filter.
       Default: 0
    --overwrite
       An option to overwrite images that are ingested in the local workspace
       directory.  By default it will keep an existing image rather than 
       downloading it again.
       Default: false
    --password
       Password to authentificate when downloading Theia imagery.
    --platform
       Satellite ('SENTINEL2A','SENTINEL2B').
       Default: <empty string>
    --pyramid
       An option to store an image pyramid for the coverage.
       Default: false
    --relativeorbitnumber
       Optional Relative Orbit Number filter.
       Default: 0
    --retainimages
       An option to keep the images that are ingested in the local workspace
       directory.  By default it will delete the local file after it is 
       ingested successfully.
       Default: false
    --skipMerge
       By default the ingest will automerge overlapping tiles as a
       post-processing optimization step for efficient retrieval, but this 
       will skip the merge process.
       Default: false
    -s, --startdate
       Optional start Date filter.
       Default: <null>
    --subsample
       Subsample the image prior to ingest by the scale factor provided.  The
       scale factor should be an integer value greater than 1.
       Default: 1
    --tilesize
       The option to set the pixel size for each tile stored in GeoWave.  The
       default is 256.
       Default: 512
    --userident
       email address to authentificate when downloading Theia imagery.
    -ws, --workspaceDir
       A local directory to write temporary files needed for Theia ingest.
       Default is <TEMP_DIR>/theia
       Default: theia
```

```
Usage: geowave theia ingestvector [options] <storename> <comma delimited index/group list>
  Options:
    --collection
       Product collection to fetch within Theia collections ('SENTINEL2').
       Default: SENTINEL2
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       location (String), productIdentifier (String), productType (String), 
       collection (String), platform (String), processingLevel (String), 
       startDate (Date), quicklook (String), thumbnail (String), bands (String), 
       resolution (int), cloudCover (int), snowCover (int), waterCover (int), 
       orbitNumber (int), relativeOrbitNumber (int) and the feature ID is 
       entityId for the scene.  Additionally attributes of the individuals 
       band can be used such as band (String).
       Default: <empty string>
    -f, --enddate
       Optional end Date filter.
       Default: <null>
    --location
       Product location, 100 km Grid Square ID of the Military Grid Reference
       System (EX: 'T30TWM').
       Default: <empty string>
    --orbitnumber
       Optional Orbit Number filter.
       Default: 0
    --platform
       Satellite ('SENTINEL2A','SENTINEL2B').
       Default: <empty string>
    --relativeorbitnumber
       Optional Relative Orbit Number filter.
       Default: 0
    -s, --startdate
       Optional start Date filter.
       Default: <null>
    -ws, --workspaceDir
       A local directory to write temporary files needed for Theia ingest.
       Default is <TEMP_DIR>/theia
       Default: theia
```

```
Usage: geowave theia ingest [options] <rasterstorename> <vectorstorename> <comma delimited index/group list>
  Options:
    --collection
       Product collection to fetch within Theia collections ('SENTINEL2').
       Default: SENTINEL2
    --converter
       Prior to ingesting an image, this converter will be used to massage the
       data.  The default is not to convert the data.
    --coverage
       The name to give to each unique coverage.  Freemarker templating can be
       used for variable substitution based on the same attributes used for
       filtering.  The default coverage name is '${entityId}_${band}'.
       If ${band} is unused in the coverage name, all bands will be merged 
       together into the same coverage.
       Default: ${entityId}_${band}
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       location (String), productIdentifier (String), productType (String), 
       collection (String), platform (String), processingLevel (String), 
       startDate (Date), quicklook (String), thumbnail (String), bands (String), 
       resolution (int), cloudCover (int), snowCover (int), waterCover (int), 
       orbitNumber (int), relativeOrbitNumber (int) and the feature ID is 
       entityId for the scene.  Additionally attributes of the individuals 
       band can be used such as band (String).
       Default: <empty string>
    --crop
       Use the spatial constraint provided in CQL to crop the image.  If no
       spatial constraint is provided, this will not have an effect.
       Default: false
    -f, --enddate
       Optional end Date filter.
       Default: <null>
    --histogram
       An option to store the histogram of the values of the coverage so that
       histogram equalization will be performed.
       Default: false
    --location
       Product location, 100 km Grid Square ID of the Military Grid Reference
       System (EX: 'T30TWM').
       Default: <empty string>
    --orbitnumber
       Optional Orbit Number filter.
       Default: 0
    --overwrite
       An option to overwrite images that are ingested in the local workspace
       directory.  By default it will keep an existing image rather than 
       downloading it again.
       Default: false
    --password
       Password to authentificate when downloading Theia imagery.
    --platform
       Satellite ('SENTINEL2A','SENTINEL2B').
       Default: <empty string>
    --pyramid
       An option to store an image pyramid for the coverage.
       Default: false
    --relativeorbitnumber
       Optional Relative Orbit Number filter.
       Default: 0
    --retainimages
       An option to keep the images that are ingested in the local workspace
       directory.  By default it will delete the local file after it is 
       ingested successfully.
       Default: false
    --skipMerge
       By default the ingest will automerge overlapping tiles as a
       post-processing optimization step for efficient retrieval, but this 
       will skip the merge process.
       Default: false
    -s, --startdate
       Optional start Date filter.
       Default: <null>
    --subsample
       Subsample the image prior to ingest by the scale factor provided.  The
       scale factor should be an integer value greater than 1.
       Default: 1
    --tilesize
       The option to set the pixel size for each tile stored in GeoWave.  The
       default is 256.
       Default: 512
    --userident
       email address to authentificate when downloading Theia imagery.
    --vectorindex
       By ingesting as both vectors and rasters you may want each indexed
       differently.  This will override the index used for vector output.
    --vectorstore
       By ingesting as both vectors and rasters you may want to ingest into
       different stores.  This will override the store for vector output.
    -ws, --workspaceDir
       A local directory to write temporary files needed for Theia ingest.
       Default is <TEMP_DIR>/theia
       Default: theia
```

Lastly, in  <Theia workspace directory>/theia-keystore.crt, it is optional to place a custom keystore for accessing Theia to reduce the set of valid server certificates for SSL connections to Theia's REST API from that of the default system keystore.