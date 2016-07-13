
# GeoWave's LandSat8 Commandline Utility

This module complements GeoWave commandline tools with direct access to landsat public imagery.  To use, ensure the module is on the classpath for your geowave commandline tools and then you should have `geowave landsat` options available to you.  `analyze` and `download` are completely separate from storage within GeoWave. The ingest routines wrap download with the additional step of ingesting into GeoWave.  If you want to ingest data that you have already downloaded just use `--retainimages`.  `ingestraster` and `ingestvector` are fairly self-explanatory and `ingest` just wraps both in a single command so for all of the scenes and bands you have ingested into your grid coverage (raster) layer, you will have the vector layers of `scenes` and `bands` with associated metadata. 
For all of the commands, the scenes and bands can be filtered using a CQL expression.  The list of the scene attributes that the CQL expression can be applied towards is this: shape (Geometry), acquisitionDate (Date), cloudCover (double), processingLevel (String), path (int), row (int) and the feature ID is entityId for the scene.  Additionally attributes of the individual bands can be used such as band (String), sizeMB (double), and bandDownloadUrl (String).  Also for all commands, you can grab only the N best cloud cover scenes or bands using `--nbestscenes` or `--nbestbands` (likely want to use nbestscenes because cloud cover is the same for a scene except for some very specific use cases).  With that, you may also want to `--nbestperspatial` which is a boolean flag that will tell the operation to maintain the N best cloud cover scenes by path and row so that overlapping scenes are minimized (for example `--nbestscenes 1 --nbestperspatial` would grab the best non-overlapping cloud cover scenes matching the filter criteria which would be an excellent choice if the goal is to produce a mosaic).  Using SPI (with a class matching the `Landsat8BandConverterSpi` interface provided on the classpath), a developer can even provide the raster ingest utility with a converter which will run through custom conversion code prior to GeoWave ingest to massage the data in any way.

## Examples
Here is an example of cropping the visible bands only, best cloud cover data available over Paris (pre-computing and ingesting an image pyramid and band intensity histograms as well).  The resulting coverage name is `paris_visible` and can be added directly to geoserver as a layer (you likely want to make sure the GeoServer style is applying the red, green,and blue bands from Landsat8 to the correct RGB rendered tiles).
```
landsat ingestraster --cql "BBOX(shape,2.08679,48.658291,2.63791,49.04694) AND (band='B2') AND (band='B3') AND (band='B4')" --usecachedscenes --nbestscenes 1 --nbestperspatial --pyramid --retainimages --crop --histogram  --coverage paris_visible <my datastore> <my index> 
```
And here's an example of ingesting both the raster and associated vector (scene and band metadata) data into GeoWave for all bands, intersecting a bounding box Paris, best cloud cover available, specifically with the scene paths 198 and 199 (scenes are organized by row and path based on Landsat8 collection).  The resulting coverage name is `paris_all_bands` which can be added as a layer in GeoServer.  If you were to add this as a layer to GeoServer perhaps you create a greyscale style using the panchromatic band from Landsat8 (band 8).  Or really just choose the stylization conforming to the band combination that you like.  Also, the `bands` and `scenes` vector layer can be added to geoserver.
```
landsat ingest --cql "BBOX(shape,2.08679,48.658291,2.63791,49.04694) AND (path=198 OR path=199)" --usecachedscenes --nbestscenes 1 --nbestperspatial --pyramid --retainimages --histogram --coverage paris_all_bands <my datastore> <my index> 
```

## Usage
The following is the commandline usage help listing the set of available commands and options:

```
Usage: geowave landsat [options]

  Commands:
    analyze
      Print out basic aggregate statistics for available Landsat 8 imagery

    download
      Download Landsat 8 imagery to a local directory

    ingest
      Ingest routine for locally downloading Landsat 8 imagery and ingesting it into GeoWave's raster store and in parallel ingesting the scene metadata into GeoWave's vector store.  These two stores can actually be the same or they can be different.

    ingestraster
      Ingest routine for locally downloading Landsat 8 imagery and ingesting it into GeoWave

    ingestvector
      Ingest routine for searching landsat scenes that match certain criteria and ingesting the scene and band metadata into GeoWave's vector store.
```
      
```
Usage: geowave landsat analyze [options]
  Options:
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       acquisitionDate (Date), cloudCover (double), processingLevel (String), path (int), row
       (int) and the feature ID is entityId for the scene.  Additionally attributes of
       the individuals band can be used such as band (String), sizeMB (double), and
       bandDownloadUrl (String)
       Default: <empty string>
    --nbestbands
       An option to identify and only use a set number of bands with the best
       cloud cover
       Default: 0
    --nbestperspatial
       A boolean flag, when applied with --nbestscenes or --nbestbands will
       aggregate scenes and/or bands by path/row
       Default: false
    --nbestscenes
       An option to identify and only use a set number of scenes with the best
       cloud cover
       Default: 0
    --sincelastrun
       An option to check the scenes list from the workspace and if it exists,
       to only ingest data since the last scene.
       Default: false
    --usecachedscenes
       An option to run against the existing scenes catalog in the workspace
       directory if it exists.
       Default: false
    -ws, --workspaceDir
       A local directory to write temporary files needed for landsat 8 ingest.
       Default is <TEMP_DIR>/landsat8
       Default: landsat8
```

```     
Usage: geowave landsat ingestraster [options] <storename> <comma delimited index/group list>
  Options:
    --converter
       Prior to ingesting an image, this converter will be used to massage the
       data. The default is not to convert the data.
    --coverage
       The name to give to each unique coverage. Freemarker templating can be
       used for variable substition based on the same attributes used for filtering. 
       The default coverage name is '${entityId}_${band}'.  If ${band} is unused in
       the coverage name, all bands will be merged together into the same coverage.
       Default: ${entityId}_${band}
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       acquisitionDate (Date), cloudCover (double), processingLevel (String), path (int), row
       (int) and the feature ID is entityId for the scene.  Additionally attributes of
       the individuals band can be used such as band (String), sizeMB (double), and
       bandDownloadUrl (String)
       Default: <empty string>
    --crop
       Use the spatial constraint provided in CQL to crop the image.  If no
       spatial constraint is provided, this will not have an effect.
       Default: false
    --histogram
       An option to store the histogram of the values of the coverage so that
       histogram equalization will be performed
       Default: false
    --nbestbands
       An option to identify and only use a set number of bands with the best
       cloud cover
       Default: 0
    --nbestperspatial
       A boolean flag, when applied with --nbestscenes or --nbestbands will
       aggregate scenes and/or bands by path/row
       Default: false
    --nbestscenes
       An option to identify and only use a set number of scenes with the best
       cloud cover
       Default: 0
    --overwrite
       An option to overwrite images that are ingested in the local workspace
       directory.  By default it will keep an existing image rather than downloading it
       again.
       Default: false
    --pyramid
       An option to store an image pyramid for the coverage
       Default: false
    --retainimages
       An option to keep the images that are ingested in the local workspace
       directory.  By default it will delete the local file after it is ingested
       successfully.
       Default: false
    --sincelastrun
       An option to check the scenes list from the workspace and if it exists,
       to only ingest data since the last scene.
       Default: false
    --subsample
       Subsample the image prior to ingest by the scale factor provided.  The
       scale factor should be an integer value greater than 1.
       Default: 1
    --tilesize
       The option to set the pixel size for each tile stored in GeoWave. The
       default is 256
       Default: 512
    --usecachedscenes
       An option to run against the existing scenes catalog in the workspace
       directory if it exists.
       Default: false
    -ws, --workspaceDir
       A local directory to write temporary files needed for landsat 8 ingest.
       Default is <TEMP_DIR>/landsat8
       Default: landsat8
```

```       
Usage: geowave landsat ingestvector [options] <storename> <comma delimited index/group list>
  Options:
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       acquisitionDate (Date), cloudCover (double), processingLevel (String), path (int), row
       (int) and the feature ID is entityId for the scene.  Additionally attributes of
       the individuals band can be used such as band (String), sizeMB (double), and
       bandDownloadUrl (String)
       Default: <empty string>
    --nbestbands
       An option to identify and only use a set number of bands with the best
       cloud cover
       Default: 0
    --nbestperspatial
       A boolean flag, when applied with --nbestscenes or --nbestbands will
       aggregate scenes and/or bands by path/row
       Default: false
    --nbestscenes
       An option to identify and only use a set number of scenes with the best
       cloud cover
       Default: 0
    --sincelastrun
       An option to check the scenes list from the workspace and if it exists,
       to only ingest data since the last scene.
       Default: false
    --usecachedscenes
       An option to run against the existing scenes catalog in the workspace
       directory if it exists.
       Default: false
    -ws, --workspaceDir
       A local directory to write temporary files needed for landsat 8 ingest.
       Default is <TEMP_DIR>/landsat8
       Default: landsat8
```

```       
Usage: geowave landsat ingest [options] <rasterstorename> <vectorstorename> <comma delimited index/group list>
  Options:
    --converter
       Prior to ingesting an image, this converter will be used to massage the
       data. The default is not to convert the data.
    --coverage
       The name to give to each unique coverage. Freemarker templating can be
       used for variable substition based on the same attributes used for filtering. 
       The default coverage name is '${entityId}_${band}'.  If ${band} is unused in
       the coverage name, all bands will be merged together into the same coverage.
       Default: ${entityId}_${band}
    --cql
       An optional CQL expression to filter the ingested imagery. The feature
       type for the expression has the following attributes: shape (Geometry),
       acquisitionDate (Date), cloudCover (double), processingLevel (String), path (int), row
       (int) and the feature ID is entityId for the scene.  Additionally attributes of
       the individuals band can be used such as band (String), sizeMB (double), and
       bandDownloadUrl (String)
       Default: <empty string>
    --crop
       Use the spatial constraint provided in CQL to crop the image.  If no
       spatial constraint is provided, this will not have an effect.
       Default: false
    --histogram
       An option to store the histogram of the values of the coverage so that
       histogram equalization will be performed
       Default: false
    --nbestbands
       An option to identify and only use a set number of bands with the best
       cloud cover
       Default: 0
    --nbestperspatial
       A boolean flag, when applied with --nbestscenes or --nbestbands will
       aggregate scenes and/or bands by path/row
       Default: false
    --nbestscenes
       An option to identify and only use a set number of scenes with the best
       cloud cover
       Default: 0
    --overwrite
       An option to overwrite images that are ingested in the local workspace
       directory.  By default it will keep an existing image rather than downloading it
       again.
       Default: false
    --pyramid
       An option to store an image pyramid for the coverage
       Default: false
    --retainimages
       An option to keep the images that are ingested in the local workspace
       directory.  By default it will delete the local file after it is ingested
       successfully.
       Default: false
    --sincelastrun
       An option to check the scenes list from the workspace and if it exists,
       to only ingest data since the last scene.
       Default: false
    --subsample
       Subsample the image prior to ingest by the scale factor provided.  The
       scale factor should be an integer value greater than 1.
       Default: 1
    --tilesize
       The option to set the pixel size for each tile stored in GeoWave. The
       default is 256
       Default: 512
    --usecachedscenes
       An option to run against the existing scenes catalog in the workspace
       directory if it exists.
       Default: false
    --vectorindex
       By ingesting as both vectors and rasters you may want each indexed
       differently.  This will override the index used for vector output.
    --vectorstore
       By ingesting as both vectors and rasters you may want to ingest into
       different stores.  This will override the store for vector output.
    -ws, --workspaceDir
       A local directory to write temporary files needed for landsat 8 ingest.
       Default is <TEMP_DIR>/landsat8
       Default: landsat8  
```           