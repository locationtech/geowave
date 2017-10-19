#### GeoWave on EMR

The configuration files in this directory can be used to deploy GeoWave to the Amazon Elastic MapReduce (EMR) service which allows you to be able to quickly stand up a cluster with Accumulo and GeoWave pre-installed.

There are tokens within the template. Running generate-emr-scripts.sh will take the template and generate a set of scripts, replacing tokens appropriately (required parameters to that scipt are --buildtype (either dev or release), --version, and --workspace (path to Jenkins job workspace)).  The resultant scripts will be in a 'generated' directory. 

The GeoWave documentation has instructions for how to deploy and use these file in the Running from EMR section.
