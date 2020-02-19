// When a new version of GeoWave is released, update the previous version to load from S3 and add the new version to the top of the list.

var _versions = {
  '1.0.0': 'https://locationtech.github.io/geowave/%%page%%',
  '0.9.8': 'http://s3.amazonaws.com/geowave/0.9.8/docs/%%page%%',
  '0.9.7': 'http://s3.amazonaws.com/geowave/0.9.7/docs/%%page%%',
  '0.9.6': 'http://s3.amazonaws.com/geowave/0.9.6/docs/%%page%%',
  '0.9.5': 'http://s3.amazonaws.com/geowave/0.9.5/docs/%%page%%',
  '0.9.4': 'http://s3.amazonaws.com/geowave/0.9.4/docs/%%page%%',
  '0.9.3': 'http://s3.amazonaws.com/geowave/0.9.3/docs/%%page%%',
  '0.9.2.1': 'http://locationtech.github.io/geowave/previous-versions/0.9.2.1/documentation.html',
  '0.9.1': 'http://locationtech.github.io/geowave/previous-versions/0.9.1/documentation.html',
};

if (typeof versions === 'undefined') {
  var versions = _versions;
} else {
  versions = _versions;
}
