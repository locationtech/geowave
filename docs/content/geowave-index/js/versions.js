// When a new version of GeoWave is released, keep the previous version's docs on GitHub Pages under its own path and add the new version to the top of the list.

var _versions = {  
  '2.0.1': 'https://locationtech.github.io/geowave/%%page%%',
  '0.9.2.1': 'https://locationtech.github.io/geowave/previous-versions/0.9.2.1/documentation.html',
  '0.9.1': 'https://locationtech.github.io/geowave/previous-versions/0.9.1/documentation.html',
};

if (typeof versions === 'undefined') {
  var versions = _versions;
} else {
  versions = _versions;
}
