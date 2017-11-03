class geowave::accumulo {

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-accumulo":
    ensure => latest,
    tag    => 'geowave-package',
  }

  if !defined(Package["geowave-${geowave::geowave_version}-core"]) {
    package { "geowave-${geowave::geowave_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

}
