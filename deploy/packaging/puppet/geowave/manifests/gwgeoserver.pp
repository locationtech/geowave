class geowave::gwgeoserver {
  if !defined(Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"]) {
    package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwgeoserver":
    ensure => latest,
    tag    => 'geowave-package',
    notify => Service['gwtomcat']
  }
}
