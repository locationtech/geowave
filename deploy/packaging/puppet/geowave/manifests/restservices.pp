class geowave::restservices {
  if !defined(Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"]) {
    package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-restservices":
    ensure => latest,
    tag    => 'geowave-package',
    notify => Service['gwtomcat']
  }
}
