class geowave::app {

  $geowave_base_app_rpms = [
    "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-tools",
    "geowave-${geowave::geowave_version}-docs",
  ]

  package { $geowave_base_app_rpms:
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
