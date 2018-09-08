class {
  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-grpc":
    ensure => latest,
    tag    => 'geowave-package',
  }
}
