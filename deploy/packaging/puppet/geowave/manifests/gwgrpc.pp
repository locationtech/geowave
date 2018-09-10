class geowave::gwgrpc {
  $grpc_port = $geowave::grpc_port

  if !defined(Package["geowave-${geowave::geowave_version}-core"]) {
    package { "geowave-${geowave::geowave_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-grpc":
    ensure => latest,
    tag    => 'geowave-package',
  }

  file { '/etc/geowave/gwgrpc':
    ensure  => present,
    path    => "/etc/geowave/gwgrpc",
    content => "GRPC_PORT=${grpc_port}",
  }

  service { 'gwgrpc':
    ensure   => 'running',
    provider => 'redhat',
    enable   => true,
  }
}
