class geowave::restservices {
  $set_public_ip = $geowave::set_public_ip
  $public_dns = $geowave::public_dns

  $line_string = "
        <context-param>
          <param-name>host_port</param-name>
          <param-value>$public_dns</param-value>
        </context-param>"
  
  if !defined(Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"]) {
    package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-restservices":
    ensure => latest,
    tag    => 'geowave-package',
    notify => Service['gwtomcat'],
  }

  if $set_public_ip{
    file_line {'Set_public_dns':
      ensure  => present,
      line    => $line_string,
      path    => "/home/akamel/test/test2.txt",
      match   => "<param-value>$public_dns<\/param-value>",
      after   => "<\/context-param>",
      replace => false,
      require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-restservices"],
      notify  => Service['gwtomcat'],
    }
  }
}
