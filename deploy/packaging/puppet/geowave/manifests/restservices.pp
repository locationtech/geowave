class geowave::restservices {
  $set_public_dns = $geowave::set_public_dns
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

  if $set_public_dns{
    file_line {'set_public_dns':
      ensure  => present,
      line    => $line_string,
      path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/webapps/restservices/WEB-INF/web.xml",
      match   => "<param-value>$public_dns<\/param-value>",
      after   => "<\/context-param>",
      replace => false,
      require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-restservices"],
      notify  => Service['gwtomcat'],
    }
  }
}
