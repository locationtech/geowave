%define timestamp           %{?_timestamp}%{!?_timestamp: %(date +%Y%m%d%H%M)}
%define version             %{?_version}%{!?_version: UNKNOWN}
%define base_name           geowave
%define name                %{base_name}
%define common_app_name     %{base_name}-%{version}
%define buildroot           %{_topdir}/BUILDROOT/%{common_app_name}-root
%define installpriority     %{_priority} # Used by alternatives for concurrent version installs
%define __jar_repack        %{nil}
%define _rpmfilename        %%{ARCH}/%%{NAME}.%%{RELEASE}.%%{ARCH}.rpm

%define geowave_home           /usr/local/geowave
%define geowave_docs_home      /usr/share/doc/%{common_app_name}
%define geowave_config         /etc/geowave

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Name:           %{base_name}
Version:        %{version}
Release:        %{timestamp}
BuildRoot:      %{buildroot}
BuildArch:      noarch
Summary:        GeoWave provides geospatial and temporal indexing on top of Accumulo and HBase
License:        Apache2
Group:          Applications/Internet
Source1:        bash_profile.sh
Source2:        site-%{version}.tar.gz
Source3:        manpages-%{version}.tar.gz
Source4:        puppet-scripts-%{version}.tar.gz
BuildRequires:  unzip
BuildRequires:  zip
BuildRequires:  xmlto
BuildRequires:  asciidoc

%description
GeoWave provides geospatial and temporal indexing on top of Accumulo and HBase.

%install
# Copy system service files into place
mkdir -p %{buildroot}/etc/profile.d
cp %{SOURCE1} %{buildroot}/etc/profile.d/geowave.sh
mkdir -p %{buildroot}%{geowave_config}

# Copy documentation into place
mkdir -p %{buildroot}%{geowave_docs_home}
tar -xzf %{SOURCE2} -C %{buildroot}%{geowave_docs_home} --strip=1

# Copy man pages into place
mkdir -p %{buildroot}/usr/local/share/man/man1
tar -xvf %{SOURCE3} -C %{buildroot}/usr/local/share/man/man1
rm -rf %{buildroot}%{geowave_docs_home}/manpages
rm -f %{buildroot}%{geowave_docs_home}/*.pdfmarks

# Puppet scripts
mkdir -p %{buildroot}/etc/puppet/modules
tar -xzf %{SOURCE4} -C %{buildroot}/etc/puppet/modules


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package -n     %{common_app_name}-core
Summary:        GeoWave Core
Group:          Applications/Internet
Provides:       %{common_app_name}-core = %{version}

%description -n %{common_app_name}-core
GeoWave provides geospatial and temporal indexing on top of Accumulo.
This package installs the GeoWave home directory and user account

%pre -n %{common_app_name}-core
getent group geowave > /dev/null || /usr/sbin/groupadd -r geowave
getent passwd geowave > /dev/null || /usr/sbin/useradd --system --home /usr/local/geowave -g geowave geowave -c "GeoWave Application Account"

%postun -n %{common_app_name}-core
if [ $1 -eq 0 ]; then
  /usr/sbin/userdel geowave
fi

%files -n %{common_app_name}-core
%attr(644, root, root) /etc/profile.d/geowave.sh

%defattr(644, geowave, geowave, 755)
%dir %{geowave_config}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package -n     %{common_app_name}-docs
Summary:        GeoWave Documentation
Group:          Applications/Internet
Provides:       %{common_app_name}-docs = %{version}
Requires:       %{common_app_name}-core = %{version}

%description -n %{common_app_name}-docs
GeoWave provides geospatial and temporal indexing on top of Accumulo and HBase.
This package installs the GeoWave documentation into the /usr/share/doc/geowave-<version> directory

%files -n       %{common_app_name}-docs
%defattr(644, geowave, geowave, 755)
%doc %{geowave_docs_home}

%doc %defattr(644 root, root, 755)
/usr/local/share/man/man1/

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%package -n     %{common_app_name}-puppet
Summary:        GeoWave Puppet Scripts
Group:          Applications/Internet
Requires:       puppet

%description -n %{common_app_name}-puppet
This package installs the geowave Puppet module to /etc/puppet/modules

%files -n %{common_app_name}-puppet
%defattr(644, root, root, 755)
/etc/puppet/modules/geowave

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%changelog
* Fri Nov 23 2016 Rich Fecher <rfecher@gmail.com> - 0.9.3
- Refactor to separate vendor-specific and common rpms
* Fri Jun 5 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.7-1
- Add external config file
* Fri May 22 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.7
- Use alternatives to support parallel version and vendor installs
- Replace geowave-ingest with geowave-tools
* Thu Jan 15 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2-3
- Added man pages
* Mon Jan 5 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2-2
- Added geowave-puppet rpm
* Fri Jan 2 2015 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2-1
- Added a helper script for geowave-ingest and bash command completion
* Wed Nov 19 2014 Andrew Spohn <andrew.e.spohn.ctr@nga.mil> - 0.8.2
- First packaging
