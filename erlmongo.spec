Name:		erlmongo
Version:	0.0.9
Release:	1%{?dist}
Summary:	Erlang driver for mongodb.

Group:		Database/Driver
License:	APL
URL:		https://github.com/wpntv/erlmongo
Source0:	%{name}-%{version}.tar.gz
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	erlang
Requires:	erlang

%define erlmongolibdir %{_libdir}/erlang/lib/%{name}-%{version}

%description
Erlmongo is a pretty complete Erlang driver for mongodb.

It supports records and proplists as datatypes. Strings can be lists or binaries, but strings received from mongodb (as a result of find) will be binaries.

Because of the way records work in Erlang, you need to call mongoapi:recinfo/2 before using any record, or define each record in erlmongo.hrl.


%prep
%setup -q


%build
%configure
make %{?_smp_mflags}


%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT


%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%doc

%{erlmongolibdir}/ebin/erlmongo.app
%{erlmongolibdir}/ebin/erlmongo_app.beam
%{erlmongolibdir}/ebin/mongodb.beam
%{erlmongolibdir}/ebin/mongodb_supervisor.beam
%{erlmongolibdir}/ebin/mongoapi.beam
%{erlmongolibdir}/include/erlmongo.hrl

%changelog
* Thu Jan 27 2011 Douglas Hubler <douglas@hubler.us>
- Initial release
