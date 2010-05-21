package CloudForecast::ConfigLoader;

use strict;
use warnings;
use base qw/Class::Accessor::Fast/;
use Path::Class qw//;
use YAML qw//;
use Cwd;
use Filesys::Notify::Simple;
use CloudForecast::Log;

__PACKAGE__->mk_accessors(qw/root_dir global_config_yaml server_list_yaml 
                           global_config global_component_config server_list
                           all_hosts host_config_cache/);

sub new {
    my $class = shift;
    my $args = shift;
    bless { 
        root_dir => $args->{root_dir},
        global_config_yaml => $args->{global_config},
        server_list_yaml => $args->{server_list},
        global_config => {},
        global_component_config => {},
        server_list => [],
        all_hosts => [],
        host_config_cache => {},
    }, $class;
}

sub load_all {
    my $self = shift;
    $self->load_global_config();
    $self->load_server_list();
}

sub load_yaml {
    my $self = shift;
    my $file = shift;

    my @data;
    eval {
        if ( ref $file ) {
            @data = YAML::Load($$file);
        }
        else {
            @data = YAML::LoadFile($file);
        }
        die "no yaml data in $file" unless @data;
    };
    die "cannot parse $file: $@" if $@;

    return wantarray ? @data : $data[0];
}

sub load_global_config {
    my $self = shift;

    my $config = $self->load_yaml(
        $self->global_config_yaml
    );

    $self->global_component_config( $config->{component_config} || {} );
    $self->global_config( $config->{config} || {} );


    my $host_config_dir = $self->global_config->{host_config_dir} || 'host_config';
    if ( $host_config_dir !~ m!^/! ) {
        $self->global_config->{host_config_dir} = Path::Class::dir(
            $self->root_dir,
            $host_config_dir );
    }

    my $data_dir = $self->global_config->{data_dir};
    die 'data_dir isnot defined in config' unless $data_dir; 
    if ( $data_dir !~ m!^/! ) {
        $self->global_config->{data_dir} = Path::Class::dir(
            $self->root_dir,
            $data_dir );
    }
}

sub load_server_list {
    my $self = shift;

    # load global config first
    if ( !$self->global_config ) {
        $self->load_global_config();
    }

    my $file = $self->server_list_yaml;
    open( my $fh, $file ) or die "cannot open $file: $!";
    my @group_titles;
    my $data="";
    while ( my $line = <$fh> ) {
        if ( $line =~ m!^---\s+#(.+)$! ) {
            $data .= "---\n";
            push @group_titles, $1;
        }
        else {
            $data .= $line;
        }
    }

    my @groups = $self->load_yaml( \$data );
    die 'number of titles and groups not match' 
        if scalar @groups != scalar @group_titles;

    my @hosts_by_group;
    my %all_hosts;
    my $i=0;
    foreach my $group ( @groups ) {

        my @group_hosts;
        my $server_count=0;
        foreach my $server ( @{$group->{servers}} ) {

            my $host_config = $server->{config}
                or die "cannot find config in $group_titles[$i] (# $server_count)";
            my $hosts = $server->{hosts} || [];
            for my $host_line ( @$hosts ) {
                my $host = $self->parse_host( $host_line, $host_config );
                push @group_hosts, $host;
                $all_hosts{$host->{address}} = $host;                    
            }

            $server_count++;
        }

        push @hosts_by_group, {
            title => $group_titles[$i],
            hosts => \@group_hosts,
        };
        $i++;
    }

    $self->server_list( \@hosts_by_group );
    $self->all_hosts( \%all_hosts );
}

sub load_host_config {
    my $self = shift;
    my $file = shift;
    my $host_config_cache = $self->host_config_cache;
    return $host_config_cache->{$file} 
        if $host_config_cache->{$file};

    my $config = $self->load_yaml(
        Path::Class::file( $self->global_config->{host_config_dir},
                          $file
                      )->stringify );
    $config ||= {};
    $config->{resources} ||= [];
    $config->{component_config} ||= {};

    my $global_config = $self->global_component_config;

    for my $component ( keys %{$global_config} ) {
        my $component_config = $config->{component_config}->{$component} || {};
        my %merge = ( %{$global_config->{$component}}, %{ $component_config } );
        $config->{component_config}->{$component} = \%merge;
    }

    $host_config_cache->{$file} = $config;
    return $config;
}

sub parse_host {
    my $self = shift;
    my $line = shift;
    my $config_yaml = shift;

    my ( $address, $hostname, $details )  = split /\s+/, $line, 3;
    die "no address" unless $address;
    $hostname ||= $address;
    $details ||= "";

    my $config = $self->load_host_config( $config_yaml );

    return {
        address => $address,
        hostname => $hostname,
        details => $details,
        component_config => $config->{component_config},
        resources => $config->{resources}
    };
}


sub watchdog {
    my $self = shift;
    my $parent_pid = $$;

    my $root_dir = $self->root_dir;
    die "root_dir is undefined" unless $root_dir;
    my @path;
    push @path, "$root_dir/lib", "$root_dir/site-lib";
    my $program_name = Cwd::realpath($0);
    push @path, $program_name if -f $program_name;
    push @path, $self->global_config_yaml if $self->global_config_yaml;
    push @path, $self->server_list_yaml if $self->server_list_yaml;
    push @path, $self->global_config->{host_config_dir}
        if $self->global_config->{host_config_dir};

    my $pid = fork();
    die "failed fork: $!" unless defined $pid;
    return $pid if($pid); # main process
    
    my $watcher = Filesys::Notify::Simple->new(\@path);
    while (1) {
        $watcher->wait( sub {
            my @path = grep { $_ !~ m![/\\][\._]|\.bak$|~$!  } map { $_->{path} } @_;
            return if ! @path;
            CloudForecast::Log->warn( "File updates: " , join(",", @path) );
            sleep 1;
            kill 'TERM', $parent_pid;
        } );
    }
}

1;

