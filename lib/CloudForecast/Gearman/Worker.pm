package CloudForecast::Gearman::Worker;

use strict;
use warnings;
use base qw/Class::Accessor::Fast/;
use Gearman::Worker;
use UNIVERSAL::require;
use Storable qw//;
use File::Temp qw//;
use Parallel::Prefork 0.13;
use Parallel::Scoreboard 0.03;
use CloudForecast::Log;
use CloudForecast::ConfigLoader;
use CloudForecast::Ledge;

#preload
require CloudForecast::Data;

use constant STATUS_IDLE   => '_';
use constant STATUS_ACTIVE => 'A';

__PACKAGE__->mk_accessors(qw/configloader
                             restarter
                             max_workers
                             max_requests_per_child
                             max_exection_time/);

our $GEARMAN_WORKER_CONNECT = {};

sub new {
    my $class = shift;
    my $args = ref $_[0] ? shift : { @_ };

    my $configloader = CloudForecast::ConfigLoader->new({
        root_dir => $args->{root_dir},
        global_config => $args->{global_config}
    });
    $configloader->load_global_config();
    my $global_config = $configloader->global_config;

    die 'gearman is disabled' unless $global_config->{gearman_enable};

    $class->SUPER::new({
        configloader => $configloader,
        restarter => $args->{restarter},
        max_workers => $args->{max_workers} || 4,
        max_requests_per_child => $args->{max_requests_per_child} || 50,
        max_exection_time => $args->{max_exection_time} || 60,
    });
}

sub gearman_worker {
    my $self = shift;
    my $global_config = $self->configloader->global_config;
    my $host = $global_config->{gearman_server}->{host};
    my $port = $global_config->{gearman_server}->{port} || 7003;

    my $worker = $GEARMAN_WORKER_CONNECT->{"${host}:$port"};
    return $worker if $worker;

    $worker = Gearman::Worker->new;
    $worker->job_servers( "${host}:$port" );
    $GEARMAN_WORKER_CONNECT->{"${host}:$port"} = $worker;
    $worker;
}


sub load_resource {
    my $self = shift;
    my $args = shift;

    my $resource_class = $args->{resource_class};
    die "resource_class not defined" unless $resource_class;
    $resource_class = ucfirst $resource_class;
    $resource_class = "CloudForecast::Data::" . $resource_class;
    $resource_class->require or die $@;
    
    my $resource = $resource_class->new({
        hostname => $args->{hostname},
        address => $args->{address},
        details => $args->{details},
        args => $args->{args},
        component_config => $args->{component_config},
        global_config => $self->configloader->global_config,
    });

    return $resource;
}

sub load_ledge {
    my $self = shift;
    return $self->{_ledge} if $self->{_ledge};
    $self->{_ledge} = CloudForecast::Ledge->new(
        data_dir => $self->configloader->global_config->{data_dir},
        db_name => $self->configloader->global_config->{db_name},
    );
    $self->{_ledge};
}

sub fetcher_worker {
    my $self = shift;

    my $worker = $self->gearman_worker;
    $worker->register_function('fetcher', sub {
        my $job = shift;
        my $resource;
        eval {
            my $args;
            eval {
                $args = Storable::thaw($job->arg);
                $args or die "invalid arg";
            };
            die "failed thaw: $@" if $@;
            $resource = $self->load_resource($args);
            $resource->exec_fetch;
        };
        CloudForecast::Log->warn("fetcher failed: $@") if $@;
        1;
    } );

    $self->run_worker(@_);
}

sub updater_worker {
    my $self  = shift;

    my $worker = $self->gearman_worker;
    $worker->register_function('updater', sub {
        my $job = shift;
        eval {
            my $args;
            eval {
                $args = Storable::thaw($job->arg);
                $args or die "invalid arg";
            };
            die "failed thaw: $@" if $@;
            my $resource = $self->load_resource($args);
            $resource->exec_updater($args->{result});
        };
        CloudForecast::Log->warn("fetcher failed: $@") if $@;
        1;
    });

    $worker->register_function('ledge', sub {
        my $job = shift;
        my $ret = {
            error => 0,
        };

        eval {
            my $method;
            my @args;
            eval {
                my $args = Storable::thaw($job->arg);
                $args or die 'invalid arg';
                @args = @$args;
                $method = shift @args;
                $method or die 'no method';
                $method =~ m!^(set|add|get|delete|expire)$! or die "invalid method: $method";
            };
            die "failed thaw: $@" if $@;

            my $ledge = $self->load_ledge();
            if ( $method =~ m!^(set|add|delete|expire)! ) {
                eval {
                    $ret->{status} = $ledge->can($method)->($ledge, @args);
                };
                if ( $@ ) {
                    $ret->{error} = 1;
                    $ret->{errorstr} = $@;
                } 
            }
            else {
                eval {
                    ( $ret->{data}, $ret->{csum} ) = $ledge->get(@args);
                };
                if ( $@ ) {
                    $ret->{error} = 1;
                    $ret->{errorstr} = $@;
                }
            }
        };
        if ( $@ ) {
            CloudForecast::Log->warn("ledge failed: $@");
            $ret->{error} = 1;
            $ret->{errorstr} = $@;
        }
        Storable::nfreeze($ret);
    });

    $self->run_worker(@_);
}

sub watchdog_zombie {
    my $self = shift;
    my $scoreboard = shift;

    my $pid = fork();
    die "fork failed: $!" unless defined $pid;
    return $pid if($pid); # main process

    $SIG{$_} = 'DEFAULT' for qw/TERM HUP USR1/;

    $0 = "$0 (watchdog)";
    while ( 1 ) {
        my $stats = $scoreboard->read_all();
        for my $pid ( keys %$stats) {
            my($status,$time,$type) = split /\s+/, $stats->{$pid}, 3;
            if ( $status eq STATUS_ACTIVE && time - $time > $self->max_exection_time ) {
                CloudForecast::Log->warn("exection_time exceed, kill: " . $pid);
                kill 'TERM', $pid;
            }
            if ( $status eq STATUS_IDLE && time - $time > 360 + int(30*(0.5-rand(1))) ) {
                CloudForecast::Log->warn("idle_time exceed, kill: " . $pid);
                kill 'TERM', $pid
            }
        }
        sleep 1 for 1..30;
    }

    $0 = "$0 (watchdog-wait-for-shutdown)";
    while (1) { sleep 1 }
    exit;
}

sub run_worker {
    my $self = shift;

    my $scoreboard_dir = File::Temp::tempdir(CLEANUP => 1);
    my $scoreboard = Parallel::Scoreboard->new( base_dir => $scoreboard_dir );

    my @watchdog_pid;
    push @watchdog_pid, $self->watchdog_zombie( $scoreboard );
    if ( $self->restarter ) {
        CloudForecast::Log->debug("restarter start");
        push @watchdog_pid, $self->configloader->watchdog;
    }

    my $pm = Parallel::Prefork->new({
        max_workers  => $self->max_workers,
        trap_signals => {
            TERM => 'TERM',
            HUP  => 'TERM',
            USR1 => undef,
        }
    });

    while ( $pm->signal_received ne 'TERM' ) {
        $pm->start and next;
        $0 = "$0 (worker)";
        $scoreboard->update( sprintf "%s %s worker", STATUS_IDLE, time );
        my $i = 0;
        my $worker = $self->gearman_worker;
        $worker->work(
            on_start => sub {
                $i++;
                $scoreboard->update( sprintf "%s %s worker", STATUS_ACTIVE, time );
            },
            on_fail => sub {
                $scoreboard->update( sprintf "%s %s worker", STATUS_IDLE, time );
            },
            on_complete => sub {
                $scoreboard->update( sprintf "%s %s worker", STATUS_IDLE, time );
            },
            stop_if => sub {
                $i++ >= $self->max_requests_per_child
            }
        );

        $pm->finish;
    }

    $pm->wait_all_children();
    for my $pid ( @watchdog_pid ) {
        kill 'TERM', $pid ;
        waitpid( $pid, 0 );
    }
}

1;

