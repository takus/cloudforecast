package CloudForecast::Data::Mongo;

use CloudForecast::Data -base;
use HTTP::Request;
use JSON;

=head1 NAME

CloudForecast::Data::Mongo - MongoDB resource monitor

=head1 SYNOPSIS

  host_config)

    resources:
      - mongo[:port]

=cut

rrds map { [ $_, 'GAUGE' ]  } qw/ back_average_ms back_last_ms /;
rrds map { [ $_, 'DERIVE' ] } qw/ back_flushes back_total_ms /;
rrds map { [ $_, 'DERIVE' ] } qw/ op_inserts op_queries op_updates op_deletes op_getmores op_commands /;
rrds map { [ $_, 'GAUGE' ]  } qw/ connected_clients/;
rrds map { [ $_, 'DERIVE' ] } qw/ index_accesses index_hits index_misses index_resets /;
rrds map { [ $_, 'GAUGE' ]  } qw/ used_mapped_mem used_virtual_mem used_resident_mem /;

# slave_lag 
graphs 'flush'     => 'Background Flushes';
graphs 'cmd'       => 'Commands';
graphs 'conn'      => 'Connections';
graphs 'index'     => 'Index Ops';
graphs 'memory'    => 'Memory';
#graphs 'slave_lag' => 'Slave Lag';

title {
    my $c = shift;
    my $title = "MongoDB";
    if ( my $port = $c->args->[0] ) {
        $title .= " ($port)";
    }
    return $title;
};

sysinfo {
    my $c = shift;
    $c->ledge_get('sysinfo') || [];
};

fetcher {
    my $c = shift;

    my $host = $c->address;
    my $port = $c->args->[0] ? $c->args->[0] + 1000 : 28017;

    my $ua = $c->component('LWP');
    my $response = $ua->request(
        HTTP::Request->new(GET => "http://$host:$port/_status")
    );
    die "failed to access Mongo HTTP Console: " . $response->status_line
        unless $response->is_success;

    my $stats = decode_json $response->content;

    my @sysinfo;
    if ( $stats->{serverStatus}->{version} ) {
        push @sysinfo, 'version' => $stats->{serverStatus}->{version};
    }
    if ( my $uptime = $stats->{serverStatus}->{uptime} ) {
        my $day  = int( $uptime /86400 );
        my $hour = int( ( $uptime % 86400 ) / 3600 );
        my $min  = int( ( ( $uptime % 86400 ) % 3600) / 60 );
        push @sysinfo, 'uptime' =>  sprintf("up %d days, %2d:%02d", $day, $hour, $min);
    }
    $c->ledge_set( 'sysinfo', \@sysinfo );

    return [
        ( map { int($stats->{serverStatus}->{backgroundFlushing}->{$_}) }
            qw/ average_ms last_ms flushes total_ms / ),
        ( map { $stats->{serverStatus}->{opcounters}->{$_} }
            qw/ insert query update delete getmore command / ),
        $stats->{serverStatus}->{connections}->{current},
        ( map { $stats->{serverStatus}->{indexCounters}->{$_} }
            qw/ accesses hits misses resets / ),
        ( map { int($stats->{serverStatus}->{mem}->{$_}) * 1024 * 1024 }
            qw/ virtual mapped resident / ),
    ];
};

__DATA__
@@ flush
DEF:my1=<%RRD%>:back_flushes:AVERAGE
DEF:my2=<%RRD%>:back_total_ms:AVERAGE
DEF:my3=<%RRD%>:back_average_ms:AVERAGE
DEF:my4=<%RRD%>:back_last_ms:AVERAGE
LINE1:my1#71FF06:Back Flushes   
GPRINT:my1:LAST:Cur\:%6.2lf%s
GPRINT:my1:AVERAGE:Ave\:%6.2lf%s
GPRINT:my1:MAX:Max\:%6.2lf%s\l
LINE1:my2#FF2400:Back Total MS  
GPRINT:my2:LAST:Cur\:%6.2lf%s
GPRINT:my2:AVERAGE:Ave\:%6.2lf%s
GPRINT:my2:MAX:Max\:%6.2lf%s\l
LINE1:my3#E83089:Back Average MS
GPRINT:my3:LAST:Cur\:%6.2lf%s
GPRINT:my3:AVERAGE:Ave\:%6.2lf%s
GPRINT:my3:MAX:Max\:%6.2lf%s\l
LINE1:my4#17D2E1:Back Last MS   
GPRINT:my4:LAST:Cur\:%6.2lf%s
GPRINT:my4:AVERAGE:Ave\:%6.2lf%s
GPRINT:my4:MAX:Max\:%6.2lf%s\l

@@ cmd 
DEF:my1=<%RRD%>:op_inserts:AVERAGE
DEF:my2=<%RRD%>:op_queries:AVERAGE
DEF:my3=<%RRD%>:op_updates:AVERAGE
DEF:my4=<%RRD%>:op_deletes:AVERAGE
DEF:my5=<%RRD%>:op_getmores:AVERAGE
DEF:my6=<%RRD%>:op_commands:AVERAGE
LINE1:my1#FF7200:Op Inserts 
GPRINT:my1:LAST:Cur\:%6.2lf%s
GPRINT:my1:AVERAGE:Ave\:%6.2lf%s
GPRINT:my1:MAX:Max\:%6.2lf%s\l
LINE1:my2#503001:Op Queries 
GPRINT:my2:LAST:Cur\:%6.2lf%s
GPRINT:my2:AVERAGE:Ave\:%6.2lf%s
GPRINT:my2:MAX:Max\:%6.2lf%s\l
LINE1:my3#EDAC00:Op Updates 
GPRINT:my3:LAST:Cur\:%6.2lf%s
GPRINT:my3:AVERAGE:Ave\:%6.2lf%s
GPRINT:my3:MAX:Max\:%6.2lf%s\l
LINE1:my4#506101:Op Deletes 
GPRINT:my4:LAST:Cur\:%6.2lf%s
GPRINT:my4:AVERAGE:Ave\:%6.2lf%s
GPRINT:my4:MAX:Max\:%6.2lf%s\l
LINE1:my5#0CCCCC:Op Getmores
GPRINT:my5:LAST:Cur\:%6.2lf%s
GPRINT:my5:AVERAGE:Ave\:%6.2lf%s
GPRINT:my5:MAX:Max\:%6.2lf%s\l
LINE1:my6#53CA05:Op Commands
GPRINT:my6:LAST:Cur\:%6.2lf%s
GPRINT:my6:AVERAGE:Ave\:%6.2lf%s
GPRINT:my6:MAX:Max\:%6.2lf%s\l

@@ conn 
DEF:my1=<%RRD%>:connected_clients:AVERAGE
LINE1:my1#9B2B1B:Connected Clients
GPRINT:my1:LAST:Cur\:%6.2lf
GPRINT:my1:AVERAGE:Ave\:%6.2lf
GPRINT:my1:MAX:Max\:%6.2lf\l

@@ index 
DEF:my1=<%RRD%>:index_accesses:AVERAGE
DEF:my2=<%RRD%>:index_hits:AVERAGE
DEF:my3=<%RRD%>:index_misses:AVERAGE
DEF:my4=<%RRD%>:index_resets:AVERAGE
LINE1:my1#FF7200:Index Accesses
GPRINT:my1:LAST:Cur\:%6.2lf%s
GPRINT:my1:AVERAGE:Ave\:%6.2lf%s
GPRINT:my1:MAX:Max\:%6.2lf%s\l
LINE1:my2#503001:Index Hits    
GPRINT:my2:LAST:Cur\:%6.2lf%s
GPRINT:my2:AVERAGE:Ave\:%6.2lf%s
GPRINT:my2:MAX:Max\:%6.2lf%s\l
LINE1:my3#EDAC00:Index Misses  
GPRINT:my3:LAST:Cur\:%6.2lf%s
GPRINT:my3:AVERAGE:Ave\:%6.2lf%s
GPRINT:my3:MAX:Max\:%6.2lf%s\l
LINE1:my4#506101:Index Resets  
GPRINT:my4:LAST:Cur\:%6.2lf%s
GPRINT:my4:AVERAGE:Ave\:%6.2lf%s
GPRINT:my4:MAX:Max\:%6.2lf%s\l

@@ memory
DEF:my1=<%RRD%>:used_mapped_mem:AVERAGE
DEF:my2=<%RRD%>:used_virtual_mem:AVERAGE
DEF:my3=<%RRD%>:used_resident_mem:AVERAGE
AREA:my1#6FD1BF:Used Virtual Memory 
GPRINT:my1:LAST:Cur\:%6.2lf%s
GPRINT:my1:AVERAGE:Ave\:%6.2lf%s
GPRINT:my1:MAX:Max\:%6.2lf%s\l
AREA:my2#6FD1BF:Used Mapped Memory  
GPRINT:my2:LAST:Cur\:%6.2lf%s
GPRINT:my2:AVERAGE:Ave\:%6.2lf%s
GPRINT:my2:MAX:Max\:%6.2lf%s\l
AREA:my3#0E6E5C:Used Resident Memory
GPRINT:my3:LAST:Cur\:%6.2lf%s
GPRINT:my3:AVERAGE:Ave\:%6.2lf%s
GPRINT:my3:MAX:Max\:%6.2lf%s\l
