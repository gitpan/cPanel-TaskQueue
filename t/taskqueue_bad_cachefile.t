#!/usr/bin/perl

use Test::More tests => 7;

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/mocks";


use cPanel::TaskQueue ( -logger => 'cPanel::FakeLogger' );

my $cachedir = '/tmp';

# In case the last test did not succeed.
cleanup();

{
    open( my $fh, '>', "$cachedir/tasks_queue.yaml" );
    print $fh "Bad YAML file.";
}

{
    my $queue = cPanel::TaskQueue->new( { name => 'tasks', cache_dir => $cachedir } );
    isa_ok( $queue, 'cPanel::TaskQueue', 'Correct object built.' );
    is( $queue->get_name, 'tasks', 'Queue is named correctly.' );
    ok( -e "$cachedir/tasks_queue.yaml.broken", 'Bad file moved out of the way.' );
    is( do{open my $fh, '<', "$cachedir/tasks_queue.yaml.broken"; scalar <$fh>;},
        "Bad YAML file.",
        'Damaged file was moved.'
    );
}

cleanup();

{
    use YAML::Syck ();

    open( my $fh, '>', "$cachedir/tasks_queue.yaml" );
    print $fh YAML::Syck::Dump( 'TaskQueue', 3.1415, {} );
}

{
    my $queue = cPanel::TaskQueue->new( { name => 'tasks', cache_dir => $cachedir } );
    isa_ok( $queue, 'cPanel::TaskQueue', 'Correct object built.' );
    is( $queue->get_name, 'tasks', 'Queue is named correctly.' );
    ok( -e "$cachedir/tasks_queue.yaml.broken", 'Bad file moved out of the way.' );
}

cleanup();

# Clean up after myself
sub cleanup {
    foreach my $file ( 'tasks_queue.yaml', 'tasks_queue.yaml.broken', 'tasks_queue.yaml.lock' ) {
        unlink "$cachedir/$file" if -e "$cachedir/$file";
    }
}