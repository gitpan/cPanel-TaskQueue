#!/usr/bin/perl

use Test::More tests => 7;

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/mocks";


use cPanel::TaskQueue::Scheduler ( -logger => 'cPanel::FakeLogger' );

my $cachedir = '/tmp';

# In case the last test did not succeed.
cleanup();

{
    open( my $fh, '>', "$cachedir/tasks_sched.yaml" );
    print $fh "Bad YAML file.";
}

{
    my $queue = cPanel::TaskQueue::Scheduler->new( { name => 'tasks', cache_dir => $cachedir } );
    isa_ok( $queue, 'cPanel::TaskQueue::Scheduler', 'Correct object built.' );
    is( $queue->get_name, 'tasks', 'Queue is named correctly.' );
    ok( -e "$cachedir/tasks_sched.yaml.broken", 'Bad file moved out of the way.' );
    is( do{open my $fh, '<', "$cachedir/tasks_sched.yaml.broken"; scalar <$fh>;},
        "Bad YAML file.",
        'Damaged file was moved.'
    );
}

cleanup();

{
    use YAML::Syck ();

    open( my $fh, '>', "$cachedir/tasks_sched.yaml" );
    print $fh YAML::Syck::Dump( 'TaskScheduler', 3.1415, {} );
}

{
    my $queue = cPanel::TaskQueue::Scheduler->new( { name => 'tasks', cache_dir => $cachedir } );
    isa_ok( $queue, 'cPanel::TaskQueue::Scheduler', 'Correct object built.' );
    is( $queue->get_name, 'tasks', 'Queue is named correctly.' );
    ok( -e "$cachedir/tasks_sched.yaml.broken", 'Bad file moved out of the way.' );
}

cleanup();

# Clean up after myself
sub cleanup {
    foreach my $file ( 'tasks_sched.yaml', 'tasks_sched.yaml.broken', 'tasks_sched.yaml.lock' ) {
        unlink "$cachedir/$file" if -e "$cachedir/$file";
    }
}