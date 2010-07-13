#!/usr/bin/perl

# Test the cPanel::TaskQueue module.
#
# This tests the feature that all TaskQueue instances for a given name and dir
# retain the same parameters.

use strict;
use FindBin;
use lib "$FindBin::Bin/mocks";

use Test::More tests => 15;
use cPanel::TaskQueue;

my $cachedir = '/tmp';

# In case the last test did not succeed.
cleanup();

# Set all parameters to non-defaults.
my $q1 = cPanel::TaskQueue->new({
    name => 'tasks', cache_dir => $cachedir,
    default_timeout => 31, max_timeout => 61,
    max_running => 7, default_child_timeout => 117,
});
ok( $q1, 'Initial queue created.' );

my $q2 = cPanel::TaskQueue->new({ name => 'tasks', cache_dir => $cachedir, });
ok( $q2, 'Second queue created.' );

is( $q2->get_default_timeout(), 31, 'Got default timeout from file' );
is( $q2->get_max_timeout(), 61, 'Got max timeout from file' );
is( $q2->get_max_running(), 7, 'Got max in process from file' );
is( $q2->get_default_child_timeout(), 117, 'Got default child timeout from file' );

# Check change for all.
my $q3 = cPanel::TaskQueue->new({
    name => 'tasks', cache_dir => $cachedir,
    default_timeout => 13, max_timeout => 16,
    max_running => 17, default_child_timeout => 742,
});
ok( $q3, 'Initial queue created.' );

is( $q3->get_default_timeout(), 13, 'Overrode default timeout from file' );
is( $q3->get_max_timeout(), 16, 'Overrode max timeout from file' );
is( $q3->get_max_running(), 17, 'Overrode max in process from file' );
is( $q3->get_default_child_timeout(), 742, 'Overrode default child timeout from file' );

# Generate a re-synch.
$q1->how_many_queued();
is( $q1->get_default_timeout(), 13, 'Original updated default timeout from file' );
is( $q1->get_max_timeout(), 16, 'Original updated max timeout from file' );
is( $q1->get_max_running(), 17, 'Original updated max in process from file' );
is( $q1->get_default_child_timeout(), 742, 'Original updated default child timeout from file' );


# Clean up after myself
sub cleanup {
    foreach my $file ( 'tasks_queue.yaml', 'task_queue.yaml.lock' ) {
        unlink "$cachedir/$file" if -e "$cachedir/$file";
    }
}
