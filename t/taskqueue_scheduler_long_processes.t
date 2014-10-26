#!/usr/bin/perl

# Test the cPanel::TaskQueue module.
#
# This tests the code for handling long-running processes. Since it is, by
#  necessity, slower to execute than we probably want to run as a normal
#  test. This code is disabled, unless it is run with the environment
#  variable CPANEL_SLOW_TESTS set.


use strict;
use FindBin;
use lib "$FindBin::Bin/mocks";
use File::Path ();

use Test::More tests => 18;
use cPanel::TaskQueue::Scheduler;

my $tmpdir = './tmp';
my $statedir = "$tmpdir/statedir";

{
    package MockQueue;

    sub new {
        return bless [];
    }

    sub queue_task {
        my ($self, $task) = @_;

        push @{$self}, $task;
        return 1;
    }

    sub clear_tasks {
        my ($self) = @_;
        @{$self} = ();
        return;
    }

    sub get_tasks {
        my ($self) = @_;
        return @{$self};
    }
}

SKIP:
{
    skip 'Long running tests not enabled.', 15 unless $ENV{CPANEL_SLOW_TESTS};

    # In case the last test did not succeed.
    cleanup();
    File::Path::mkpath( $statedir );

    my $sched = cPanel::TaskQueue::Scheduler->new( { name => 'tasks', state_dir => $statedir } );
    isa_ok( $sched, 'cPanel::TaskQueue::Scheduler', 'Correct object built.' );

    my $q = MockQueue->new();

    ok( $sched->schedule_task( 'noop 0', {at_time=>time} ), 'command scheduled for now.' );
    is( $sched->process_ready_tasks( $q ), 1, 'task queued' );
    is( ($q->get_tasks())[0]->full_command(), 'noop 0', 'Correct task.' );
    $q->clear_tasks();

    ok( $sched->schedule_task( 'noop 1', {delay_seconds=>1} ), 'command 1 scheduled in one second.' );
    ok( $sched->schedule_task( 'noop 2', {delay_seconds=>1} ), 'command 2 scheduled in one second.' );
    ok( $sched->schedule_task( 'noop 3', {delay_seconds=>1} ), 'command 3 scheduled in one second.' );
    ok( $sched->schedule_task( 'noop 5', {delay_seconds=>4} ), 'command 5 scheduled in four seconds.' );

    is( $sched->how_many_scheduled(), 4, 'All four are scheduled.' );
    # Wait for them to be ready.
    sleep 2;

    is( $sched->process_ready_tasks( $q ), 3, '3 tasks queued' );
    is_deeply(
        [ map { $_->full_command() } $q->get_tasks() ],
        [ map { "noop $_" } 1 .. 3 ],
        'All correct tasks.'
    );
    $q->clear_tasks();

    ok( $sched->schedule_task( 'noop 4', {delay_seconds=>-1} ), 'command 4 scheduled one second ago.' );
    ok( $sched->schedule_task( 'noop 6', {delay_seconds=>2} ), 'command 6 scheduled in two seconds.' );

    sleep 3;
    is( $sched->process_ready_tasks( $q ), 3, '3 tasks queued' );
    is_deeply(
        [ map { $_->full_command() } $q->get_tasks() ],
        [ map { "noop $_" } 4 .. 6 ],
        'All correct tasks.'
    );
    $q->clear_tasks();

    cleanup();
}

{
    File::Path::mkpath( $statedir );

    my $sched = cPanel::TaskQueue::Scheduler->new( { name => 'tasks', state_dir => $statedir } );
    ok( $sched->schedule_task( 'noop 0', {at_time=>time} ), 'command scheduled for now.' );

    eval { $sched->process_ready_tasks(); };
    like( $@, qr/No valid queue/, 'do not process with missing queue' );

    eval { $sched->process_ready_tasks( {} ); };
    like( $@, qr/No valid queue/, 'do not process with non-queue' );

    cleanup();
}

# Clean up after myself
sub cleanup {
    File::Path::rmtree( $tmpdir ) if -d $tmpdir;
}
