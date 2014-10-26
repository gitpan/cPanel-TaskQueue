#!/usr/bin/perl

# Test the cPanel::TaskQueue::Scheduler module.
#
# This tests the code for tasks scheduled for some time in the future. Since these
#  tasks are, by necessity, slower to execute than we probably don't want to run
#  as a normal test. This code is disabled, unless it is run with the environment
#  variable CPANEL_SLOW_TESTS set.


use strict;
use FindBin;
use lib "$FindBin::Bin/mocks";
use File::Path ();

use Test::More tests => 85;
use cPanel::TaskQueue::Scheduler;

my $tmpdir = './tmp';
my $statedir = $tmpdir;

# In case the last test did not succeed.
cleanup();
File::Path::mkpath( $tmpdir ) or die "Unable to create tmpdir: $!";

eval {
    cPanel::TaskQueue::Scheduler->new( {state_dir => $statedir} );
};
like( $@, qr/scheduler name/, 'Must supply a name.' );

my $sched = cPanel::TaskQueue::Scheduler->new(
    { name => 'tasks', state_dir => $statedir }
);
isa_ok( $sched, 'cPanel::TaskQueue::Scheduler', 'Correct object built.' );

# Internal method, do not use
is( $sched->_state_file, "$statedir/tasks_sched.stor", 'State file has correct form.' );

# Check failures to schedule
eval {
    $sched->schedule_task();
};
like( $@, qr/empty command/, 'Must supply the command.' );
eval {
    $sched->schedule_task( '   ', {delay_seconds=>5} );
};
like( $@, qr/empty command/, 'Must supply a non-empty command.' );
eval {
    $sched->schedule_task( 'noop 0' );
};
like( $@, qr/not a hash ref/, 'Must supply the time hash.' );
eval {
    $sched->schedule_task( 'noop 0', 10 );
};
like( $@, qr/not a hash ref/, 'Second arg must be a hash ref' );

# Check ability to schedule
my @qid;
my $time1 = time;

push @qid, $sched->schedule_task( 'noop', {delay_seconds=>3} );
my $next_secs = $sched->seconds_until_next_task();
if( $next_secs == 3 or $next_secs == 2 ) {
    pass( 'Correct wait time.' );
}
else {
    is( $next_secs, 3, 'Correct wait time.' );
}
ok( $qid[0], 'task scheduled' );
ok( $sched->is_task_scheduled( $qid[0] ), 'Scheduler thinks it is there' );
my $t = $sched->peek_next_task();
is( $t->uuid(), $qid[0], 'Correct id in the scheduler' );
is( $sched->when_is_task_scheduled( $qid[0] ), $time1+3, 'Scheduled at correct time.' );

my $time2 = time+4;
push @qid, $sched->schedule_task( 'noop 1', {at_time=>$time2} );
ok( $qid[1], 'second: task scheduled' );
ok( $sched->is_task_scheduled( $qid[1] ), 'second: Scheduler thinks it is there' );
$t = $sched->peek_next_task();
isnt( $t->uuid(), $qid[1], 'second: did not move to front' );
is( $sched->when_is_task_scheduled( $qid[1] ), $time2, 'second: Scheduled at correct time.' );

push @qid, $sched->schedule_task( 'noop 2', {at_time=>$time2} );
ok( $qid[2], 'third: task scheduled' );
ok( $sched->is_task_scheduled( $qid[2] ), 'third: Scheduler thinks it is there' );
$t = $sched->peek_next_task();
isnt( $t->uuid(), $qid[2], 'third: did not move to front' );
is( $sched->when_is_task_scheduled( $qid[2] ), $time2, 'third: Scheduled at correct time.' );

my $scheds = $sched->snapshot_task_schedule();
is( scalar( @{$scheds} ), 3, 'Correct number in schedule' );
is( $scheds->[0]->{task}->uuid(), $sched->peek_next_task()->uuid(), 'first task is correct' );
is( $scheds->[0]->{time}-time, $sched->seconds_until_next_task(), 'first time is correct' );
# Ensure actually sorted.
$scheds = [ sort { $a->{time} <=> $b->{time} }  @{$scheds} ];
foreach my $i ( 0 .. $#qid ) {
    is( $scheds->[$i]->{task}->uuid(), $qid[$i], "$i: task scheduled." );
}

my $i = 0;
while ( my $first = $sched->peek_next_task() ) {
    is( $first->uuid(), $qid[$i], "$i: Id in correct order." );
    ok( $sched->unschedule_task( $first->uuid() ), "$i: Remove queue front." );
    ++$i;
}

my $stime = time;
# Insert in order
check_task_insertion( $sched, 'in-order',
    [ 'noop 0', $stime+0 ],
    [ 'noop 1', $stime+1 ],
    [ 'noop 2', $stime+2 ],
    [ 'noop 3', $stime+3 ],
);

# Insert in reverse order
check_task_insertion( $sched, 'reverse',
    [ 'noop 3', $stime+3 ],
    [ 'noop 2', $stime+2 ],
    [ 'noop 1', $stime+1 ],
    [ 'noop 0', $stime+0 ],
);

# Insert in order with duplicates
check_task_insertion( $sched, 'in-order, dupes',
    [ 'noop 0', $stime+0 ],
    [ 'noop 1', $stime+1 ],
    [ 'noop 2', $stime+1 ],
    [ 'noop 4', $stime+2 ],
    [ 'noop 5', $stime+3 ],
    [ 'noop 3', $stime+1 ],
);

my $sched2 = cPanel::TaskQueue::Scheduler->new(
    { name => 'tasks', state_dir => $statedir }
);

ok( !defined $sched->peek_next_task(), 'Return undef task if no tasks.' );
ok( !defined $sched->seconds_until_next_task(), 'Return undef seconds if no tasks.' );

ok( my $t1 = $sched->schedule_task( 'noop 1', {delay_seconds=>0} ), 'Scheduled in first' );
ok( $sched2->is_task_scheduled( $t1 ), 'Exists in second' );
ok( $sched2->unschedule_task( $t1 ), 'Removed from second' );
ok( !$sched->is_task_scheduled( $t1 ), 'Gone in first' );

# Make certain the scheduler is empty.
while ( my $task = $sched->peek_next_task() ) {
    $sched->unschedule_task( $task->uuid() );
}

ok( $sched->schedule_task( 'noop 0', {} ), 'Scheduled with no time setting.' );
ok( $sched->seconds_until_next_task() <= 0, 'Scheduled right now (or in the last second.' );

cleanup();


sub insert_tasks {
    my $s = shift;
    my $label = shift;

    my $i = 0;
    foreach my $cmd ( @_ ) {
        my $qid = $s->schedule_task( $cmd->[0], {at_time=>$cmd->[1]} );
        ok( $qid, "$label [$i]: scheduled successfully" );
        ++$i;
    }
}

sub remove_and_check_tasks {
    my $s = shift;
    my $label = shift;
    
    is( scalar(@_), $s->how_many_scheduled(), "$label: correct number scheduled" );
    my $i = 0;
    while ( my $first = $s->peek_next_task() ) {
        is( $first->get_arg( 0 ), $i, "$label [$i]: correct order." );
        ok( $s->unschedule_task( $first->uuid() ), "$label [$i]: Remove queue front." );
        ++$i;
    }
}

sub check_task_insertion {
    my $s = shift;
    my $label = shift;

    insert_tasks( $s, $label, @_ );
    remove_and_check_tasks( $s, $label, @_ );
}

# Clean up after myself
sub cleanup {
    File::Path::rmtree( $tmpdir ) if -d $tmpdir;
}
