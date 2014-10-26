package cPanel::TaskQueue::Ctrl;
BEGIN {
  $cPanel::TaskQueue::Ctrl::VERSION = '0.503_04';
}

# cpanel - cPanel/TaskQueue/Ctrl.pm               Copyright(c) 2009 cPanel, Inc.
#                                                           All rights Reserved.
# copyright@cpanel.net                                         http://cpanel.net

use warnings;
use strict;

use cPanel::TaskQueue                ();
use cPanel::TaskQueue::Scheduler     ();
use cPanel::TaskQueue::PluginManager ();
use Text::Wrap                       ();

my @required = qw(qdir qname);
my %validate = (
    'qdir'   => sub { return -d $_[0]; },
    'qname'  => sub { return defined $_[0] && length $_[0]; },
    'sdir'   => sub { return -d $_[0]; },
    'sname'  => sub { return defined $_[0] && length $_[0]; },
    'logger' => sub { return 1; },
    'out'    => sub { return 1; },
);

my %commands = (
    queue => {
        code     => \&queue_tasks,
        synopsis => 'queue "cmd string" ...',
        help     => '    Adds the specified commands to the TaskQueue. Prints the task id on
    success or an error on failure. Multiple command strings may be supplied,
    and each will be queued in turn.',
    },
    pause => {
        code => sub { $_[1]->pause_processing(); return; },
        synopsis => 'pause',
        help     => '    Pause the processing of waiting tasks from the TaskQueue.',
    },
    resume => {
        code => sub { $_[1]->resume_processing(); return; },
        synopsis => 'resume',
        help     => '    Resume the processing of waiting tasks from the TaskQueue.',
    },
    unqueue => {
        code     => \&unqueue_tasks,
        synopsis => 'unqueue {taskid} ...',
        help     => '    Removes the tasks identified by taskids from the TaskQueue.',
    },
    schedule => {
        code     => \&schedule_tasks,
        synopsis => 'schedule [at {time}] "cmd string" ... | schedule after {seconds} "cmd string" ...',
        help     => '    Schedule the specified commands for later execution. If the "at"
    subcommand is used, the next arguemnt is expected to be a UNIX epoch time for the
    command to be queued. The "after" subcommand specified a delay in seconds after
    which the command is queued.',
    },
    unschedule => {
        code     => \&unschedule_tasks,
        synopsis => 'unschedule {taskid} ...',
        help     => '    Removes the tasks identified by taskids from the TaskQueue Scheduler.',
    },
    list => {
        code     => \&list_tasks,
        synopsis => 'list [verbose] [active|deferred|waiting|scheduled]',
        help     => '    List current outstanding tasks. With the verbose flag, list more
    information on each task. Specify the specific subset of tasks to limit output.',
    },
    find => {
        code     => \&find_task,
        synopsis => 'find task {taskid} | find command {text}',
        help     => '    Find a task in the queue by either task ID or a portion of the command
    string.',
    },
    plugins => {
        code     => \&list_plugins,
        synopsis => 'plugins [verbose]',
        help     => '    List the names of the plugins that have been loaded.',
    },
    commands => {
        code     => \&list_commands,
        synopsis => 'commands [modulename]',
        help     => '    List the commands that are currently supported by the loaded plugins.
    If a module name is supplied, only the commands from that plugin are displayed.',
    },
    status => {
        code     => \&queue_status,
        synopsis => 'status',
        help     => '    Print the status of the Task Queue and Scheduler.',
    },
);

sub new {
    my ( $class, $args ) = @_;

    $args = {} unless defined $args;
    die "Argument to new is not a hashref.\n" unless 'HASH' eq ref $args;
    foreach my $arg (@required) {
        die "Missing required '$arg' argument.\n" unless defined $args->{$arg} and length $args->{$arg};
    }
    my $self = {};
    foreach my $arg ( keys %{$args} ) {
        next unless exists $validate{$arg};
        die "Value of '$arg' parameter ($args->{$arg}) is not valid.\n"
          unless $validate{$arg}->( $args->{$arg} );
        $self->{$arg} = $args->{$arg};
    }
    $self->{sdir} ||= $self->{qdir} if $self->{sname};
    $self->{out} ||= \*STDOUT;

    return bless $self, $class;
}

sub run {
    my ( $self, $cmd, @args ) = @_;
    die "No command supplied to run.\n" unless $cmd;
    die "Unrecognized command '$cmd' to run.\n" unless exists $commands{$cmd};

    $commands{$cmd}->{code}->( $self->{out}, $self->_get_queue(), $self->_get_scheduler(), @args );
}

sub synopsis {
    my ( $self, $cmd ) = @_;

    if ($cmd) {
        return $commands{$cmd}->{'synopsis'}, '';
    }
    return map { $commands{$_}->{'synopsis'}, '' } sort keys %commands;
}

sub help {
    my ( $self, $cmd ) = @_;
    if ($cmd) {
        return @{ $commands{$cmd} }{ 'synopsis', 'help' }, '';
    }
    return map { @{ $commands{$_} }{ 'synopsis', 'help' }, '' } sort keys %commands;
}

sub _get_queue {
    my ($self) = @_;
    return cPanel::TaskQueue->new(
        {
            name => $self->{qname}, state_dir => $self->{qdir},
            ( exists $self->{logger} ? ( logger => $self->{logger} ) : () ),
        }
    );
}

sub _get_scheduler {
    my ($self) = @_;

    # Explicitly returning undef because should only be called in scalar context.
    # I want it to either return a scheduler or undef, returning an empty list
    # never makes sense in this situation.
    return undef unless exists $self->{sdir};    ## no critic (ProhibitExplicitReturnUndef)
    return cPanel::TaskQueue::Scheduler->new(
        {
            name => $self->{sname}, state_dir => $self->{sdir},
            ( exists $self->{logger} ? ( logger => $self->{logger} ) : () ),
        }
    );
}

sub queue_tasks {
    my $fh    = shift;
    my $queue = shift;
    my $sched = shift;
    die "No command to queue.\n" unless @_;

    foreach my $cmdstring (@_) {
        eval {
            print $fh "Id: ", $queue->queue_task($cmdstring), "\n";
            1;
        } or do {
            print $fh "ERROR: $@\n";
        };
    }
    return;
}

sub unqueue_tasks {
    my $fh    = shift;
    my $queue = shift;
    my $sched = shift;
    die "No task ids to unqueue.\n" unless @_;

    my $count = 0;
    foreach my $ids (@_) {
        eval {
            ++$count if $queue->unqueue_task($ids);
            1;
        } or do {
            print $fh "ERROR: $@\n";
        };
    }
    print $fh "$count tasks unqueued\n";
    return;
}

sub schedule_tasks {
    my $fh     = shift;
    my $queue  = shift;
    my $sched  = shift;
    my $subcmd = shift;
    die "No command to schedule.\n" unless defined $subcmd;

    my $args = {};
    if ( $subcmd eq 'at' ) {
        $args->{'at_time'} = shift;
    }
    elsif ( $subcmd eq 'after' ) {
        $args->{'delay_seconds'} = shift;
    }
    else {
        unshift @_, $subcmd;
    }

    die "No command to schedule.\n" unless @_;
    foreach my $cmdstring (@_) {
        eval {
            print $fh "Id: ", $sched->schedule_task( $cmdstring, $args ), "\n";
            1;
        } or do { print $fh "ERROR: $@\n"; };
    }
    return;
}

sub unschedule_tasks {
    my $fh    = shift;
    my $queue = shift;
    my $sched = shift;
    die "No task ids to unschedule.\n" unless @_;

    my $count = 0;
    foreach my $ids (@_) {
        eval {
            ++$count if $sched->unschedule_task($ids);
            1;
        } or do {
            print $fh "ERROR: $@\n";
        };
    }
    print $fh "$count tasks unscheduled\n";
    return;
}

sub _any_is {
    my $match = shift;
    return unless defined $match;
    foreach (@_) {
        return 1 if $match eq $_;
    }
    return;
}

sub find_task {
    my $fh    = shift;
    my $queue = shift;
    my $sched = shift;
    my ( $subcmd, $match ) = @_;

    if ( !defined $match ) {
        print $fh "No matching criterion.\n";
        return;
    }

    my @t;
    if ( $subcmd eq 'task' ) {
        @t = $queue->find_task($match);
    }
    elsif ( $subcmd eq 'command' ) {
        @t = $queue->find_commands($match);
    }
    else {
        print $fh "'$subcmd' is not a valid find type.\n";
        return;
    }
    if (@t) {
        foreach (@t) {
            _verbosely_print_task( $fh, $_ );
            print $fh "\n";
        }
    }
    else {
        print $fh "No matching task found.\n";
    }
}

sub list_tasks {
    my $fh    = shift;
    my $queue = shift;
    my $sched = shift;
    my $print = \&_print_task;
    if ( _any_is( 'verbose', @_ ) ) {
        $print = \&_verbosely_print_task;
        @_ = grep { $_ ne 'verbose' } @_;
    }

    @_ = qw/active deferred waiting scheduled/ unless @_;
    my $lists = $queue->snapshot_task_lists;

    if ( _any_is( 'active', @_ ) ) {
        print $fh "Active Tasks\n-------------\n";
        if ( @{ $lists->{'processing'} } ) {
            foreach my $t ( @{ $lists->{'processing'} } ) {
                $print->( $fh, $t );
            }
        }
    }

    if ( _any_is( 'deferred', @_ ) ) {
        print $fh "Deferred Tasks\n-------------\n";
        if ( @{ $lists->{'deferred'} } ) {
            foreach my $t ( @{ $lists->{'deferred'} } ) {
                $print->( $fh, $t );
                print $fh "\n";
            }
        }
    }

    if ( _any_is( 'waiting', @_ ) ) {
        print $fh "Waiting Tasks\n-------------\n";
        if ( @{ $lists->{'waiting'} } ) {
            foreach my $t ( @{ $lists->{'waiting'} } ) {
                $print->( $fh, $t );
                print $fh "\n";
            }
        }
    }

    return unless $sched;
    if ( _any_is( 'scheduled', @_ ) ) {
        my $sched_tasks = $sched->snapshot_task_schedule();
        print $fh "Scheduled Tasks\n---------------\n";
        if ( @{$sched_tasks} ) {
            foreach my $st ( @{$sched_tasks} ) {
                $print->( $fh, $st->{task} );
                print $fh "\tScheduled for: ", scalar( localtime $st->{time} ), "\n";
                print $fh "\n";
            }
        }
    }
}

sub list_plugins {
    my $fh    = shift;
    my $queue = shift;
    my $sched = shift;

    if ( defined $_[0] && $_[0] eq 'verbose' ) {
        my $plugins = cPanel::TaskQueue::PluginManager::get_plugins_hash();
        foreach my $plug ( sort keys %{$plugins} ) {
            print $fh "* $plug\n\t", join( "\n\t", map { "- $_" } sort @{ $plugins->{$plug} } ), "\n\n";
        }
    }
    else {
        print $fh join( "\n", map { "* $_" } cPanel::TaskQueue::PluginManager::list_loaded_plugins() ), "\n\n";
    }
}

sub list_commands {
    my $fh     = shift;
    my $queue  = shift;
    my $sched  = shift;
    my $module = shift;

    my $plugins = cPanel::TaskQueue::PluginManager::get_plugins_hash();
    if ( !defined $module ) {
        my @commands = sort map { @{$_} } values %{$plugins};
        print $fh join( "\n", ( map { "* $_" } @commands ) ), "\n\n";
    }
    elsif ( exists $plugins->{$module} ) {
        my @commands = sort @{ $plugins->{$module} };
        print $fh join( "\n", ( map { "* $_" } @commands ) ), "\n\n";
    }
    else {
        print $fh "No module named $module was loaded.\n";
    }
}

sub queue_status {
    my $fh    = shift;
    my $queue = shift;
    my $sched = shift;

    print $fh "Queue:\n";
    print $fh "\tQueue Name:\t",    $queue->get_name,                  "\n";
    print $fh "\tDef. Timeout:\t",  $queue->get_default_timeout,       "\n";
    print $fh "\tMax Timeout:\t",   $queue->get_max_timeout,           "\n";
    print $fh "\tMax # Running:\t", $queue->get_max_running,           "\n";
    print $fh "\tChild Timeout:\t", $queue->get_default_child_timeout, "\n";
    print $fh "\tProcessing:\t",    $queue->how_many_in_process,       "\n";
    print $fh "\tQueued:\t\t",      $queue->how_many_queued,           "\n";
    print $fh "\tDeferred:\t\t",    $queue->how_many_deferred,         "\n";

    if ( defined $sched ) {
        print $fh "Scheduler:\n";
        print $fh "\tSchedule Name:\t", $sched->get_name,           "\n";
        print $fh "\tScheduled:\t",     $sched->how_many_scheduled, "\n";
        my $seconds = $sched->seconds_until_next_task;
        print $fh "\tTime to next:\t$seconds\n" if defined $seconds;
    }
    print $fh "\n";
}

sub _print_task {
    my ( $fh, $task ) = @_;
    print $fh '[', $task->uuid, ']: ', $task->full_command, "\n";
    print $fh "\tQueued:  ", scalar( localtime $task->timestamp ), "\n";
    print $fh "\tStarted: ", scalar( localtime $task->started ), "\n" if defined $task->started;
}

sub _verbosely_print_task {
    my ( $fh, $task ) = @_;
    print $fh '[', $task->uuid, ']: ', $task->full_command, "\n";
    print $fh "\tQueued:  ", scalar( localtime $task->timestamp ), "\n";
    print $fh "\tStarted: ", ( defined $task->started ? scalar( localtime $task->started ) : 'N/A' ), "\n";
    print $fh "\tChild Timeout: ", $task->child_timeout, " secs\n";
    print $fh "\tPID: ", ( $task->pid || 'None' ), "\n";
    print $fh "\tRemaining Retries: ", $task->retries_remaining, "\n";
}

1;

__END__

Copyright (c) 2010, cPanel, Inc. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

