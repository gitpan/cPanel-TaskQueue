package cPanel::TaskQueue;

# cpanel - cPanel/TaskQueue.pm                    Copyright(c) 2010 cPanel, Inc.
#                                                           All rights Reserved.
# copyright@cpanel.net                                         http://cpanel.net
#
# This module handles queuing of tasks for execution. The queue is persistent
# handles consolidating of duplicate tasks.

use strict;
#use warnings;
use YAML::Syck             ();     # Data Serialization
use cPanel::TaskQueue::Task();
use cPanel::TaskQueue::Processor();
use cPanel::CacheFile      ();

our $VERSION = 0.400;

my $WNOHANG;
if ( !exists $INC{'POSIX.pm'} ) {
    # If POSIX is not already loaded, try for CPanel's tiny module.
    ## no critic (ProhibitStringyEval)
    eval 'local $SIG{__DIE__} = "DEFAULT";
          use cPanel::POSIX::Tiny 0.8;  #issafe
          $WNOHANG = &cPanel::POSIX::Tiny::WNOHANG;';
}
if ( !$WNOHANG ) {
    ## no critic (ProhibitStringyEval)
    eval 'use POSIX ();
          $WNOHANG = &POSIX::WNOHANG;';
}

# -----------------------------------------------------------------------------
# Policy code: The following allows is a little weird because its intent is to 
# change the policy by which some code is executed, without adding a gratuitous
# object and polymorphism into the mix.

my $are_policies_set = 0;

#
# This method allows changing the policies for logging and locking.
sub import {
    my $class = shift;
    die 'Not an even number of arguments to the '. __PACKAGE__ ." module\n" if @_ % 2;
    die "Policies already set elsewhere\n" if $are_policies_set;
    return 1 unless @_; # Don't set the policies flag.

    while ( @_ ) {
        my ($policy,$module) = splice( @_, 0, 2 );
        my @methods = ();
        if ( '-logger' eq $policy ) {
            cPanel::CacheFile->import( '-logger' => $module );
        }
        else {
            die "Unrecognized policy '$policy'\n";
        }
    }
    $are_policies_set = 1;
    return 1;
}

# Replacement for List::Util::first, so I don't need to bring in the whole module.
sub _first (&@) {  ## no critic(ProhibitSubroutinePrototypes)
    my $pred = shift;
    local $_;
    foreach (@_) {
        return $_ if $pred->();
    }
    return;
}

# Namespace string used when creating task ids.
my $taskqueue_uuid = "TaskQueue";

{
    # Class-wide definition of the valid processors
    my %valid_processors;
    my $FILETYPE = "TaskQueue"; # Identifier at the beginning of the cache file
    my $CACHE_VERSION = 3; # Cache file version number.

    # Disk Cache & cache file.
    #
    sub get_name                  { return $_[0]->{queue_name}; }
    sub get_default_timeout       { return $_[0]->{default_task_timeout}; }
    sub get_max_timeout           { return $_[0]->{max_task_timeout}; }
    sub get_max_running           { return $_[0]->{max_in_process}; }
    sub get_default_child_timeout { return $_[0]->{default_child_timeout}; }

    # --------------------------------------
    # Class methods

    sub register_task_processor {
        my ($class, $command, $processor) = @_;

        unless ( defined $command and length $command ) {
            cPanel::CacheFile->_throw( "Missing command in register_task_processor.\n" );
        }
        unless ( defined $processor ) {
            cPanel::CacheFile->_throw( "Missing task processor in register_task_processor.\n" );
        }
        if ( exists $valid_processors{$command} ) {
            cPanel::CacheFile->_throw( "Command '$command' already has a TaskQueue::Processor registered.\n" );
        }
        if( 'CODE' eq ref $processor ) {
            $valid_processors{$command} = cPanel::TaskQueue::Processor::CodeRef->new( {code =>$processor} );
            return 1;
        } elsif( eval { $processor->isa( 'cPanel::TaskQueue::Processor' ) } ) {
            $valid_processors{$command} = $processor;
            return 1;
        }

        cPanel::CacheFile->_throw( "Unrecognized task processor object.\n" );
    }

    sub unregister_task_processor {
        my ($class, $command) = @_;

        unless ( defined $command and length $command ) {
            cPanel::CacheFile->_throw( "Missing command in unregister_task_processor.\n" );
        }
        unless ( exists $valid_processors{$command} ) {
            cPanel::CacheFile->_throw( "Command '$command' not registered, ignoring.\n" );
            return;
        }

        delete $valid_processors{$command};
        return 1;
    }

    # Initialize parameters.
    sub new {
        my ( $class, $args_ref ) = @_;
        cPanel::CacheFile->_throw( "Args parameter must be a hash reference\n" ) unless 'HASH' eq ref $args_ref;

        cPanel::CacheFile->_throw( "No caching directory supplied.\n" ) unless exists $args_ref->{cache_dir};
        cPanel::CacheFile->_throw( "No queue name supplied.\n" ) unless exists $args_ref->{name};

        # TODO: Do I want to sanity check the arguments?
        my $self = bless {
            queue_name => $args_ref->{name},
            default_task_timeout => 60,
            max_task_timeout => 300,
            max_in_process => 2,
            default_child_timeout => 3600,
            disk_cache_file => "$args_ref->{cache_dir}/$args_ref->{name}_queue.yaml",
            next_id => 1,
            queue_waiting => [],
            processing_list => [],
            disk_cache => undef,
        }, $class;

        # Make a disk file to track the object.
        my $cache_args = {
            cache_file=>$self->{disk_cache_file}, data_obj => $self,
            exists $args_ref->{cache_timeout} ? (timeout => $args_ref->{cache_timeout}) : (),
            exists $args_ref->{logger} ? (logger => $args_ref->{logger}) : (),
        };
        eval {
            $self->{disk_cache} = cPanel::CacheFile->new( $cache_args );
        };
        if ( $@ ) {
            my $ex = $@;
            # If not a loading error, rethrow.
            cPanel::CacheFile->_throw( $ex ) unless $ex =~ /Not a recognized|Invalid version/;
            cPanel::CacheFile->_warn( $ex );
            cPanel::CacheFile->_warn( "Moving bad cache file and retry.\n" );
            cPanel::CacheFile->_notify(
                'Unable to load TaskQueue metadata',
                "Loading of [$self->{disk_cache_file}] failed: $ex\n"
                . "Moving bad file to [$self->{disk_cache_file}.broken] and retrying.\n"
            );
            unlink "$self->{disk_cache_file}.broken";
            rename $self->{disk_cache_file}, "$self->{disk_cache_file}.broken"; 

            $self->{disk_cache} = cPanel::CacheFile->new( $cache_args );
        }

        # Use incoming parameters to override what's in the file.
        if ( grep { exists $args_ref->{$_ } } qw/default_timeout max_timeout max_running default_child_timeout/ ) {
            my $guard = $self->{disk_cache}->synch();
            $self->{default_task_timeout} = $args_ref->{default_timeout}        if exists $args_ref->{default_timeout};
            $self->{max_task_timeout} = $args_ref->{max_timeout}                if exists $args_ref->{max_timeout};
            $self->{max_in_process} = $args_ref->{max_running}                  if exists $args_ref->{max_running};
            $self->{default_child_timeout} = $args_ref->{default_child_timeout} if exists $args_ref->{default_child_timeout};
            $guard->update_file();
        }

        return $self;
    }

    sub throw {
        my $self = shift;
        return $self->{disk_cache} ? $self->{disk_cache}->throw( @_ ) : cPanel::CacheFile->_throw( @_ );
    }
    sub warn {
        my $self = shift;
        return $self->{disk_cache} ? $self->{disk_cache}->warn( @_ ) : warn @_;
    }
    sub info {
        my $self = shift;
        return $self->{disk_cache} ? $self->{disk_cache}->info( @_ ) : undef;
    }
    # -------------------------------------------------------
    # Public methods
    sub load_from_cache {
        my ($self, $fh) = @_;

        local $/;
        my ($magic, $version, $meta) = YAML::Syck::Load( scalar <$fh> );

        $self->throw( "Not a recognized TaskQueue cache." ) unless $magic eq $FILETYPE;
        $self->throw( "Invalid version of TaskQueue cache." ) unless $version eq $CACHE_VERSION;

        # Next id should continue increasing.
        #   (We might want to deal with wrap-around at some point.)
        $self->{next_id} = $meta->{nextid} if $meta->{nextid} > $self->{next_id};

        # TODO: Add more sanity checks here.
        $self->{default_task_timeout} = $meta->{def_task_to}   if $meta->{def_task_to} > 0;
        $self->{max_task_timeout} = $meta->{max_task_to}       if $meta->{max_task_to} > 0;
        $self->{max_in_process} = $meta->{max_running}         if $meta->{max_running} > 0;
        $self->{default_child_timeout} = $meta->{def_child_to} if $meta->{def_child_to} > 0;

        # Clean queues that have been read from disk.
        $self->{queue_waiting} = [
            grep { defined $_ and eval { $_->isa( 'cPanel::TaskQueue::Task' ) } }
            @{$meta->{waiting_queue}}
        ];
        $self->{processing_list} = [
            grep { defined $_ and eval { $_->isa( 'cPanel::TaskQueue::Task' ) } }
            @{$meta->{processing_queue}}
        ];

        return 1;
    }


    sub save_to_cache {
        my ($self,$fh) = @_;

        my $meta = {
            nextid => $self->{next_id},
            def_task_to => $self->{default_task_timeout},
            max_task_to => $self->{max_task_timeout},
            max_running => $self->{max_in_process},
            def_child_to => $self->{default_child_timeout},
            waiting_queue => $self->{queue_waiting},
            processing_queue => $self->{processing_list},
        };
        return print $fh YAML::Syck::Dump( $FILETYPE, $CACHE_VERSION, $meta );
    }

    sub queue_task {
        my ( $self, $command ) = @_;

        $self->throw( "Cannot queue an empty command." ) unless defined $command;

        if ( eval { $command->isa( 'cPanel::TaskQueue::Task' ) } ) {
            if ( 0 == $command->retries_remaining() ) {
                $self->info( 'Task with 0 retries not queued.' );
                return;
            }
            my $task = $command->mutate( {timeout => $self->{default_child_timeout}} );
            return $self->_queue_the_task( $task );
        }

        # must have non-space characters to be a command.
        $self->throw( "Cannot queue an empty command." ) unless $command =~ /\S/;

        my $task = cPanel::TaskQueue::Task->new(
            {cmd=>$command, nsid=>$taskqueue_uuid, id=>$self->{next_id}++,
             timeout=>$self->{default_child_timeout}}
        );
        return $self->_queue_the_task( $task );
    }

    sub unqueue_task {
        my ( $self, $uuid ) = @_;

        unless ( _is_valid_uuid( $uuid ) ) {
            $self->throw( "No Task uuid argument passed to unqueue_cmd." );
        }

        # Lock the queue before we begin accessing it.
        my $guard = $self->{disk_cache}->synch();
        my $old_count = @{$self->{queue_waiting}};

        $self->{queue_waiting} = [ grep { $_->uuid() ne $uuid } @{$self->{queue_waiting}} ];
        # All changes complete, save to disk.
        $guard->update_file();
        return $old_count > @{$self->{queue_waiting}};
    }

    sub is_task_queued {
        my ( $self, $uuid ) = @_;

        unless ( _is_valid_uuid( $uuid ) ) {
            $self->throw( "No Task uuid argument passed to is_task_queued." );
        }

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        return defined _first { $_->uuid() eq $uuid } @{$self->{queue_waiting}};
    }

    sub is_task_processing {
        my ( $self, $uuid ) = @_;

        unless ( _is_valid_uuid( $uuid ) ) {
            $self->throw( "No Task uuid argument passed to is_task_processing" );
        }

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        return defined _first { $_->uuid() eq $uuid } @{$self->{processing_list}};
    }

    sub find_task {
        my ($self, $uuid) = @_;

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        my $task = _first { $_->uuid() eq $uuid } @{$self->{queue_waiting}}, @{$self->{processing_list}};

        return unless defined $task;
        return $task->clone();
    }

    sub find_command {
        my ($self, $command) = @_;

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        my $task = _first { $_->command() eq $command } @{$self->{queue_waiting}}, @{$self->{processing_list}};

        return unless defined $task;
        return $task->clone();
    }

    sub find_commands {
        my ($self, $command) = @_;

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        my @tasks = grep { $_->command() eq $command } @{$self->{processing_list}}, @{$self->{queue_waiting}};

        return unless @tasks;
        return map { $_->clone() } @tasks;
    }

    sub how_many_queued {
        my ($self) = @_;

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        return scalar @{$self->{queue_waiting}};
    }

    sub how_many_in_process {
        my ($self) = @_;

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        return scalar @{$self->{processing_list}};
    }

    sub has_work_to_do {
        my ($self) = @_;

        # Update from disk, but don't worry about lock. Possibly information only.
        $self->{disk_cache}->synch();
        $self->_clean_completed_tasks();

        return scalar(@{$self->{processing_list}}) < $self->{max_in_process} && 0 != @{$self->{queue_waiting}};
    }

    sub peek_next_task {
        my ($self) = @_;

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();
        return unless @{$self->{queue_waiting}};

        return $self->{queue_waiting}->[0]->clone();
    }

    sub process_next_task {
        my ($self) = @_;

        # Lock the queue before doing any manipulations.
        my $guard = $self->{disk_cache}->synch();

        $self->_handle_already_running_tasks( $guard );

        if ( _first { !defined $_ } @{$self->{queue_waiting}} ) {
            # Somehow some undefined tasks got into the queue, log and
            # delete them.
            $self->warn( "Undefined tasks found in the queue, removing..." );
            $self->{queue_waiting} = [ grep { defined $_ } @{$self->{queue_waiting}} ];
            # Since we've changed the wait queue, we need to update disk file,
            # otherwise changes could be lost if we return early, below.
            $guard->update_file();
        }
        # We can now schedule new tasks
        return 1 unless @{$self->{queue_waiting}};
        my $task = shift @{$self->{queue_waiting}};

        # can fail if the processor for this command was removed.
        unless ( exists $valid_processors{$task->command()} ) {
            # TODO: log missing processor.
            $self->warn( "No processor found for '".$task->command()."'." );
            return 1;
        }
        my $processor = $valid_processors{$task->command()};

        $task->begin();
        push @{$self->{processing_list}}, $task;

        # Finished making changes, save to disk.
        $guard->update_file();

        # I don't want to stay locked while processing.
        my $pid;
        my $ex;
        $guard->call_unlocked(
            sub {
                my $orig_alarm;
                eval {
                    local $SIG{'ALRM'} = sub { die "time out reached\n"; };
                    $orig_alarm = alarm( $self->_timeout( $processor ) );
                    $pid = $processor->process_task( $task->clone() );
                    alarm $orig_alarm;
                };
                $ex = $@; # save exception for later
                alarm $orig_alarm if $@;
            }
        );

        # Deal with a child process or remove from processing.
        if( $pid ) {
            $task->set_pid( $pid );
        }
        else {
            my $uuid = $task->uuid();
            # remove finished item from the list.
            $self->{processing_list} = [ grep { $_->uuid() ne $uuid } @{$self->{processing_list}} ];
        }

        # Don't lose any exceptions.
        if ( $ex ) {
            if ( $ex eq "time out reached\n" ) {
                # TODO: log timeout condition.
                $self->warn( "Task '" . $task->full_command() . "' timed out during processing." );
            }
            else {
                $self->throw( $ex );
            }
        }

        # Finished making changes, save to disk.
        $guard->update_file();
        return $pid == 0;
    }

    sub finish_all_processing {
        my ($self) = @_;

        # Lock the queue for manipulation and to reduce new task items.
        my $guard = $self->{disk_cache}->synch();
        while ( @{$self->{processing_list}} ) {
            # we still need to remove some
            my $pid;
            # TODO: Might want to deal with timeouts or something to keep this
            #   from waiting forever.
            $guard->call_unlocked( sub { $pid = waitpid( -1, 0 ) } );

            next unless $pid;
            $self->{processing_list} = [
                grep { 0 == waitpid( $_->pid(), $WNOHANG ) }
                grep { $_->pid() && $_->pid() != $pid } @{$self->{processing_list}}
            ];
            $guard->update_file();
        }
        return;
    }

    sub snapshot_task_lists {
        my ($self) = @_;

        # Update from disk, but don't worry about lock. Information only.
        $self->{disk_cache}->synch();

        return {
            waiting => [ map { $_->clone() } @{$self->{queue_waiting}} ],
            processing => [ map { $_->clone() } @{$self->{processing_list}} ],
        };
    }

    # ---------------------------------------------------------------
    #  Private Methods.

    # Test whether the supplied task descriptor duplicates any in the queue.
    sub _is_duplicate_command {
        my ($self, $task) = @_;
        my $proc = $valid_processors{$task->command()};

        return defined _first { $proc->is_dupe( $task, $_ ) } reverse @{$self->{queue_waiting}}; 
    }

    sub _process_overrides {
        my ($self, $task) = @_;
        my $proc = $valid_processors{$task->command()};

        $self->{queue_waiting} = [
            grep { !$proc->overrides( $task, $_ ) } @{$self->{queue_waiting}}
        ];

        return;
    }

    # Perform all of the steps needed to put a task in the queue.
    # Only queues legal commands, that are not duplicates.
    # If successful, returns the new queue id.
    sub _queue_the_task {
        my ($self, $task) = @_;

        # Validate the incoming task.
        # It must be a command we recognize, have valid parameters, and not be a duplicate.
        unless ( exists $valid_processors{$task->command()} ) {
            $self->throw( "No known processor for '" . $task->command() ."'." );
        }
        unless ( $valid_processors{$task->command()}->is_valid_args( $task ) ) {
            $self->throw( "Requested command [". $task->full_command() ."] has invalid arguments." );
        }

        # Lock the queue here, because we begin looking what's currently in the queue
        #  and don't want it to change under us.
        my $guard = $self->{disk_cache}->synch();
        # Check overrides first and then duplicate. This seems backward, but
        # actually is not. See the tests labelled 'override, not dupe' in
        # t/07.task_queue_dupes_and_overrides.t for the case that makes sense.
        #
        # By making the task override its duplicates as well, we can get the
        # behavior you expect when you think this is wrong. If we swap the order of
        # the tests there's no way to force the right behavior.
        $self->_process_overrides( $task );
        return if $self->_is_duplicate_command( $task );

        push @{$self->{queue_waiting}}, $task;

        # Changes to the queue are complete, save to disk.
        $guard->update_file();

        return $task->uuid();
    }

    # Use either the timeout in the processor or the default timeout,
    #  unless that is greater than the max, the use the max.
    sub _timeout {
        my ($self, $processor) = @_;

        my $timeout = $processor->get_timeout() || $self->{default_task_timeout};

        return $timeout > $self->{max_task_timeout} ? $self->{max_task_timeout} : $timeout;
    }

    # Clean the processing list of any child tasks that completed since the
    # last time we looked. The $guard object is an optional parameter. If
    # a guard does not exist, we will create one if necessary for any locking.
    sub _clean_completed_tasks {
        my ($self, $guard) = @_;

        my $num_processing = @{$self->{processing_list}};
        return unless $num_processing;

        # Remove tasks that have already completed from the in-memory list.
        $self->_remove_completed_tasks_from_list();
        return if @{$self->{processing_list}} == $num_processing;

        # Was not locked, so we need to lock and remove completed tasks again.
        if ( !$guard ) {
            $guard = $self->{disk_cache}->synch();
            $self->_remove_completed_tasks_from_list();
        }
        $guard->update_file();
        return;
    }

    # Remove child tasks that have completed executing from the processing
    # list in memory.
    sub _remove_completed_tasks_from_list {
        my ($self) = @_;
        $self->{processing_list} = [
            grep { $_->pid() && 0 == waitpid( $_->pid(), $WNOHANG ) }
            @{$self->{processing_list}}
        ];
        return;
    }

    # Handle the case of too many tasks being processed
    # Are there too many in processing?
    #    check to see if any processes are complete, and remove them.
    # Are there still too many in processing?
    #    waitpid - blocks.
    #    remove process that completed
    #    remove any other completed processes
    sub _handle_already_running_tasks {
        my ($self, $guard) = @_;

        $self->_clean_completed_tasks( $guard );

        while ( $self->{max_in_process} <= scalar @{$self->{processing_list}} ) {
            # we still need to remove some
            my $pid;
            # TODO: Might want to deal with timeouts or something to keep this
            #   from waiting forever.
            $guard->call_unlocked( sub { $pid = waitpid( -1, 0 ) } );

            next if $pid < 1;
            $self->{processing_list} = [
                grep { $_->pid() != $pid } @{$self->{processing_list}}
            ];
            $guard->update_file();
        }
        $self->_clean_completed_tasks( $guard );
        return;
    }

    sub _is_valid_uuid {
        return cPanel::TaskQueue::Task::is_valid_taskid( shift );
    }
}

# One guaranteed processor, the no-operation case.
__PACKAGE__->register_task_processor( 'noop', sub {} );

1;    # Magic true value required at the end of the module

__END__

Copyright (c) 2010, cPanel, Inc. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

