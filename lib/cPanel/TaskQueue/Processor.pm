package cPanel::TaskQueue::Processor;

use strict;
#use warnings;

our $VERSION = 0.400;

{
    sub new {
        my ($class) = @_;
        return bless {}, $class;
    }

    sub get_timeout {
        my ($self) = @_;
        return;
    }

    sub is_dupe {
        my ($self, $a, $b) = @_;

        return unless $a->command() eq $b->command();
        my @a_args = $a->args();
        my @b_args = $b->args();
        return unless @a_args == @b_args;

        foreach my $i ( 0 .. $#a_args ) {
            return unless $a_args[$i] eq $b_args[$i];
        }

        return 1;
    }

    sub overrides {
        my ($self, $new, $old) = @_;

        return;
    }

    sub is_valid_args {
        my ($self, $task) = @_;

        return 1;
    }

    sub process_task {
        my ($self, $task) = @_;

        die "No processing has been specified for this task.\n";
    }
}

# To simplify use, here is a simple module that turns a code ref into a valid
# TaskQueue::Processor.
{
    package cPanel::TaskQueue::Processor::CodeRef;
    use base 'cPanel::TaskQueue::Processor';

    {
        sub new {
            my ($class, $args_ref) = @_;
            die "Args must be a hash ref.\n" unless 'HASH' eq ref $args_ref;

            unless ( exists $args_ref->{code} and 'CODE' eq ref $args_ref->{code} ) {
                die "Missing required code parameter.\n";
            }
            return bless { proc=>$args_ref->{code} }, $class;
        }

        # Override the default behavior to call the stored coderef with the
        # arguments supplied in the task. Return whatever the coderef returns.
        sub process_task {
            my ($self, $task) = @_;

            $self->{proc}->( $task->args() );
            return 0;
        }
    }
}

1;

__END__

Copyright (c) 2010, cPanel, Inc. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

