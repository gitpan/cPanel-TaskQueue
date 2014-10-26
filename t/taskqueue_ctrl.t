#!/usr/bin/perl

# Test the cPanel::TaskQueue module.
#

use strict;
use FindBin;
use lib "$FindBin::Bin/mocks";
use File::Path ();
use Cwd;

use Test::More 'no_plan'; #tests => 42;
use cPanel::TaskQueue::Ctrl;

my $tmpdir = './tmp';
my $statedir = "$tmpdir/state_test";

# In case the last test did not succeed.
cleanup();
File::Path::mkpath( $statedir ) or die "Unable to create tmpdir: $!";

eval { cPanel::TaskQueue::Ctrl->new( 'fred' ); };
like( $@, qr/not a hashref/, 'Ctrl::new requires a hashref.' );

eval { cPanel::TaskQueue::Ctrl->new( { qname => 'test' } ); };
like( $@, qr/required 'qdir'/, 'Required qdir test.' );

eval { cPanel::TaskQueue::Ctrl->new( { qdir => $statedir } ); };
like( $@, qr/required 'qname'/, 'Required qname test.' );

my $output;
my $ctrl = cPanel::TaskQueue::Ctrl->new( { qdir => $statedir, qname => 'test', out => \$output } );
isa_ok( $ctrl, 'cPanel::TaskQueue::Ctrl' );

my @commands = sort qw/queue pause resume unqueue schedule unschedule list find plugins commands status convert/;
foreach my $cmd (@commands) {
    my @ret = $ctrl->synopsis( $cmd );
    like( $ret[0], qr/^$cmd/, "$cmd: Found synopsis" );
    is( $ret[1], '', "$cmd: spacer" );

    @ret = $ctrl->help( $cmd );
    like( $ret[0], qr/^$cmd/, "$cmd: Found synopsis" );
    isnt( $ret[1], '', "$cmd: found help text" );
    is( $ret[2], '', "$cmd: spacer" );
}

{
    my @synopsis = $ctrl->synopsis();
    is( scalar(@synopsis), 2*@commands, 'The right number of commands are returned for synopsis' );
    my @help = $ctrl->help();
    is( scalar(@help), 3*@commands, 'The right number of commands are returned for help' );
}

cleanup();

# Clean up after myself
sub cleanup {
    File::Path::rmtree( $tmpdir );
}
