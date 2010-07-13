#!/usr/bin/perl

# Test the cPanel::CacheFile module.
#

use FindBin;
use lib "$FindBin::Bin/mocks";
use File::Path ();
use Data::Dumper;

use Test::More tests => 24;
use cPanel::CacheFile;
use MockCacheable;

my $dir = '/tmp/cache_test';
my $file = "$dir/cache_dir/cache_file";
my $lockname = "$file.lock";

# TODO: Need to testing for timeout logic, but it would slow down the tests.
#   Decide how I would like to turn it on provisionally: cmdline, env, etc.

# clean up if last run failed.
cleanup();

# test valid creation
my $mock_obj = MockCacheable->new;

my $cache = cPanel::CacheFile->new( { cache_file => $file, data_obj => $mock_obj } );
isa_ok( $cache, 'cPanel::CacheFile' );

ok( -e $file, "Cache file should have been created." );

ok( !$mock_obj->{load_called}, "File didn't exist, should not have loaded." );
is( $mock_obj->{save_called}, 1, "File didn't exist, should have saved." );

ok( !-e $lockname, "File not locked at this time." );

{
    my $msg;
    local $SIG{__WARN__} = sub { $msg = join( ' ', @_ ); };
    $cache->warn( "This is a warning\n" );
    is( $msg, "This is a warning\n", 'warn method works.' );

    $cache->info( "This is an info message\n" );
    is( $msg, "This is an info message\n", 'info method works.' );
}

# Test empty file case
{
    open( my $fh, '>', $file ) or die "Unable to create empty cache file: $!";
    close( $fh );

    my $cache = cPanel::CacheFile->new( { cache_file => $file, data_obj => $mock_obj } );
    isa_ok( $cache, 'cPanel::CacheFile' );

    ok( !-z $file, "Cache file should be filled." );

    ok( !$mock_obj->{load_called}, "File was empty, should not have loaded." );
    is( $mock_obj->{save_called}, 2, "File was empty, should have saved." );

    ok( !-e $lockname, "File not locked at this time." );
}

{
    open( my $fh, '<', $file ) or die "Unable to read cache file: $!\n";
    my $file_data = <$fh>;
    is( $file_data, 'Save string: 2 0', 'cache_file is correct.' );
}

# Test re-synch when file hasn't changed.
# Lock the file for update.
{
    my $guard = $cache->synch();
    ok( -e $lockname, "File is locked." );

    ok( !$mock_obj->{load_called}, "memory up-to-date, don't load." );
    $guard->update_file();
    is( $mock_obj->{save_called}, 3, "update calls save." );
}
ok( !-e $lockname, "File is unlocked." );

# Update cache file directly.
{
    open( my $fh, '>', $file ) or die "Unable to write cache file: $!\n";
    print $fh 'This is the updated cache file.';
    close( $fh );
}

ok( $cache->synch(), 'Synch occured.' );
ok( !-e $lockname, "File is not locked." );

is( $mock_obj->{load_called}, 1, "file changed, load." );
is( $mock_obj->{data}, 'This is the updated cache file.', 'Correct data is loaded.' );

# Test that we don't reload after the last synch
ok( $cache->synch(), 'Synch occured.' );
is( $mock_obj->{load_called}, 1, "don't load again." );
is( $mock_obj->{data}, 'This is the updated cache file.', 'Correct data is loaded.' );

cleanup();

# Discard temporary files that we don't need any more.
sub cleanup {
    unlink $file if -e $file;
    unlink $lockname if -e $lockname;
    File::Path::rmtree( $dir ) if -d $dir;
}