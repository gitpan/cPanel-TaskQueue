#!/usr/bin/perl

# Test the cPanel::CacheFile module.
#

use FindBin;
use lib "$FindBin::Bin/mocks";
use File::Path ();
use Data::Dumper;
use cPanel::FakeLogger;

use Test::More tests => 4;
my $logger;
BEGIN {
    $logger = cPanel::FakeLogger->new;
};

use cPanel::CacheFile ( '-logger' => $logger );

# test bad new calls.
eval {
    my $cf = cPanel::CacheFile->new();
};
like( $@, qr/cache filename/, 'Cannot create CacheFile without parameters' );
like( ($logger->get_msgs())[0], qr/throw.*?cache filename/, 'Logged correctly.' );

# Put a logger on the specific CacheFile
my $cfile = '/tmp/wade.cache';

my $logger2 = cPanel::FakeLogger->new;
eval {
    my $cf = cPanel::CacheFile->new({cache_file=>$cfile,data_obj=>{},logger=>$logger2});
};
like( $@, qr/required interface/, 'Cannot create CachFile with bad data object.' );
like( ($logger2->get_msgs())[0], qr/throw.*?required interface/, 'Logged correctly.' );
