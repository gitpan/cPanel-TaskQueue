#!/usr/bin/perl

use FindBin;
use lib "$FindBin::Bin/mocks";

use Test::More tests => 7;

use strict;
use warnings;

use cPanel::FakeLogger;
BEGIN { use_ok( 'cPanel::CacheFile::FileLocker' ); };

eval { cPanel::CacheFile::FileLocker->new(); };
like( $@, qr/Required logger/, 'no_args' );
eval { cPanel::CacheFile::FileLocker->new( 'fred' ); };
like( $@, qr/hash reference/, 'Parameter to FileLocker must be hashref.' );
eval { cPanel::CacheFile::FileLocker->new( {} ); };
like( $@, qr/Required logger/, 'Missing logger parameter' );

my $locker = cPanel::CacheFile::FileLocker->new( {max_age=>120, max_wait=>180, logger=>cPanel::FakeLogger->new()} );
isa_ok( $locker, 'cPanel::CacheFile::FileLocker', 'with_hashref' );

$locker = cPanel::CacheFile::FileLocker->new( {sleep_secs=>0.1, logger=>cPanel::FakeLogger->new()} );
isa_ok( $locker, 'cPanel::CacheFile::FileLocker', 'with_hashref and subsecond sleep attempt' );
# Peek inside for test, don't try this at home.
is( $locker->{sleep_secs}, 1, 'Sub-second sleep repaired.' );
