#!/usr/bin/perl

use Test::More tests => 6;

use strict;
use warnings;
use cPanel::CacheFile;

my $dir = '/tmp/cache_test';
my $file = "$dir/cache_dir/cache_file";

eval {
    my $cf = cPanel::CacheFile->new();
};
like( $@, qr/cache filename/, "Cannot create CacheFile without parameters" );

eval {
    my $cf = cPanel::CacheFile->new( { data_obj => 1 } );
};
like( $@, qr/cache filename/, "Cannot create CacheFile without cache directory" );

eval {
    my $cf = cPanel::CacheFile->new( { cache_file => $file } );
};
like( $@, qr/data object/, "Cannot create CacheFile without a data object" );

eval {
    my $cf = cPanel::CacheFile->new( { cache_file => $file, data_obj => {} } );
};
like( $@, qr/required interface/, "Cannot create CacheFile without a data object" );

eval {
    cPanel::CacheFile->new( {logger => ''} );
};
like( $@, qr/Supplied logger/, 'Recognize bad logger.' );

eval {
    cPanel::CacheFile->new( {locker => ''} );
};
like( $@, qr/Supplied locker/, 'Recognize bad locker.' );
