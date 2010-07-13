#!/usr/bin/perl

use Test::More tests => 6;

use strict;
use warnings;
use cPanel::CacheFile ();

eval {
    cPanel::CacheFile->import( '-logger' );
};
like( $@, qr/even number/, 'Must have argument pairs.' );

eval {
    cPanel::CacheFile->import( '-other' => 'one' );
};
like( $@, qr/Unrecognized/, 'Unknown policy handled.' );

eval {
    cPanel::CacheFile->import( '-logger' => 'Fred::UnknownLogger' );
};
like( $@, qr/Can't locate/, 'Bad logger module.' );

eval {
    cPanel::CacheFile->import( '-logger' => {} );
};
like( $@, qr/correct interface/, 'Bad logger object.' );

eval {
    cPanel::CacheFile->import( '-filelock' => 'Fred::UnknownLocker' );
};
like( $@, qr/Can't locate/, 'Bad filelock module.' );

eval {
    cPanel::CacheFile->import( '-filelock' => {} );
};
like( $@, qr/correct interface/, 'Bad locker object.' );

