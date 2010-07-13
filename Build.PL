use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'cPanel::TaskQueue',
    license             => 'perl',
    dist_author         => 'cPanel, Inc. <cpan@cpanel.net>',
    dist_version_from   => 'lib/cPanel/TaskQueue.pm',
    requires => {
        'Test::More' => 0,
        'Fcntl' => 0,
        'YAML::Syck' => 0,
        'Term::ReadLine' => 0,
        'File::Path' => 0,
        'POSIX' => 0,
    },
    recommends => {
        # Useful for the taskqueued program.
        'Unix::PID' => 0.21,
        'File::Spec' => 0,
    },
    add_to_cleanup      => [ 'cPanel-TaskQueue-*' ],
);

$builder->create_build_script();
