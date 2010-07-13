use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'cPanel::TaskQueue',
    AUTHOR              => 'cPanel, Inc. <cpan@cpanel.net>',
    VERSION_FROM        => 'lib/cPanel/TaskQueue.pm',
    ABSTRACT_FROM       => 'lib/cPanel/TaskQueue.pod',
    PL_FILES            => {},
    EXE_FILES => [ map { "bin/$_" } qw{taskqueuectl taskqueued taskqueuerun} ],
    PREREQ_PM => {
        'Test::More' => 0,
        'Fcntl' => 0,
        'YAML::Syck' => 0,
        'Term::ReadLine' => 0,
        'File::Path' => 0,
        'POSIX' => 0,
    #   'Unix::PID' => 0.21,  # Useful for the taskqueued program.
    #   'File::Spec' => 0,    # Useful for the taskqueued program.
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'cPanel-TaskQueue-*' },
);
