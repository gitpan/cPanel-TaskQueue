
use strict;
use warnings;



use ExtUtils::MakeMaker 6.31;



my %WriteMakefileArgs = (
  'ABSTRACT' => 'Manage a FIFO queue of tasks to perform.',
  'AUTHOR' => 'cPanel, Inc. <cpan@cpanel.net>',
  'BUILD_REQUIRES' => {
    'Carp' => '0',
    'FindBin' => '0',
    'Test::More' => '0'
  },
  'CONFIGURE_REQUIRES' => {
    'ExtUtils::MakeMaker' => '6.31'
  },
  'DISTNAME' => 'cPanel-TaskQueue',
  'EXE_FILES' => [
    'bin/taskqueuectl',
    'bin/taskqueued',
    'bin/taskqueuerun'
  ],
  'LICENSE' => 'perl',
  'NAME' => 'cPanel::TaskQueue',
  'PREREQ_PM' => {
    'Fcntl' => '0',
    'File::Path' => '0',
    'File::Spec' => '0',
    'Getopt::Long' => '0',
    'POSIX' => '0',
    'Term::ReadLine' => '0',
    'Text::Wrap' => '0',
    'Unix::PID' => '0.21',
    'YAML::Syck' => '0'
  },
  'VERSION' => '0.503_03',
  'test' => {
    'TESTS' => 't/*.t'
  }
);


unless ( eval { ExtUtils::MakeMaker->VERSION(6.56) } ) {
  my $br = delete $WriteMakefileArgs{BUILD_REQUIRES};
  my $pp = $WriteMakefileArgs{PREREQ_PM};
  for my $mod ( keys %$br ) {
    if ( exists $pp->{$mod} ) {
      $pp->{$mod} = $br->{$mod} if $br->{$mod} > $pp->{$mod};
    }
    else {
      $pp->{$mod} = $br->{$mod};
    }
  }
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);



