
use strict;
use warnings;

use Module::Build 0.3601;


my %module_build_args = (
  'build_requires' => {
    'Carp' => '0',
    'Cwd' => '0',
    'FindBin' => '0',
    'Test::Exception' => '0',
    'Test::More' => '0'
  },
  'configure_requires' => {
    'Module::Build' => '0.3601'
  },
  'dist_abstract' => 'Manage a FIFO queue of tasks to perform.',
  'dist_author' => [
    'cPanel, Inc. <cpan@cpanel.net>'
  ],
  'dist_name' => 'cPanel-TaskQueue',
  'dist_version' => '0.603',
  'license' => 'perl',
  'module_name' => 'cPanel::TaskQueue',
  'recommends' => {},
  'recursive_test_files' => 1,
  'requires' => {
    'Fcntl' => '0',
    'File::Path' => '0',
    'File::Spec' => '0',
    'Getopt::Long' => '0',
    'POSIX' => '0',
    'Storable' => '0',
    'Term::ReadLine' => '0',
    'Text::Wrap' => '0',
    'Unix::PID' => '0.21',
    'YAML::Syck' => '0'
  },
  'script_files' => [
    'bin/taskqueuectl',
    'bin/taskqueued',
    'bin/taskqueuerun'
  ]
);


my $build = Module::Build->new(%module_build_args);

$build->create_build_script;
