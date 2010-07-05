
package Proc::JobQueue::Sort;

# $Id: Sort.pm 13848 2009-07-23 21:34:00Z david $

use strict;
use warnings;
require Proc::JobQueue::Job;
our @ISA = qw(Proc::JobQueue::Job);
use Tie::Function::Examples qw(%q_shell);

sub new
{
	my ($pkg, $opts, $config, $dest, @inputs) = @_;
	return $pkg->SUPER::new(
		config	=> $config,
		opts	=> $opts,
		dest	=> $dest,
		inputs	=> \@inputs,
		desc	=> "Sort to $dest",
	);
}

sub command
{
	my ($job) = @_;
	my $mem = '';
	my $config = $job->{config};
	if ($config->{unix_sort_memory_buffer}) {
		$mem = "--buffer-size=$config->{unix_sort_memory_buffer}";
	}
	return "sort $mem -o $q_shell{$job->{dest}} @q_shell{@{$job->{inputs}}}";
}

sub success
{
	my ($job) = @_;
	unlink(@{$job->{inputs}});
}

sub failed
{
	my ($job) = @_;
	unlink($job->{dest});
}

1;

__END__

=head1 NAME

 Proc::JobQueue::Sort - sort files in the background

=head1 SYNOPSIS

 use Proc::JobQueue::BackgroundQueue;
 use aliased 'Proc::JobQueue::Sort';

 my $queue = new Proc::JobQueue::BackgroundQueue;

 my $job = Sort->new($opts, $config, "/dest/file", "/input/file1", "/input/file2");

 $queue->add($job);

 $queue->finish;

=head1 DESCRIPTION

This is a subclass of L<Proc::JobQueue::Job>.
In the background, sort the input files into the output.
using the unix L<sort(1)> command.

=head1 SEE ALSO

L<Proc::JobQueue>
L<Proc::JobQueue::Job>
L<Proc::JobQueue::BackgroundQueue>
L<Proc::JobQueue::Move>
L<Proc::JobQueue::Command>
L<Proc::JobQueue::Sequence>

=head1 LICENSE

This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

