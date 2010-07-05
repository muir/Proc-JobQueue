
package Proc::JobQueue::RemoteDependencyJob;

use strict;
use warnings;
use Scalar::Util qw(blessed);
use Callback;
use Proc::JobQueue::Job;
use RPC::ToWorker;
require Proc::JobQueue::Job;
use Carp qw(confess);

our @ISA = qw(Proc::JobQueue::Job);

sub new
{
	my ($pkg, $dependency_graph, $preload, $func, %params) = @_;

	my $job = $pkg->SUPER::new(
		dependency_graph	=> $dependency_graph,
		preload			=> $preload,
		eval			=> "$func(\@\$data)",
		desc			=> "RPC call to $func",
		local_data		=> undef,
		args			=> undef,
		chdir			=> undef,
		prequel			=> undef,
		%params,
	);
	$dependency_graph->add($job);
	return $job;
}

sub startup
{
	my ($job) = @_;
	do_remote_job(
		data		=> $job->{args} || [],
		desc		=> $job->{desc},
		host		=> $job->{host},
		eval		=> $job->{eval},
		chdir		=> $job->{chdir},
		prequel		=> $job->{prequel},
		preload		=> $job->{preload},
		when_done	=> sub {
			$job->finished(0);
		},
		local_data	=> $job->{local_data} || {
			dependency_graph	=> $job->{dependency_graph},
			master_job		=> $job,
		},
	);
}

1;

__END__

=head1 NAME

Proc::JobQueue::RemoteDependencyJob - add a remote job to a dependency queue

=head1 SYNOPSIS

 use Proc::JobQueue::RemoteDependencyJob;

 Proc::JobQueue::RemoteDependencyJob->new(
	dependency_graph	=> $dependency_graph,
	host			=> $remote_host_name,
	%remote_job_args
 );

=head1 DESCRIPTION

This is sublcass of L<Proc::JobQueue::Job>.   It combines 
L<RPC::ToWorker> with a L<Proc::JobQueue>.

It's just like using a RPC::ToWorker, except that
the job doesn't run right away: it starts up when the 
job queue is ready to run it.

=head1 LICENSE

This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

