
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

sub create
{
	my ($pkg, %params) = @_;

	my $graph = $params{dependency_graph} || die "missing dependency_graph parameter";
	my $job;
	$job = $pkg->SUPER::new(
		local_data		=> undef,
		data			=> undef,
		chdir			=> undef,
		prequel			=> '',
		force_host		=> undef, # mirror DependencyJob
		when_done		=> undef,
		all_done		=> undef,
		%params,
	);
	$graph->add($job);
	return $job;
}

sub new
{
	my ($pkg, $dependency_graph, $preload, $func, %params) = @_;

	my $data = delete $params{args};
	my $whendone = delete $params{whendone};

	return $pkg->create(
		dependency_graph	=> $dependency_graph,
		preload			=> $preload,
		eval			=> "$func(\@\$data)",
		desc			=> "RPC call to $func",
		data			=> $data,
		when_done		=> $whendone,
		%params,
	);
}

sub startup
{
	my ($job) = @_;
	$job->{on_start}->($job, $job->{dependency_graph}) if $job->{on_start};
	do_remote_job(
		data		=> $job->{data},
		desc		=> $job->{desc},
		host		=> $job->{host},
		eval		=> $job->{eval},
		chdir		=> $job->{chdir},
		prequel		=> $job->{prequel},
		preload		=> $job->{preload},
		prefix		=> $job->{prefix},
		when_done	=> sub {
			if ($job->{when_done}) {
				$job->{when_done}(@_);
			} else {
				$job->finished(0);
			}
		},
		all_done	=> $job->{all_done},
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
 use Object::Dependency;

 $dependency_graph = Object::Dependency->new();

 $job = Proc::JobQueue::RemoteDependencyJob->create(
	dependency_graph	=> $dependency_graph,
	host			=> $remote_host_name,
	%remote_job_args
 );

=head1 DESCRIPTION

This is sublcass of L<Proc::JobQueue::Job>.   It combines 
a L<RPC::ToWorker> with a L<Proc::JobQueue> and provides a
way to run arbitrary perl code in dependency order on a 
network of systems.  Overall execution is controlled by
L<Proc::JobQueue::DependencyQueue>.

It is just like using a L<RPC::ToWorker>, except that
the job doesn't run right away: it starts up when the 
job queue is ready to run it.

Most construction (note: use C<create> not C<new>) parameters 
are passed through to L<RPC::ToWorker> but there are a
couple that are handled specially:

=over 12

=item when_done

A callback to invoke when the remote job has finished.  The return values
from the remotely eval'ed code will be passed to the callback.  If provided,
the callback must call C<$job-E<gt>finished(0)> or otherwise mark itself
as finished (see L<Proc::JobQueue::DependencyJob>).  If no callback is 
provided then C<$job-E<gt>finished(0)> will be called.

=item local_data

If not set, the following will be provided:

=over

=item dependency_graph

A reference to the dependency graph the job queue is using.

=item master_job

A reference to self.

=back

=item data

Data to send to remote job.

=item desc

Description of work.

=item host

Hostname to run on.

=item eval

B<String>.  Code to run on remote system.

=item chdir

Directory to change to.

=item prequel

B<String>.  File-scope eval code.

=item prefix

B<String>.  Prepend each line of output from the remote system with this string.

=item all_done

A callback to invoke when the remote slave is completely shut down.

=back

=head1 ERRATA

There is also a C<new> constructor with different arguments.  It is retained
for backwards compatbility.

=head1 LICENSE

Copyright (C) 2007-2008 SearchMe, Inc.   
Copyright (C) 2008-2010 David Sharnoff.
Copyright (C) 2011 Google, Inc.
This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

