
package Proc::JobQueue;

use strict;
use warnings;

use Time::HiRes qw(sleep);
use Sys::Hostname;
use Carp qw(confess);
use Hash::Util qw(lock_keys);
use Time::HiRes qw(time);
use Module::Load;
require Exporter;

our $VERSION = 0.601;
our $debug ||= 0;
our $status_frequency ||= 2;
our $host_canonicalizer ||= 'File::Slurp::Remote::CanonicalHostnames';

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(is_remote_host canonicalize my_hostname);

sub configure
{
	my ($queue, %params) = shift;
	@$queue{keys %params} = values %params;
}

sub addhost
{
	my ($queue, $host, %params) = @_;
	my $hr;
	if ($hr = $queue->{status}{$host}) {
		@$hr{keys %params} = values %params;
	} else {
		$hr = $queue->{status}{$host} = {
			name		=> $host,
			running		=> {},
			queue		=> {},
			jobs_per_host	=> $queue->{jobs_per_host},
			in_startmore	=> 0,
			%params,
		};
	}
	$queue->set_readiness($host);
}

sub set_readiness
{
	my ($queue, $host) = @_;
	my $hr = $queue->{status}{$host};
	if ($hr->{jobs_per_host} and $hr->{jobs_per_host} > keys %{$hr->{running}}) {
		$queue->{ready_hosts}{$host} = $hr;
	} elsif (! keys %{$hr->{running}}) {
		$queue->{ready_hosts}{$host} = $hr;
	} else {
		delete $queue->{ready_hosts}{$host};
	}
}

sub new
{
	my ($pkg, %params) = @_;
	my $queue = bless {
		host_overload	=> 120,
		host_is_over	=> 0,
		jobnum		=> 1000,
		jobs_per_host	=> 4,
		queue		=> {},
		status		=> {},
		ready_hosts	=> {},
		hold_all	=> 0,
		hosts		=> [ my_hostname() ],
		%params,
	}, $pkg;
	$queue->addhost($_) for @{$queue->{hosts}};
	lock_keys(%$queue);
	return $queue;
}

sub hold
{
	my ($self, $new) = @_;
	$self->{hold_all} = $new if defined $new;
	return $self->{hold_all};
}

sub add
{
	my ($queue, $job, $host) = @_;
	confess "$job not a ref" unless ref $job;
	confess "$job is not a job" unless $job->isa('Proc::JobQueue::Job');

	$job->jobnum($queue->{jobnum}++)
		unless $job->jobnum;
	my $jobnum = $job->jobnum();

	print STDERR "Adding $jobnum - ".ref($job)." to worklist\n" if $debug > 2;
	my $q;
	if ($host) {
		confess "no $host" unless $queue->{status}{$host};
		$q = $queue->{status}{$host}{queue};
	} else {
		$q = $queue->{queue};
	}

	$q->{$jobnum} = $job;
	$job->queue($queue);
	$queue->startmore;
}

sub startmore
{
	my ($queue) = @_;
	return 0 if $queue->{hold_all};
	print "# Looking to start more\n" if $debug > 8;
	confess "no hosts added" unless keys %{$queue->{status}};
	my $stuff = 0;
	my $new_host_is_over = 0;
	while(1) {
		my $redo = 0;
		HOST:
		for my $host (keys %{$queue->{ready_hosts}}) {
			print STDERR "# checking $host to maybe start more jobs\n" if $debug > 3;
			my $hr = $queue->{ready_hosts}{$host};
			JOB:
			while ((! $hr->{jobs_per_host} && ! keys %{$hr->{running}}) || $hr->{jobs_per_host} > (keys %{$hr->{running}} || 0)) {
				print STDERR "# there is room for more on $host\n" if $debug > 4;
				$new_host_is_over++
					if keys(%{$hr->{queue}}) > $queue->{host_overload};
				my @q;
				push (@q, $hr->{queue});
				push (@q, $queue->{queue})
					if $hr->{jobs_per_host} && ! $queue->{host_is_over};
				for my $q (@q) {
					next unless keys %$q;
					$stuff = 1;
					for my $jobnum (reverse sort { $q->{$a}{priority} <=> $q->{$b}{priority} || $a <=> $b } keys %$q) {
						print STDERR "# looking to start $jobnum on $host\n" if $debug > 5;
						my $job = $q->{$jobnum};
						unless ($job->runnable) {
							print STDERR "# can't start $jobnum $job->{desc} on $host: not runnable\n" if $debug > 5;
							next;
						}
						delete $q->{$jobnum};
						$queue->startjob($host, $jobnum, $job);
						$queue->set_readiness($host);
						$redo = 1;
						next HOST;
					}
				}
				last;
			}
		}
		last unless $redo;
	}
	$queue->{host_is_over} = $new_host_is_over;
	return 0 if $stuff;
	return $queue->alldone();
}

sub startjob
{
	my ($queue, $host, $jobnum, $job) = @_;
	print STDERR "# starting $jobnum $job->{desc} on $host\n" if $debug > 1;
	my $hr = $queue->{status}{$host};
	$hr->{running}{$jobnum} = $job;
	$job->host($host);
	$job->start();
}


# This routine is re-enterant: it may be called from something it calls.
sub checkjobs
{
	my ($queue) = @_;
	my $found = 0;
	for my $host (keys %{$queue->{status}}) {
		print STDERR "# checking jobs on $host\n" if $debug > 7;
		my $hr = $queue->{status}{$host} || die;
		for my $jobnum (keys %{$hr->{running}}) {
			my $job = $hr->{running}{$jobnum};
			if ($job) {
				print STDERR "# checking $jobnum $job->{desc} on $host\n" if $debug > 8;
				$found++
					if defined $job->checkjob($queue);
			} else {
				print STDERR "# job $jobnum is undef!\n" if $debug;
				delete $hr->{running}{$jobnum};
				$found++;
			}

		}
		$queue->set_readiness($host);
	}
	return $found;
}

sub jobdone
{
	my ($queue, $job, $startmore, @exit_code) = @_;

	$startmore = 1 unless defined $startmore;

	my $host = $job->host;
	my $jobnum = $job->jobnum;

	print STDERR "# job $jobnum $job->{desc} on $host is done\n" if $debug > 5;

	my $hr = $queue->{status}{$host} or confess;
	delete $hr->{running}{$jobnum} or confess;

	$queue->set_readiness($host);

	$queue->startmore() if $startmore;
}


sub alldone
{
	my ($queue, $skip_status) = @_;
	$queue->status() if $debug && ! $skip_status;
	return 0 if keys %{$queue->{queue}};
	for my $host (keys %{$queue->{status}}) {
		my $hr = $queue->{status}{$host};
		return 0 unless $queue->{ready_hosts}{$host};
		return 0 if keys %{$hr->{queue}};
		return 0 if keys %{$hr->{running}};
		next unless $hr->{jobs_per_host} > 0;
	}
	return 1;
}

my $last_dump = time;

sub status
{
	my ($queue) = @_;
	return if time < $last_dump + $status_frequency;
	$last_dump = time;
	print STDERR "Queue Status\n";
	printf STDERR "\titems in main queue: %d, alldone=%d\n", scalar(keys %{$queue->{queue}}), $queue->alldone(1);
	print STDERR "\tHost overload condition is true\n" if $queue->{host_is_over};
	for my $host (sort keys %{$queue->{status}}) {
		my $hr = $queue->{status}{$host};
		printf STDERR "\titems in queue for %s: %d, items running: %s, host is %sready\n", 
			$host,
			scalar(keys(%{$hr->{queue}})),
			scalar(keys(%{$hr->{running}})),
			($queue->{ready_hosts}{$host} ? "" : "not ");
		for my $job (values %{$hr->{running}}) {
			print STDERR "\t\tRunning: $job->{jobnum} $job->{desc}\n";
		}
	}
}

my $canonicalizer;
sub get_canonicalizer
{
	return $canonicalizer if $canonicalizer;
	load($host_canonicalizer);
	$canonicalizer = $host_canonicalizer->new();
}

sub canonicalize
{
	my ($host) = @_;
	return get_canonicalizer()->canonicalize($host);
}

my $my_hostname;
sub my_hostname
{
	return $my_hostname if $my_hostname;
	$my_hostname = get_canonicalizer()->myname();
}

sub is_remote_host
{
	my ($host) = @_;
	return my_hostname() ne canonicalize($host);
}


1;

__END__

=head1 NAME

 Proc::JobQueue - generic job queue base class

=head1 SYNOPSIS

 use Proc::JobQueue;

 $queue = Proc::JobQueue->new(%parameters);

 $queue->addhost($host, %parameters);

 $queue->add($job);
 $queue->add($job, $host);

 $queue->startmore();

 $queue->hold($new_value);

 $queue->checkjobs();

 $queue->jobdone($job, $do_startmore, @exit_code);

 $queue->alldone()

 $queue->status()

 $queue->startjob($host, $jobnum, $job);

=head1 DESCRIPTION

Generic queue of "jobs".   Most likely to be subclassed
for different situations.  Jobs are registered.  Hosts are
registered.  Jobs may or may not be tied to particular hosts.
Jobs are started on hosts.   

Jobs are started with:

  $job->jobnum($jobnum);
  $jobnum = $job->jobnum();
  $job->queue($queue);
  $job->host($host);
  $job->start();

When jobs complete, they must call:

  $queue->jobdone($job, $do_startmore, @exit_code);

Hosts are added with:

  $queue->addhost($host, name => $hostname, jobs_per_host => $number_to_run_on_this_host)

=head1 CONSTRUCTION

The parameters for C<new> are:

=over

=item jobs_per_host (default: 4)

Default number of jobs to run on each host simultaneously.  This can be overridden on a per-host basis.

=item host_overload (default: 120)

If any one host has more than this many jobs waiting for it, no can-run-on-any-host jobs will be started.
This is to prevent the queue for this one overloaded host from getting too large.

=item jobnum (default: 1000)

This is the starting job number.   Job numbers are sometimes displayed.  They increment for each new job. 

=item hold_all (default: 0)

If true, prevent any jobs from starting until C<$queue-E<gt>hold(0)> is called.

=back

=head1 METHODS

=over

=item configure(%params)

Adjusts the same parameters that can be set with C<new>.

=item addhost($host, %params)

Register a new host.  Parameters are:

=over 

=item name

The hostname

=item jobs_per_host

The number of jobs that can be run at once on this host.  This defaults
to the jobs_per_host parameter of the C<$queue>.

=back

=item add($job, $host)

Add a job object to the queue.   The job object must be 
a L<Proc::JobQueue::Job> or subclass of L<Proc::JobQueue::Job>.  
The C<$host> parameter is optional: if not set, the job can be run on any host.

The C<$job> object is started with:

  $job->jobnum($jobnum);
  $jobnum = $job->jobnum();
  $job->queue($queue);
  $job->host($host);
  $job->start();

When the job complets, it must call:

  $queue->jobdone($job, $do_startmore, @exit_code);

=item jobdone($job, $do_startmore, @exit_code)

When jobs complete, they must call jobdone.  If C<$do_startmore> is true,
then C<startmore()> will be called.  A true exit code signals an
error and it is used by L<Proc::JobQueue::CommandQueue>.

=item alldone

This checks the job queue.  It returns true if all jobs have completed and
the queue is empty.

=item status

This prints a queue status to STDERR showing what's running on which hosts. 
Printing is supressed unless $Proc::JobQueue::status_frequency seconds have
passed since the last call to C<status()>.

=item startmore

This will start more jobs if possible.  The return value is true if there are 
no more jobs to start.

=item startjob($host, $jobnum, $job)

This starts a single job.  It is used by startmore() and probably should not be
used otherwise.

=item checkjobs

This is used be L<Proc::JobQueue::BackgroundQueue>.

=item hold($new_value)

Get (or set if $new_value is defined) the queue's hold-all-jobs parameter.

  $queue->jobdone($job, $do_startmore, @exit_code);

=back

=head1 CANONICAL HOSTNAMES

Proc::JobQueue needs canonical hostnames.  It gets them by default
with L<Proc::JobQueue::CanonicalHostnames>.  You can override this 
default by overriding C<$Proc::JobQueue::host_canonicalizer> with
the name of a perl module to use instead of 
L<Proc::JobQueue::CanonicalHostnames>.  

Helper functions are provided by Proc::JobQueue and are available
via explicit import:

 use Proc::JobQueue qw(my_hostname canonicalize is_remote_host);

=head1 SEE ALSO

L<Proc::JobQueue::Job>
L<Proc::JobQueue::DependencyQueue>
L<Proc::JobQueue::BackgroundQueue>

=head1 LICENSE

Copyright (C) 2007-2008 SearchMe, Inc.   
Copyright (C) 2008-2010 David Sharnoff.
Copyright (C) 2011 Google, Inc.
This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

