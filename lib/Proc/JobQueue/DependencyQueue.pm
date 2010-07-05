
package Proc::JobQueue::DependencyQueue;

use strict;
use warnings;
use Carp qw(confess);
use Hash::Util qw(lock_keys unlock_keys);
require Proc::JobQueue;
use Time::HiRes qw(time);
require POSIX;

our @ISA = qw(Proc::JobQueue);

our $timer_interval = 6;
my $debug = 0;

sub new
{
	my ($pkg, %params) = @_;

	confess if $params{host} && $params{host} eq 'host';
	die unless $params{dependency_graph};

	my $queue = $pkg->SUPER::new(
		dependency_graph	=> undef,
		startmore_in_progress	=> 0,
		on_failure		=> \&on_failure,
		%params
	);

	if (defined(&IO::Event::unloop_all)) {
		my $last_dump = time;

		my $timer = IO::Event->timer(
			interval	=> $params{timer_interval} || $timer_interval,
			cb		=> sub {
				print STDERR "beep!\n" if $debug;
				eval {
					$queue->startmore;
				};
				if ($@) {
					print STDERR "DIE DIE DIE DIE DIE (DT1): $@";
					# exit 1; hangs
					POSIX::_exit(1);
				};
				if ($debug && time > $last_dump + $timer_interval) {
					$params{dependency_graph}->dump_graph();
					$last_dump = time;
				}
				use POSIX ":sys_wait_h";
				my $k;
				do { $k = waitpid(-1, WNOHANG) } while $k > 0;
			},
		);

		$Event::DIED = sub {
			Event::verbose_exception_handler(@_);
			IO::Event::unloop_all();
		};
	}

	return $queue;
}

sub add
{
	my ($job_queue, $job, $host) = @_;
	# confess if exists $job->{job_is_finished};
	unlock_keys(%$job);
	$job->{dependency_graph} = $job_queue->{dependency_graph};
	lock_keys(%$job);
	$job_queue->SUPER::add($job, $host);
}

sub job_part_finished
{
	my ($job_queue, $job, $do_startmore) = @_;
	$job_queue->SUPER::jobdone($job, $do_startmore);
}

sub jobdone
{
	my ($job_queue, $job, $do_startmore, @exit_code) = @_;
	if ($job->{dependency_graph}) {
		if ($exit_code[0]) {
			print STDERR "Things dependent on $job->{desc} will never run: @exit_code\n";
			$job->{dependency_graph}->stuck_dependency($job, "exit @exit_code");
		} else {
			$job->{dependency_graph}->remove_dependency($job);
		}
		$job->{dependency_graph} = undef;
		# unlock_keys(%$job);
		# $job->{this_is_finished} = 1;
		# lock_keys(%$job);
	}
	$job_queue->SUPER::jobdone($job, $do_startmore, @exit_code);
}

sub startmore
{
	my ($job_queue) = shift;

	if ($job_queue->{startmore_in_progress}) {
		print STDERR "Re-entry to startmore prevented\n" if $debug;
		$job_queue->{startmore_in_progress}++;
		return 0;
	}
	$job_queue->{startmore_in_progress} = 2;

	my $dependency_graph = $job_queue->{dependency_graph};

	my $stuff_started = 0;

	my $jq_done;

	print STDERR "looking for more depenency graph items to queue up\n" if $debug;
	eval {
		$job_queue->checkjobs();

		while ($job_queue->{startmore_in_progress} > 1) {
			$job_queue->{startmore_in_progress} = 1;
			while (my @runnable = $dependency_graph->independent(lock => 1)) {
				$stuff_started++;
				for my $task (@runnable) {
					if ($task->can('run_dependency_task')) {
						$job_queue->{startmore_in_progress}++ if $task->run_dependency_task($dependency_graph);
					} elsif ($task->isa('Proc::JobQueue::Job')) {
						$job_queue->add($task, $task->{force_host});
					} else {
						die "don't know how to handle $task";
					}
				}
			}

			$jq_done = $job_queue->SUPER::startmore();

			redo if $job_queue->{startmore_in_progress} > 1;
		}
	};
	if ($@) {
		print STDERR "DIE DIE DIE DIE DIE (DT2): $@";
		# exit 1; hangs!
		POSIX::_exit(1);
	};

	$job_queue->{startmore_in_progress} = 0;

	if ($jq_done && $dependency_graph->alldone) {
		print STDERR "Nothing more to do\n";
		if (defined(&IO::Event::unloop_all)) {
			IO::Event::unloop_all();
		}
		return 1;
	} elsif ($jq_done && ! $stuff_started) {
		if (keys %{$dependency_graph->{stuck}}) {
			print STDERR "All runnable jobs are done, remaining dependencies are stuck:\n";
			for my $o (values %{$dependency_graph->{stuck}}) {
				printf "\t%s\n", $dependency_graph->desc($o);
			}
			if (defined(&IO::Event::unloop_all)) {
				IO::Event::unloop_all();
			}
			return 1;
		} else {
			print STDERR "Job queue is empty, but dependency graph doesn't think there is any work to be done!\n";
			$dependency_graph->dump_graph();
		}
	}
	return 0;
}

sub on_failure
{
	my ($queue, $job, @exit_code) = @_; 
	if ($job->{on_failure}) {
		$job->{on_failure}->(@exit_code);
	} elsif ($job->{errors}) {
		$job->{errors}->("FAILED: $job->{desc}", @exit_code);
	} else {
		print STDERR "JOB $job->{desc} FAILED\nexit @exit_code\n";
	}
}

sub status
{
	my ($queue) = @_;
	$queue->SUPER::status();
	my $dg = $queue->{dependency_graph};
	printf "Dependency Graph items: %d independent (%d locked %d active), %d total, alldone=%s\n",
		scalar(keys(%{$dg->{independent}})),
		scalar(grep { $_->{dg_lock} } values %{$dg->{independent}}),
		scalar(grep { $_->{dg_active} } values %{$dg->{independent}}),
		scalar(keys(%{$dg->{addrmap}})),
		$dg->alldone;
}

1;

__END__

=head1 SYNOPSIS

 use Proc::JobQueue::DependencyQueue;
 use Object::Dependency;
 use Proc::JobQueue::DependencyTask;
 use Proc::JobQueue::DependencyJob;

 my $dependency_graph = Object::Dependency->new();

 my $job = Proc::JobQueue::DependencyJob->new($dependency_graph, $callback_func);

 my $task => Proc::JobQueue::DependencyTask->new(desc => $desc, func => $callback_func);

 $dependency_graph->add($job);
 $dependency_graph->add($task);

 my $queue = Proc::JobQueue::DependencyQueue->new(
	dependency_graph => $dependency_graph,
	hold_all => 1,
 );

 $job_queue->hold(0);

 $queue->startmore();

 IO::Event::loop();

 IO::Event::unloop_all() if $queue->alldone;

=head1 DESCRIPTION

This module is a sublcass of L<Proc::JobQueue>.  It combines a job
queue with a a dependency graph, L<Object::Dependency>.

The jobs that it runs are either full-fledged jobs, 
L<Proc::JobQueue::DependencyJob>, or 
simple perl callbacks that aren't scheduled: L<Proc::JobQueue::DependencyTask>.

Generally, the way to use this is to generate your dependency graph, then
create your job queue, then start some jobs.

It's expected that you'll use asynchronous I/O via L<IO::Event>, but that is not
required.   If you're using L<IO::Event>, it sets up a timer event to start more
jobs.  It also changes C<$Event::DIED> to unloop.

=head1 API

In addition to the parameters supported by L<Proc::JobQueue>, the following
parameters are used:

=over

=item dependency_graph

This should be a L<Object::Dependency> object.

=back

In addition to the methods inherited from L<Proc::JobQueue>, this module
add:

=ovew

=item job_part_finished($job)

This marks the C<$job> as complete and a new job can start in its place.

=back

=head1 LICENSE

This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

