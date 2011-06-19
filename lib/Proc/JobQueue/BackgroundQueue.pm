
package Proc::JobQueue::BackgroundQueue;

use strict;
use warnings;
use Carp qw(confess);
use Time::HiRes qw(sleep);
require Proc::JobQueue;

our @ISA = qw(Proc::JobQueue);

our $debug = $Proc::JobQueue::debug;

sub new
{
	my ($pkg, %params) = @_;
	my $queue = $pkg->SUPER::new(sleeptime => 2, %params);
}

sub add
{
	my $queue = shift;
	my $job = shift;
	confess "cannot handle callback jobs" 
		unless $job->can_command;
	$queue->SUPER::add($job, @_);
}

sub finish
{
	my $queue = shift;
	for(;;) {
		$queue->checkjobs();
		$queue->startmore();

		my $running = 0;
		my $queued = keys %{$queue->{queue}};
		for my $host (@{$queue->{hosts}}) {
			my $hr = $queue->{status}{$host};
			$running += keys %{$hr->{running}};
			$queued += keys %{$hr->{queue}};
		}

		if ($debug > 2) {
			print STDERR "Finish loop top: $running running, $queued queued\n";
			for my $host (@{$queue->{hosts}}) {
				my $hr = $queue->{status}{$host};
				print "running: " . join(", ", map { $_ . ": " . $hr->{running}{$_}{desc} } keys %{$hr->{running}} ) . "\n" if $running;
				print "queued: " . join(", ", map { $_ . ": " . $hr->{running}{$_}{desc} } keys %{$hr->{queue}} ) . "\n" if $queued;
			}
		}

		return unless $queued || $running;

		print "Jobs are waiting to be run, but none are running\n" unless $running;
		sleep($queue->{sleeptime});
	}
}

1;

__END__

=head1 SYNOPSIS

 use Proc::JobQueue::BackgroundQueue;

 my $queue = new Proc::JobQueue::BackgroundQueue;

 $queue->add($job);

 $queue->checkjobs;

 $queue->finish;

=head1 DESCRIPTION

This is a job queue module for jobs that will be run in the background.

C<checkjobs()> needs to be called periodically.   When all the jobs
are queued, a call to C<finish()> will block until all the jobs
have completed.

Jobs are invoked using L<Proc::Background>.

=head1 LICENSE

Copyright (C) 2007-2008 SearchMe, Inc.
Copyright (C) 2008-2010 David Sharnoff.
Copyright (C) 2011 Google, Inc.
This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

