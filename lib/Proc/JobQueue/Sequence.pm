
package Proc::JobQueue::Sequence;

# $Id: Sequence.pm 13848 2009-07-23 21:34:00Z david $

use strict;
use warnings;
use Proc::JobQueue::Job;
use Hash::Util qw(lock_keys unlock_keys);
our @ISA = qw(Proc::JobQueue::Job);

my $debug = $Proc::JobQueue::debug;

{
	package Proc::JobQueue::Sequence::Command;
	use strict;
	use warnings;
	our @ISA = qw(Proc::JobQueue::Sequence);
	sub command { my $job = shift; return $job->{jobs}[0]->command(@_) }
}

{
	package Proc::JobQueue::Sequence::Startup;
	use strict;
	use warnings;
	our @ISA = qw(Proc::JobQueue::Sequence);
	sub startup { my $job = shift; return $job->{jobs}[0]->startup(@_) }
}

sub new
{
	my ($pkg, $opts, $config, @jobs) = @_;

	my $job = $pkg->SUPER::new(
		opts		=> $opts,
		config		=> $config,
		jobs		=> \@jobs,
		fubar		=> 0,
		priority	=> 20,
		callback	=> sub { 
			my ($job, $host, $jobnum, $queue) = @_;
			# runs quick!
			$job->finished(0);
		},
	);
	$job->rebless;
}

sub rebless
{
	my ($job) = @_;
	my $jobs = $job->{jobs};
	my $new;
	if ($jobs && @$jobs && $jobs->[0]->can('command')) {
		$new = 'Proc::JobQueue::Sequence::Command';
	} elsif ($jobs && @$jobs && $jobs->[0]->can('startup')) {
		$new = 'Proc::JobQueue::Sequence::Startup';
	} else {
		$new = __PACKAGE__;
	}
	unlock_keys(%$job);
	bless $job, $new;
	lock_keys(%$job);
	return $job;
}

sub runnable 	{ my $job = shift; return $job->{jobs}[0]->runnable(@_) }
sub start	{ my $job = shift; return $job->{jobs}[0]->start(@_) }

sub checkjob
{
	my $job = shift;
	my $e = $job->{jobs}[0]->checkjob(@_);
	return undef unless defined $e;
	$job->{procbg} = $job->{jobs}[0]{procbg};  # fakes out Proc::JobQueue::Job::checkjob()
	$job->SUPER::checkjob(@_);
}

sub jobnum
{
	my ($job, $jobnum) = @_;
	$job->{jobs}[0]->jobnum($jobnum . ":" . scalar(@{$job->{jobs}}))
		if $jobnum;
	$job->SUPER::jobnum($jobnum);
}

sub host
{
	my $job = shift;
	$job->{jobs}[0]->host(@_);
	$job->SUPER::host(@_);
}

sub finished
{
	my $job = shift;
	$job->{jobs}[0]->finished(@_);
	$job->SUPER::finished(@_);	# should call success()
}

sub calljobdone
{
	# skip
}

sub success 
{
	#
	# By this point, the JobQueue is done with this job so it needs
	# to be re-submitted if it's to run again.
	#
	my ($job) = @_;
	print "# SEQUENCE success on $job->{jobnum}\n" if $debug > 8;
	my $queue = $job->{queue};
	my $host = $job->{host};
	my $first = $job->{jobs}[0];
	$first->finished(0, $queue, $host);
	if ($first->is_finished) {
		shift(@{$job->{jobs}});
		my $next = $job->{jobs}[0];
		if ($next) {
			# don't set a queue
			$next->host($job->host);
			$job->jobnum($job->jobnum);
			$queue->add($job);
			$queue->startmore;
		} else {
			print STDERR "no more jobs in sequence\n" if $debug > 1;
		}
	} else {
		print STDERR "first isn't finished\n" if $debug > 1;
		$queue->add($job);
	}
}

sub failed 
{
	my ($job, $e) = @_;
	my $first = $job->{jobs}[0];
	$first->failed($e);
	$job->{fubar} = 1;
}

sub is_finished 
{
	my $job = shift;
	if ($job->{fubar}) {
		# print STDERR "# seqence broken by bad exit\n";
		return 1;
	}
	if (@{$job->{jobs}}) {
		# print STDERR "# jobs still in queue\n";
		return 0;
	}
	my $j1f = $job->{jobs}[0]->is_finished;
	# print STDERR "Job1 is $j1f\n";
	return $j1f;
}

sub cancelled
{
	my $job = shift;
	my $first = $job->{jobs}[0];
	return $job->{fubar} || $first->cancelled;
}

1;

__END__

=head1 NAME

 Proc::JobQueue::Sequence - do a sequence of background jobs

=head1 SYNOPSIS

 use Proc::JobQueue::BackgroundQueue;
 use aliased 'Proc::JobQueue::Sequence';

 my $queue = new Proc::JobQueue::BackgroundQueue;

 my $job = Sequence->new($opts, $config,
	Sort->new($opts, $config, $sorted_output, @unsorted_files),
	Move->new($opts, $config, $sorted_output, $final_name, $final_host),
 );

 $queue->add($job);

 $queue->finish;

=head1 DESCRIPTION

This is a subclass of L<Proc::JobQueue::Job>.
In the background, do a sequence of jobs.  If a job fails,
the jobs later in the sequence are cancelled.

=head1 SEE ALSO

L<Proc::JobQueue>
L<Proc::JobQueue::Job>
L<Proc::JobQueue::BackgroundQueue>
L<Proc::JobQueue::Command>
L<Proc::JobQueue::Move>
L<Proc::JobQueue::Sort>

=head1 LICENSE

This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

