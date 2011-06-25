
package Proc::JobQueue::DependencyTask;

use strict;
use warnings;
use Callback;
use Scalar::Util qw(blessed);
use Carp qw(confess);

sub new
{
	my ($pkg, %params) = @_;
	my ($cpkg, $file, $line) = caller;
	confess "A function is required" unless $params{func};
	$params{args} ||= [];
	my $task = bless { 
		desc		=> $params{desc} || "no desc, called from $file:$line",
		trace		=> "$file:$line",
		errors		=> $params{errors} || sub { print STDERR @_ },
		%params,
	}, $pkg;
	$task->set_cb($params{func}, $params{args});
	return $task;
}

sub set_cb
{
	my ($task, $func, $args) = @_;

	if (blessed($func) && $func->isa('Callback')) {
		$task->{cb} = $func;
	} else {
		$task->{cb} = Callback->new($func, @$args);
	}
}

sub run_dependency_task
{
	my ($task, $dependency_graph) = @_;

	my ($code, @more);
	eval {
		($code, @more) = $task->{cb}->call($task, $dependency_graph);
	};
	if ($@) {
		$task->{errors}->("----------------------- FAILURE\nTask $task->{desc} failed: $@\n");
		$dependency_graph->stuck_dependency($task, "call failed: $@");
		return 0;
	}

	if ($code eq 'done') {
		$dependency_graph->remove_dependency($task);
		return 1;
	} elsif ($code eq 'keep' || $code eq 'ignore') {
		# nada
	} elsif ($code eq 'requeue') {
		$task->set_cb(@more) if @more;
		$dependency_graph->unlock($task);
	} else {
		die "bad return code from $task->{desc}";
	}
 	return 0;
}

1;

__END__

=head1 NAME

Proc::JobQueue::DependencyTask - callbacks for use with DependencyQueue

=head1 SYNOPSIS

 use Proc::JobQueue::DependencyTask;
 use Object::Dependency;

 $graph = Object::Dependency->new()

 $task = Proc::JobQueue::DependencyTask->new( $description, $callback );

 $graph->add($task);

=head1 DESCRIPTION

A task is lighter than a job (L<Proc::JobQueue::DependencyJob>) -- it
is never more than a callback.   It does not get schedueled as a
job (L<Proc::JobQueue>).

Tasks can be put in a dependency graph (L<Object::Dependency>) 
and used by L<Proc::JobQueue::DependencyQueue>.

A task requires a callback.  
The callback is invoked in array context.  The first element of the
return value must be one of the following strings:

=over

=item C<done>

Remove this task from the dependency graph.

=item C<requeue>

Unlock the task in the dependency graph so that it can be called
again.  If there is a second return value from the callback, the
callback is replaced with the second return value.

=item C<keep>

Keep the dependency around but take no further action.

Later, the task will need to removed from the dependency graph with

 $dependency_graph->remove_dependency($task)

=back

=head1 LICENSE

Copyright (C) 2007-2008 SearchMe, Inc.   
Copyright (C) 2008-2010 David Sharnoff.
Copyright (C) 2011 Google, Inc.
This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

