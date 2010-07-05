
package Object::Dependency;

use strict;
use warnings;
use Scalar::Util qw(refaddr blessed);
use Hash::Util qw(lock_keys);
use Carp qw(confess);
use List::MoreUtils qw(uniq);

my $debug = 0;

sub new
{
	my ($pkg, %more) = @_;
	my $self = {
		addrmap		=> {},
		independent	=> {},
		stuck		=> {},
		%more,
	};
	bless $self, $pkg;
	lock_keys(%$self);
	return $self;
}

sub newitem
{
	my ($self, $i) = @_;
	my $addr = refaddr($i);
	my %item = (
		dg_addr		=> $addr,
		dg_item		=> $i,
		dg_depends	=> {},
		dg_blocks	=> {},
		dg_active	=> 0,
		dg_lock		=> 0,
		dg_desc		=> undef,
		dg_stuck	=> undef,
	);
	return %item if wantarray;
	lock_keys(%item);
	return \%item;
}

sub get_item
{
	my ($self, $addr) = @_;
	return $self->{addrmap}{$addr};
}

sub get_addr
{
	my ($self, $item) = @_;
	my $addr = refaddr($item);
	die unless $self->{addrmap}{$addr};
	return $addr;
}

sub unlock
{
	my ($self, $item) = @_;
	my $da = refaddr($item);
	my $dao = $self->{addrmap}{$da} or confess;
	$dao->{dg_lock} = 0;
	$dao->{dg_active} = 0;
}

sub add
{
	my ($self, $item, @depends_upon) = @_;
	for my $i ($item, @depends_upon) {
		my $addr = refaddr($i);
		next if $self->{addrmap}{$addr};
		$self->{addrmap}{$addr} = $self->newitem($i);
		$self->{independent}{$addr} = $self->{addrmap}{$addr};
		printf STDERR "ADD ITEM %s\n", $self->desc($addr) if $debug;
	};
	my $da = refaddr($item);
	my $dao = $self->{addrmap}{$da};
	delete $self->{independent}{$da}
		if @depends_upon;
	for my $d (@depends_upon) {
		my $addr = refaddr($d);
		my $o = $self->{addrmap}{$addr};
		$o->{dg_blocks}{$da} = $dao;
		$o->{dg_active} = 0;
		$dao->{dg_depends}{$addr} = $o;
	}
}

sub remove_all_dependencies
{
	my ($self, @items) = @_;
	my (@remove);
	for my $i (@items) {
		my $addr = refaddr($i);
		my $o = $self->{addrmap}{$addr};
		for my $ubi (keys %{$o->{dg_blocks}}) {
			my $unblock = delete $o->{dg_blocks}{$ubi};
			delete $unblock->{dg_depends}{$addr};
			$self->remove_all_dependencies($unblock);
			push(@remove, $unblock);
			next if keys %{$unblock->{dg_depends}};
			$self->{independent}{$unblock->{dg_addr}} = $unblock;
		}
	}
	$self->remove_dependency(grep { $self->{addrmap}{refaddr($_)} } uniq @remove);
}

sub remove_dependency
{
	my ($self, @items) = @_;
	for my $i (@items) {
		my $addr = refaddr($i);
		if ($debug) {
			my($p,$f,$l) = caller;
			printf STDERR "REMOVE ITEM %s:%d: %s %s\n", $f, $l, $self->desc($addr), ($i->{desc} ? $i->{desc} : ($i->{trace} ? $i->{trace} : "$i"));
		}
		delete $self->{independent}{$addr};
		confess unless defined $self->{addrmap}{$addr};
		my $o = delete $self->{addrmap}{$addr};
		if (keys %{$o->{dg_depends}}) {
			printf STDERR "attempting to remove %s but it has dependencies that aren't met:\n", $self->desc($o);
			for my $da (keys %{$o->{dg_depends}}) {
				printf STDERR "\t%s\n", $self->desc($da);
			}
			die "fatal error";
		}
		for my $unblock (values %{$o->{dg_blocks}}) {
			delete $unblock->{dg_depends}{$addr};
			$unblock->{dg_active} = 0;
			next if keys %{$unblock->{dg_depends}};
			$self->{independent}{$unblock->{dg_addr}} = $unblock;
		}
	}
}

sub stuck_dependency
{
	my ($self, $item, $problem) = @_;
	my $addr = refaddr($item);
	my $o = $self->{addrmap}{$addr};
	$o->{dg_stuck} = $problem || sprintf("stuck called from %s line %d", (caller())[1,2]);
	$self->{stuck}{$addr} = $o;
}

sub independent
{
	my ($self, %opts) = @_;

	my $count = $opts{count} || 0;
	my $active = $opts{active} || 0;
	my $lock = $opts{lock} || 0;

	my @ind;
	for my $o (values %{$self->{independent}}) {
		next if $active && $o->{dg_active};
		next if $o->{dg_lock};
		push(@ind, $o->{dg_item});
		$o->{dg_active} = 1;
		$o->{dg_lock} = $lock;
		last if $count && @ind == $count;
	}
	return @ind if @ind;
	return () if keys %{$self->{independent}};
	return () unless keys %{$self->{addrmap}};
	$self->dump_graph();
	confess "No independent objects, but there are still objects in the dependency graph";
}

sub alldone
{
	my ($self) = @_;
	return 0 if keys %{$self->{independent}};
	return 0 if keys %{$self->{addrmap}};
	return 1;
}

sub desc
{
	my ($self, $addr, $desc) = @_;
	my $o;
#print "ADDR: $addr\n";
	if (ref($addr)) {
		$o = $addr;
		$addr = refaddr($addr);
	} else {
		$o = $self->{addrmap}{$addr};
	}
#print "O: $o\n";
	return "NO SUCH OBJECT $addr" unless $o;
	my $node = $o->{dg_item};
#print "NODE: $node\n";
	$o->{dg_desc} = $desc
		if defined $desc;
	$desc = '';
	$desc .= 'INDEPENDENT ' if $self->{independent}{$addr};
	$desc .= 'LOCKED ' if $o->{dg_lock};
	$desc .= 'ACTIVE ' if $o->{dg_lock};
	$desc .= "$addr ";
	if ($o->{dg_desc}) {
		$desc .= $o->{dg_desc};
	} elsif (blessed($node)) {
		if ($node->isa('Proc::JobQueue::Job')) {
			no warnings;
			$desc .= "JOB$node->{jobnum} $node->{status} $node->{desc}";
		} elsif ($node->isa('Proc::JobQueue::DependencyTask')) {
			$desc .= "TASK $node->{desc}";
		} else {
			die;
		}
	} else {
		$desc .= "???????????????????";
	}
	$desc .= " STUCK: $o->{dg_stuck}" if $o->{dg_stuck};
	return $desc;
}

sub dump_graph
{
	my ($self) = @_;

	printf "Dependency Graph, alldone=%d\n", $self->alldone;
	my %desc;
	for my $addr (sort keys %{$self->{addrmap}}) {
		$desc{$addr} = $self->desc($addr);
	}
	for my $addr (sort keys %{$self->{addrmap}}) {
		print "\t$desc{$addr}\n";
		my $node = $self->{addrmap}{$addr};
		for my $b (keys %{$node->{dg_blocks}}) {
			print "\t\tBLOCKS\t$desc{$b}\n";
		}
		for my $d (keys %{$node->{dg_depends}}) {
			print "\t\tDEP_ON\t$desc{$d}\n";
		}
	}
}

1;

__END__

=head1 NAME

Object::Dependency - maintain a dependency graph

=haad1 SYNOPSIS

 use Object::Dependency;

 my $graph = Object::Dependency->new()

 $graph->add($object, @objects_the_first_object_depends_upon)

 $graph->remove_dependency(@objects_that_are_no_longer_relevant)

 @objects_without_dependencies = $graph->independent;

 my $addr = $graph->get_addr($item);

 my $item = $graph->get_item($addr);

=head1 DESCRIPTION

This module maintains a simple dependency graph.    
Items can be C<add>ed more than once to note additional depenencies.
Dependency relationships cannot be removed except by removing 
objects entirely.

We do not currently check for cycles so please be careful!  

Although often used with L<Proc::JobQueue>, it does not have to
be paired with L<Proc::JobQueue>.

=head1 CONSTRUCTION

Construction is easy: no parameters are expected.

=head1 METHODS

=over 

=item add($object, @depends_upon_objects)

Adds an item (C<$object>) to the dependency graph and notes which items
it depends upon.  

=item remove_all_dependencies(@objects)

Removes the C<@objects> from the dependency graph.  
All objects dependent on C<@objects> will also be removed.

=item remove_dependency(@objects)

Removes the C<@objects> from the dependency graph.  
Dependencies upon
these objects will be considered to be satisfied.

=item stuck_dependency($object, $description_of_problem)

Mark that the C<$object> will never be removed from the dependency graph because
there is some problem with it.   All objects that depend upon C<$object> will now
be considered "stuck".

=item independent(%opts)

Returns a list of objects that do not depend upon other objects.  Mark the returned
objects as active and locked.

Options are:

=over

=item count => COUNT

Return at most COUNT items.

=item active => 1

Normally active objects are not included in the returned list.  With C<active => 1>, 
active objects are returned.

=item lock => 1

Normally locked objects are not included in the returned list.  With C<lock => 1>, 
locked objects are returned.

=back

=item alldone()

Returns true if there are no non-stuck objects in the dependency graph.

=item desc($object, $description)

Sets the description of the object (if C<$description> is defined).

Returns the description of the object, annotated by it's dependency graph
status: LOCKED, INDEPENDENT, ACTIVE, or STUCK.

Special handling is done for L<Proc::JobQueue::Job> 
and L<Proc::JobQueue::DependencyTask> objects.

=item dummp_graph

Prints the dependency graph (described objects with the dependencies).

=back

=head1 SEE ALSO

L<Proc::JobQueue::DependencyQueue>

=head1 LICENSE

This package may be used and redistributed under the terms of either
the Artistic 2.0 or LGPL 2.1 license.

