#!/usr/bin/perl -w -I../lib

# $Id: jobqueue.t 13853 2009-07-24 00:59:44Z david $

use strict;
use Test::More;
use Proc::JobQueue::BackgroundQueue;
use aliased 'Proc::JobQueue::Sort';
use aliased 'Proc::JobQueue::Move';
use aliased 'Proc::JobQueue::Sequence';
use aliased 'Proc::JobQueue::Command';
use Sys::Hostname;
use File::Temp qw(tempdir);
use Time::HiRes qw(time);
use File::Slurp;
use File::Slurp::Remote::BrokenDNS qw($myfqdn);

my $debug = 0;

my $nfiles = 6;
my $generate_files_time = 0.05;
my $ndir = 5;

my $tmpdir = tempdir(CLEANUP => 1);

if ($debug) {
	open(STDOUT, "| tee $tmpdir/output")
		or die "open STDOUT | tee: $!";
} else {
	open(STDOUT, ">$tmpdir/output")
		or die "redirect STDOUT to $tmpdir/output: $!";
}
select(STDOUT);
$| = 1;
open(STDERR, ">&STDOUT") or die "dup STDOUT: $!";
select(STDERR);
$| = 1;

plan tests => $nfiles + 1;

my $queue = new Proc::JobQueue::BackgroundQueue (sleeptime => .01);
$queue->addhost($myfqdn, jobs_per_host => 1);

for my $n (1..$nfiles) {
	open my $fd, ">", "$tmpdir/d1.f$n" or die;
	my $t = time;
	while (time - $t < $generate_files_time) {
		my $r = rand();
		print $fd "f$n $r\n" x 400
			or die;
	}
	close($fd)
		or die;
}

for my $n (1..$nfiles) {
	my @seq;
	for (my $d = 1; $d < $ndir; $d++) {
		push(@seq, Sort->new({}, {}, "$tmpdir/d$d.s$n", "$tmpdir/d$d.f$n"));
		push(@seq, Command->new("mv $tmpdir/d$d.s$n $tmpdir/d$d.m$n"));
		my $nd = $d+1;
		push(@seq, Move->new({}, {}, "$tmpdir/d$d.m$n", "$tmpdir/d$nd.f$n", $myfqdn));
	}
	$queue->add(Sequence->new({}, {}, @seq));
}

$queue->finish();

my $combined = read_file("$tmpdir/output");

my @match = ($combined =~ /^(\+ .*)$/mg);

for my $n (1..$nfiles) {
	ok(-e "$tmpdir/d$ndir.f$n", "file $tmpdir/d$ndir.f$n exists");
}

is(scalar(@match), $nfiles * ($ndir -1) * 3, "count of commands run");
