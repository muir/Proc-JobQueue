#!/usr/bin/perl 


use strict;
use warnings;
use Test::More qw(no_plan);
use Object::Dependency;

my $finished = 0;

END { ok($finished, 'finished') }

my $dg = new Object::Dependency;

my $zero	= [ 0 ];
my $one		= [ 1 ];
my $two		= [ 2 ];
my $three	= [ 3 ];
my $four	= [ 4 ];
my $five	= [ 5 ];
my $six		= [ 6 ];
my $seven	= [ 7 ];
my $eight	= [ 8 ];
my $nine	= [ 9 ];

$dg->add($one, $zero);
$dg->add($two, $one);
$dg->add($three, $zero, $one);
$dg->add($four, $two);
$dg->add($five, $zero, $two);
$dg->add($six, $one, $two);
$dg->add($seven);
$dg->add($seven, $zero);
$dg->add($seven, $one);
$dg->add($seven, $two);
$dg->add($seven, $two);
$dg->add($eight, $three);
$dg->add($nine, $zero, $three);

my @i;

sub check
{
	my @i = $dg->independent;
	return join(' ', sort map { $_->[0] } @i);
}

is(check(), "0");

$dg->delete($zero);

is(check(), "1");

$dg->delete($one);

is(check(), "2 3");

$dg->delete($three);

is(check(), "2 8 9");

$dg->delete($two, $eight);

is(check(), "4 5 6 7 9");

$dg->delete($four, $seven);

is(check(), "5 6 9");

$dg->delete($five, $six, $nine);

is(check(), "");

$finished = 1;
