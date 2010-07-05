#!/usr/bin/perl 

package t::remotejob;

use strict;
use warnings;
use Test::More qw(no_plan);
use RPC::ToWorker;
use RPC::ToWorker::Callback;
use IO::Event qw(emulate_Event);
use File::Slurp::Remote::BrokenDNS qw($myfqdn %fqdnify);
use Eval::LineNumbers qw(eval_line_numbers);
use Tie::Function::Examples qw(%q_perl);
use FindBin;

my $finished = 0;

our $extra_remote_init = '';

my $debug = 0;

$RPC::ToWorker::debug = $debug;

END { ok($finished, 'finished') }

my $dead_timer = IO::Event->timer(
	after	=> 10,
	cb	=> sub {
		IO::Event::unloop_all();
		die "dead timer expired";
	},
);

my %invoked;

sub example_master
{
	my ($a, $b, %more) = @_;
	$invoked{example_master}++;
	is($a, 'foo', 'first value from slave');
	is($b, 'bar', 'second value from slave');
	ok($more{return7}, "local return7 key");
	is($more{return7}->(), 7, "local return7 value");
	is($more{nine}, 9, "local nine");
	return ('foo', 'bar');
}

do_remote_job(
	prefix		=> '## ',
	chdir		=> $FindBin::Bin,
	host		=> $myfqdn,
	data		=> { this => { and => 'that' }, show => 'something' },
	preload		=> [],
	prequel		=> "BEGIN { \@INC = (" . join(', ', map { "'$_'" } @INC) . ");\n" .
		eval_line_numbers(<<PREQUEL),
			BEGIN { no warnings; \$RPC::ToWorker::debug = $debug; }
			$extra_remote_init
			use RPC::ToWorker::Callback;
PREQUEL
	desc		=> 'test remote job',
	eval		=> eval_line_numbers(<<'REMOTE_JOB'),
		my $rec = 7;
		$rec++ if $data->{this};
		$rec++ if $data->{this}{and};
		$rec++ if $data->{this}{and} eq 'that';
		$rec++ if $data->{show};
		$rec++ if $data->{show} eq 'something';
		print STDERR "i want to trigger_error_handler\n";
		print "i want to trigger_output_handler\n";
		my ($foo, $bar) = master_call('', '::example_master', [qw(return7 nine)], 'foo', 'bar');
		return(rec => $rec, { $foo => $bar }, { 'bar' => $foo });
REMOTE_JOB
	when_done	=> sub {
		my ($recstr, $rec, $v1, $v2) = @_;
		is($recstr, 'rec', "values passed there and back");
		is($rec, 12, "values passed there and back");
		ok(ref($v1), "verify 1st return value");
		ok(ref($v2), "verify 2nd return value");
		is($v1->{foo}, 'bar', 'more verification of 1st return value');
		is($v2->{bar}, 'foo', 'more verification of 2nd return value');
		ok(1, "when_done called");
		$invoked{when_done} = 1;
	},
	all_done	=> sub {
		ok(1, "alldone");
		IO::Event::unloop_all();
	},
	error_handler	=> sub {
		my $e = join('', @_);
		print STDERR "#E $e" if $debug;
		$invoked{error_handler} = 1
			if $e =~ /trigger_error_handler/;
	},
	output_handler	=> sub {
		my $o = join('', @_);
		print STDERR "#O $o" if $debug;
		$invoked{output_handler} = 1
			if $o =~ /trigger_output_handler/;
	},
	local_data	=> {
		return7	=> sub { return 7 },
		nine	=> 9,
	},
);

ok(1, "started");

IO::Event::loop();

ok(1, "unlooped");

ok($invoked{when_done}, "when_done called");
# ok($invoked{error_handler}, "error output trigger");
ok($invoked{output_handler}, "output trigger");
ok($invoked{example_master}, "example master");

$finished = 1;

