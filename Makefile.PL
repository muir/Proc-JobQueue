
use ExtUtils::MakeMaker;

WriteMakefile( 
	'VERSION_FROM' => 'lib/Proc/JobQueue.pm',
	'NAME' => 'Proc-JobQueue',
	'ABSTRACT' => 'Jobqueue with dependency graph',
	'AUTHOR'   => 'David Muir Sharnoff <muir@idiom.org>',
	'dist' => { COMPRESS=>"gzip", SUFFIX=>"gz" },
	'PREREQ_PM'	=> {
		'File::Temp'			=> 0,
		'Tie::Function::Examples'	=> 0,
		'Scalar::Util'			=> 0,
		'Callback'			=> 0,
		'RPC::ToWorker'			=> 0,
		'Hash::Util'			=> 0,
		'Proc::Background'		=> 0,
		'File::Slurp::Remote::BrokenDNS'	=> 0,
		'Time::HiRes'			=> 0,
		'Hash::Util'			=> 0,
		'aliased'			=> 0,
		'Eval::LineNumbers'		=> 0,
		'File::Slurp'			=> 0,
	},
);

