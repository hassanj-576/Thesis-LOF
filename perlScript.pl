use strict;
use warnings;
use Carp;
use English qw(-no_match_vars);


my $files = 'output2.csv';
if (-f $files) {
	unlink $files
		or croak "Cannot delete $files: $!";
}

my $OUTFILE;


# use the three arguments version of open
# and check for errors
open $OUTFILE, '>>', $files
	or croak "Cannot open $files: $OS_ERROR";


my $bucketWidth=400;
print { $OUTFILE } "N,iteration,fast,Time,\n";

my $txt=".csv";
my $output="output";
my $N;
for($N=100;$N<=500;$N=$N+100){
	my $outputFile = `python dataGenerator.py $N$txt 1 $N 5`;
	for($a=0;$a<1;$a=$a+1){
			print { $OUTFILE } "$N,$a,0,";
			my $cmd = "spark-submit --master local[*] --driver-memory 256g --class main.scala.mainClass target/scala-2.10/spark_proj-assembly-1.0.jar $N$txt $bucketWidth $N$output 10 9 8 7 6";
			my @output = `$cmd`;
			chomp @output;

			foreach my $line (@output)
			{
					print { $OUTFILE } "$line"
							or croak "Cannot write to $files: $OS_ERROR";
			}
			print { $OUTFILE } "\n";
			my $outputFile2 = `rm -rf $N$output*`;
	}
	my $outputFile2 = `rm -rf $N$txt`;

}

my $hostname = 'cwi.nl';
my $this_day = 'date';
my $email = "hassan.jalil576\@gmail.com";
my $to = "$email";
my $from = "h.jalil\@$hostname";
my $subject = "JOB COMPLETE - $this_day";
my $message = "The Job Running on Stones02 is complete...";
open(MAIL, "|/usr/sbin/sendmail -t");
print MAIL "To: $to\n";
print MAIL "From: $from\n";
print MAIL "Subject: $subject\n\n";
print MAIL $message;
close(MAIL);
