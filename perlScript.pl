use strict;
use warnings;
use Carp;
use English qw(-no_match_vars);


my $files = 'output.csv';
if (-f $files) {
	unlink $files
		or croak "Cannot delete $files: $!";
}

my $OUTFILE;


# use the three arguments version of open
# and check for errors
open $OUTFILE, '>>', $files
	or croak "Cannot open $files: $OS_ERROR";
my @fileNames = ("dataLarge1.csv","dataLarge2.csv","dataLarge3.csv","dataLarge4.csv");
#my @fileNames = ("dataLarge1.csv");

my $bucketWidth=50;
print { $OUTFILE } "File Name,Bucket Width,Iteration,Missing,Less,Time,\n";
foreach (@fileNames)
{
	for($bucketWidth=50;$bucketWidth<=550;$bucketWidth=$bucketWidth+10){

		for($a=0;$a<3;$a=$a+1){
				print { $OUTFILE } "$_,$bucketWidth,$a,";
				my $cmd = "spark-submit --master local[*] --driver-memory 256g --class main.scala.neighbor target/scala-2.10/spark_proj-assembly-1.0.jar $_ $bucketWidth";
				my @output = `$cmd`;
				chomp @output;

				foreach my $line (@output)
				{
						print { $OUTFILE } "$line,"
								or croak "Cannot write to $files: $OS_ERROR";
				}
				print { $OUTFILE } "\n"
		}
	}
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
