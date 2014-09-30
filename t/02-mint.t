use strict;
use warnings;

use Test::More tests => 3;

use_ok( 'Data::Floid' );
my $floid = Data::Floid->new('example');
ok( defined $floid );
isa_ok( $floid, 'Data::Floid' );

