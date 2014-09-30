package Data::Floid;

use strict;
use warnings;

use vars qw($VERSION);

$VERSION = '0.04';

use Fcntl;
use DB_File;

sub new {
    my $cls = shift;
    unshift @_, 'directory' if @_ % 2;
    my %self = @_;
    my ($ifile, $lfile) = map { $self{'directory'} . '/' . $_ } qw(floid.idx floid.log);
    tie my %index, 'DB_File', $ifile, O_CREAT, 0644, $DB_BTREE or die "Can't open index file $ifile: $!";
    bless {
        %self,
        'logfile' => $lfile,
        'logfh' => undef,
        'index' => \%index,
    }, $cls;
}

sub mint {
    my ($self, $spec, $data) = @_;
    my $index = $self->{'index'};
    my $logfh = $self->{'logfh'};
    if (!defined $logfh) {
        open $logfh, '>>', $self->{'logfile'} or die "Can't open log file $self->{'logfile'}: $!";
    }
    if ($spec =~ /^([^%]*)%R(\d*)x([^%]*)$/) {
        my ($pfx, $size, $sfx) = ($1, $2, $3);
        use Digest;
        my $hash = eval { Digest->new('SHA-256') }
                || eval { Digest->new('MD5'    ) }
                || die;
        my $next;
        my $fmt = '%';
        $fmt .= "-$size.$size" if $size;
        $fmt .= 's';
        while (1) {
            $hash->add($$, ':', rand, ':', time);
            $next = $pfx . sprintf($fmt, $hash->clone->hexdigest) . $sfx;
            if (!exists $index->{'#'.$next}) {
                $index->{'#'.$next} = 1; # serialize($data);
                print $logfh 'ID ', $next, ' FROM ', $spec;
                print $logfh ' DATA ', serialize($data) if defined $data;
                print $logfh "\n";
                return $next;
            }
        }
        return $next;
    }
    elsif ($spec =~ /^([^%]*)%N(\d*)([dx])([^%]*)$/) {
        my ($pfx, $size, $type, $sfx) = ($1, $2, $3, $4);
        my $fmt = '%';
        $fmt .= '0'.$size if $size;
        $fmt .= $type;
        my $nextint = ++$index->{'<'.$spec};
        my $next = $pfx . sprintf($fmt, $nextint) . $sfx;
        die if exists $index->{'#'.$next};
        $index->{'#'.$next} = 1; # serialize($data);
        print $logfh 'ID ', $next, ' FROM ', $spec, ' INCR ', $nextint+1;
        print $logfh ' DATA ', serialize($data) if defined $data;
        print $logfh "\n";
        return $next;
    }
    else {
        die;
    }
}

sub serialize {
    my ($val) = @_;
    return '~' if !defined $val;
    my $ref = ref $val;
    return '=' .  $val if $ref eq '';
    return '=' . $$val if $ref eq 'SCALAR';
    die;
}

1;

=pod

=head1 NAME

Data::Floid - simple, lightweight unique identifier generator

=head1 SYNOPSIS

    $floid = Data::Floid->new($dir);
    $id = $floid->mint('0x%R16x');
    $id = $floid->mint('%Nd');

=head1 DESCRIPTION

B<Data::Floid> creates ("mints") unique identifiers using simple formulas.

There are currently two types of formula: random hex strings and serial numbers.  To create an identifier using a particular formula, simply pass a string that specifies the formula to B<Data::Floid::mint>.  A specification contains the following elements:

=over 4

=item I<prefix> (optional)

Any string that does B<not> contain a percent sign C<%>.

=item C<%>

=item I<type>

Either C<N> (a positive serial number beginning with 1) or C<R> (a string of
random bytes).

=item I<length>

An integer that specifies how long the variable part of the identifier should
be.  If omitted (or zero), the identifier will be only as long as it needs to
be.

=item I<output>

Either C<d> (decimal number, valid only for type C<N>) or C<x> (hexadecimal
number, valid for type C<N> or C<R>).

=item I<suffix> (optional)

Any string.

=back

=head1 METHODS

=over 4

=item B<new>

    $floid = Data::Floid->new;
    $floid = Data::Floid->new($dir);

Create a B<Data::Floid> instance with index and log file kept in the specified
directory.

=item B<mint>

    $id = $floid->create('%Nd');        # 1, 2, etc.
    $id = $floid->create('abc%Ndxyz');  # abc1xyz, abc2xyz, etc.
    $id = $floid->create('<%R4x>');     # e.g., <b8d6>

=back

=cut
