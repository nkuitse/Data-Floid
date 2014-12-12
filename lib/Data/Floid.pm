package Data::Floid;

use strict;
use warnings;

use vars qw($VERSION $DBVERSION);

$VERSION = '0.06';
$DBVERSION = '1.0';

use Fcntl;
use Digest;

# Key prefixes
use constant CTL => '!';  # Control entry
use constant KEY => '#';  # Minted ID
use constant PRV => '-';  # Previous ID number
use constant TPL => '@';  # Template
use constant USR => ':';  # User entry

# Value prefixes
use constant UND => '~';
use constant STR => '$';

my %mode2mode = (
    'r'  => O_RDONLY,
    'ro' => O_RDONLY,
    'rw' => O_RDWR|O_CREAT,
    'r+' => O_RDWR|O_CREAT,
    'w'  => O_WRONLY|O_CREAT,
    '<'  => O_RDONLY,
    '>'  => O_WRONLY|O_CREAT,
    '<>' => O_RDWR|O_CREAT,
    '+>' => O_RDWR|O_CREAT,
    '+<' => O_RDWR,
);

sub new {
    my $cls = shift;
    unshift @_, 'path' if @_ % 2;
    my %self = @_;
    my ($path, $dir, $file) = @self{qw(path directory file)};
    if (defined $path) {
        if (defined $dir) {
            $file = $path;
        }
        elsif (defined $file) {
            $dir = $path;
        }
        elsif (-f $path) {
            ($dir, $file) = (dirname($path), basename($path));
        }
        elsif (-d _) {
            $dir = $path;
        }
    }
    $file = 'floid' if !defined $file;
    $dir = '.' if !defined $dir;
    my $ifile = "$dir/$file";
    my $lfile = $ifile . '.log';
    $self{'mode'} ||= 'r';
    my $mode = $mode2mode{$self{'mode'}};
    die "Unrecognized mode: $self{'mode'}" if !defined $mode;
    my $perm = $self{'permissions'} || 0644;
    my $dbm = $self{'dbm'} || 'AnyDBM_File';
    eval "use $dbm; 1" or die "Can't instantiate $dbm: $@";
    my @args = ($dbm, $ifile, $mode, $perm);
    #push @args, $DB_File::DB_BTREE if $dbm eq 'DB_File';
    $self{'tieobj'} = tie my %index, @args or die "Can't open index file $ifile: $!";
    bless {
        %self,
        'logfile' => $lfile,
        'logfh' => undef,
        'index' => \%index,
    }, $cls;
}

sub DESTROY {
    my ($self) = @_;
    delete @$self{qw(tieobj index)};
}

sub mint {
    my ($self, $tpl, $val) = @_;
    my $index = $self->{'index'};
    my $logfh = $self->{'logfh'};
    if (!defined $logfh) {
        open $logfh, '>>', $self->{'logfile'} or die "Can't open log file $self->{'logfile'}: $!";
        $self->{'logfh'} = $logfh;
    }
    $val = val($val);
    if ($tpl =~ /^([^%]*)%R(\d*)x([^%]*)$/) {
        my ($pfx, $size, $sfx) = ($1, $2, $3);
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
            my $key = key($next);
            if (!defined $index->{$key}) {
                $index->{$key} = $val;
                print $logfh join(' ', MNT => time, IDN => esc($next), TPL => $tpl, VAL => $val), "\n";
                return $next;
            }
        }
        return $next;
    }
    elsif ($tpl =~ /^([^%]*)%N(\d*)([dx])([^%]*)$/) {
        my ($pfx, $size, $type, $sfx) = ($1, $2, $3, $4);
        my $fmt = '%';
        $fmt .= '0'.$size if $size;
        $fmt .= $type;
        while (1) {
            my $nextint = ++$index->{prv($tpl)};
            my $next = $pfx . sprintf($fmt, $nextint) . $sfx;
            my $key = key($next);
            next if defined $index->{$key};
            $index->{$key} = $val;
            print $logfh join(' ', MNT => time, IDN => esc($next), TPL => $tpl, NXT => $nextint+1, VAL => $val), "\n";
            return $next;
        }
    }
    else {
        die;
    }
}

sub init {
    my ($self) = @_;
    my $index = $self->{'index'};
    die 'Already initialized' if keys %$index;
    $index->{ctl('floidb')} = val($DBVERSION);
}

sub seed {
    my ($self, $tpl, $seed) = @_;
    my $index = $self->{'index'};
    if ($tpl =~ /^([^%]|%%)*%N/) {
        $seed = 0 if !defined $seed;
        my $prv = prv($tpl);
        die if defined $index->{$prv};
        $index->{$prv} = $seed;
    }
    $index->{tpl($tpl)} = UND;
}

sub get {
    my ($self, $key) = @_;
    $key = key($key);
    my $index = $self->{'index'};
    my $escval = $index->{$key};
    exit 2 if !defined $escval;
    return unval($escval);
}

sub set {
    my ($self, $key, $val) = @_;
    $key = key($key);
    $val = val($val);
    my $index = $self->{'index'};
    my $oldval = $index->{$key};
    exit 1 if !defined $oldval;
    $index->{$key} = $val;
    my $logfh = $self->{'logfh'};
    if (!defined $logfh) {
        open $logfh, '>>', $self->{'logfile'} or die "Can't open log file $self->{'logfile'}: $!";
        $self->{'logfh'} = $logfh;
    }
    print $logfh join(' ', SET => time, ID => esc($_[1]), VAL => $val), "\n";
}

sub all {
    my ($self, $raw) = @_;
    my $index = $self->{'index'};
    if ($raw) {
        return wantarray ? %$index : { %$index }
    }
    elsif (wantarray) {
        return map  { unesc(substr($_, 1)) }
               grep { substr($_, 0, 1) eq KEY }
               keys %$index;
    }
    else {
        my %h;
        while (my ($k, $v) = each %$index) {
            next if substr($k, 0, 1) ne KEY;
            $h{unkey($k)} = unval($v);
        }
        return \%h;
    }
}

sub uget {
    my ($self, $key) = @_;
    my $index = $self->{'index'};
    return unval($index->{key($key)} // return);
}

sub uset {
    my ($self, $key, $val) = @_;
    my $index = $self->{'index'};
    $index->{key($key)} = val($val);
}

# Private methods and functions

sub ctl { CTL . shift }
sub key { KEY . esc(shift) }
sub prv { PRV . esc(shift) }
sub tpl { TPL . esc(shift) }
sub usr { USR . esc(shift) }
sub val { defined($_[0]) ? STR . esc($_[0]) : UND }

sub esc {
    my ($val) = @_;
    $val =~ s{([=\x00-\x1f])}{sprintf '=%02X', ord($1)}eg;
    $val =~ s/^ | $/sprintf('=%02X', ord ' ')/eg;
    return $val;
}

sub unesc {
    my ($esc) = @_;
    $esc =~ s/=(..)/chr(hex $1)/eg;
    return $esc;
}

sub unval {
    my ($escval) = @_;
    return if $escval eq UND;
    substr($escval, 0, 1) eq STR or die;
    return unesc(substr($escval, 1));
}

sub unkey { substr $_[0], 1 }

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

There are currently two types of formula: random hex strings and serial
numbers.  To create an identifier using a particular formula, simply pass a
template (a string that specifies the formula) to B<Data::Floid::mint>.

A template contains the following elements:

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

Create a B<Data::Floid> instance with index and log files kept in the specified
directory.

=item B<mint>

    $id = $floid->mint('%Nd');        # 1, 2, etc.
    $id = $floid->mint('<%R4x>');     # e.g., <b8d6>
    $id = $floid->mint($pattern);
    $id = $floid->mint($pattern, $data);

Mint a unique identifier using the specified pattern.

The second argument, if present, must be a scalar value; this (or B<undef> if a
second parameter is not given) is stored without any interpretation and may be
retrieved or replaced later using B<get> or B<set>, respectively.

=item B<get>

    $val = $floid->get($id);

Return any data associated with a previously minted identifier.  If no such
identifier has been minted, an exception is thrown.  If an identifier has no
associated value, the undefined value is returned.

=item B<set>

    $floid->set($id, $val);

Set the value associated with a previously minted identifier.  If no such
identifier has been minted, an exception is thrown.  I<$val> must be a scalar
value, or undefined.

=item B<all>

    @array = $floid->all;
    $hash  = $floid->all;

Return all identifiers that have been minted.  In list context, only the
identifiers are returned; in scalar context a reference to a hash mapping from
identifiers to their associated values is returned.

=back

=cut
