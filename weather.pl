#!/usr/bin/perl
use strict;
use warnings;
use LWP::UserAgent;
use Config::General;
use DBI;
use JSON;
use Text::CSV;
use Data::Dumper;

#Verify input
my ($command, $latitude, $longitude) = @ARGV;
unless (scalar @ARGV == 3) { die "Use: perl weather.pl command latitude longitude"; }
unless ($latitude =~ /^-?\d+(\.\d+)?$/ && $longitude =~ /^-?\d+(\.\d+)?$/) {
        die "Error: Latitude and Longitude need to be numbers.";
}
unless ($command eq "scrape" || $command eq "display" || $command eq "display-full" || $command eq "both") {
        die "Error: Command must be 'scrape', 'display', 'display-full', or 'both'.";
}

#Load Config file
my %config = Config::General->new("./config.conf")->getall;

#Create database connection
my $db_name = $config{'database'}{'db_name'};
my $db_user = $config{'database'}{'db_user'};
my $db_password = $config{'database'}{'db_password'};
my $dbh = DBI->connect("dbi:Pg:dbname=$db_name;host=localhost",
	$db_user,
	$db_password,
	{ RaiseError => 1, AutoCommit => 0 }
) or die("Couldn't open database: ".$DBI::errstr);

#Run whichever command/s was inputted
if ($command eq "scrape" || $command eq "both") {
	full_scrape();
}
if ($command eq "display" || $command eq "display-full" || $command eq "both") {
	display();
}

#######################
# Primary Subroutines #
#######################
sub full_scrape {
	nws_forecast();
	vc_forecast();
	meteo_forecast();
	aggregate_staging_data();
}

sub display {
	my $query = "SELECT * FROM forecasts WHERE latitude=? AND longitude=? AND forecast_date>=NOW() AND EXTRACT(HOUR FROM forecast_date)=12 ORDER BY forecast_date;";
	if ($command eq "display-full") { $query = "SELECT * FROM forecasts WHERE latitude=? AND longitude=? AND forecast_date>=NOW() ORDER BY forecast_date;"; }
	my $sql = $dbh->prepare($query);
	$sql->execute($latitude, $longitude);
	print "Date and time:\t\tTemp:\tW.Spd:\tW.Dir:\t%Rain:\n";
	while (my $row = $sql->fetchrow_hashref) {
		print "$row->{'forecast_date'}\t$row->{'temperature'}\t$row->{'wind_speed'}\t$row->{'wind_direction'}\t$row->{'rain_chance'}\n";
	}
	$sql->finish();
}

###################################
# Scrape + Additional Subroutines #
###################################
sub ua_call_to_json {
	my $url = shift;
	my $ua = LWP::UserAgent->new;
	my $response = $ua->get($url);
	return decode_json($response->decoded_content);
}

sub nws_forecast {
	eval {
		#Use the coordinates to get a URL that leads to the forecast URL
		my $forecast_url = ua_call_to_json("https://api.weather.gov/points/$latitude,$longitude/")->{'properties'}->{'forecastHourly'};

		my $json = ua_call_to_json($forecast_url);
		my $periods = $json->{'properties'}->{'periods'};

		clear_existing_staging_data('NWS');
		foreach my $period (@$periods) {
			my %weather_data_point = (
				origin => 'NWS',
				latitude => $latitude,
				longitude => $longitude,
				date => $period->{'startTime'},
				temperature => $period->{'temperature'},
				wind_speed => $period->{'windSpeed'},
				wind_direction => $period->{'windDirection'},
				rain_chance => ($period->{'probabilityOfPrecipitation'}->{'value'} || 0),
			);
			insert_staging_data_point(\%weather_data_point);
		}
	};
	if ($@) {
		print "Error scraping NWS: $@\n";
	}
}

sub meteo_forecast {
	eval {
		my $json = ua_call_to_json("https://api.open-meteo.com/v1/forecast?latitude=$latitude&longitude=$longitude&hourly=temperature_2m,precipitation_probability,wind_speed_10m,wind_direction_10m&temperature_unit=fahrenheit&wind_speed_unit=mph")->{'hourly'};

		#Meteo outputs the values separately and in sequential order
		my @times = @{$json->{'time'}};
		my @temperatures = @{$json->{'temperature_2m'}};
		my @wind_speeds = @{$json->{'wind_speed_10m'}};
		my @wind_directions = @{$json->{'wind_direction_10m'}};
		my @rain_chances = @{$json->{'precipitation_probability'}};

		clear_existing_staging_data('Meteo');
		for my $i (0 .. (scalar @times - 1)) {
			my %weather_data_point = (
				origin => 'Meteo',
				latitude => $latitude,
				longitude => $longitude,
				date => $times[$i],
				temperature => $temperatures[$i],
				wind_speed => $wind_speeds[$i],
				wind_direction => $wind_directions[$i],
				rain_chance => $rain_chances[$i],
			);
			insert_staging_data_point(\%weather_data_point);
			#print "$times[$i]  Temp: $temperatures[$i]  W. Speed: $wind_speeds[$i]  W. Dirs: $wind_directions[$i]  Rain: $rain_chances[$i]\n";
		}
	};
	if ($@) {
		print "Error scraping Meteo: $@\n";
	}
}

sub vc_forecast {
	eval {
		my $api_key = $config{'keys'}{'vc_key'};
	
		my $url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/$latitude,$longitude?unitGroup=us&include=hours&key=$api_key&contentType=csv";
		my $ua = LWP::UserAgent->new;
		my $response = $ua->get($url);
		if ($response->is_success) {
			my @header;
			my $csv_content;
			my $csv = Text::CSV->new({ binary => 1, auto_diag => 1 });
			open my $fh, '<', \$response->decoded_content or die "Error reading: $!\n";
			#put in data structure for my sanity...
			while (my $row = $csv->getline($fh)) {
				if (not @header) {
					@header = @{$row}; #first row is the header
				} else {
					push  @{$csv_content}, { map { $header[$_] => $row->[$_] } 0..$#header };
				}
			}

			#...now parse through data
			clear_existing_staging_data('VC');
			for my $i (0 .. (scalar @{$csv_content} - 1)) {
				my %weather_data_point = (
					origin => 'VC',
					latitude => $latitude,
					longitude => $longitude,
					date => @{$csv_content}[$i]->{'datetime'},
					temperature => @{$csv_content}[$i]->{'temp'},
					wind_speed => @{$csv_content}[$i]->{'windspeed'},
					wind_direction => @{$csv_content}[$i]->{'winddir'},
					rain_chance => @{$csv_content}[$i]->{'precipprob'},
				);
				insert_staging_data_point(\%weather_data_point);
			}
		} else {
			print "Failed to get VC data: " . $response->status_line;
		}
	};
	if ($@) {
		print "Error scraping VC: $@\n";
	}
}

sub clear_existing_staging_data {
	my $origin = shift;

	my $sql = $dbh->prepare("DELETE FROM staging_forecasts WHERE source=? AND latitude=? AND longitude=?;");
	$sql->execute($origin, $latitude, $longitude);
	$sql->finish();
}

sub insert_staging_data_point {
	my $weather_data_point = shift;

	my $sql = $dbh->prepare("INSERT INTO staging_forecasts (source, latitude, longitude, weather_date, temperature, wind_speed, wind_direction, rain_chance) VALUES (?,?,?,?,?,?,?,?);");
	$sql->execute(
		$weather_data_point->{'origin'},
		$weather_data_point->{'latitude'},
		$weather_data_point->{'longitude'},
		$weather_data_point->{'date'},
		$weather_data_point->{'temperature'},
		normalize_wind_speed($weather_data_point->{'wind_speed'}),
		normalize_wind_direction($weather_data_point->{'wind_direction'}),
		normalize_rain_chance($weather_data_point->{'rain_chance'})
	);
	$sql->finish();
}

sub aggregate_staging_data {
	#Update existing forecast records, only if there are same number of sources or more
	my $sql = $dbh->prepare("UPDATE forecasts AS f SET sources=sf.sources, temperature=sf.avg_temperature, wind_speed=sf.avg_wind_speed, wind_direction=sf.avg_wind_direction, rain_chance=sf.avg_rain_chance FROM (SELECT latitude, longitude, weather_date, COUNT(staging_id) AS sources, AVG(temperature) AS avg_temperature, AVG(wind_speed) AS avg_wind_speed, AVG(wind_direction) AS avg_wind_direction, AVG(rain_chance) AS avg_rain_chance FROM staging_forecasts sf WHERE latitude=? AND longitude=? GROUP BY 1,2,3) AS sf WHERE f.latitude=sf.latitude AND f.longitude=sf.longitude AND f.forecast_date=sf.weather_date AND sf.sources >= f.sources;");
	$sql->execute($latitude, $longitude);
	$sql->finish();

	#Create a new record in the database, only if there isn't already one
	$sql = $dbh->prepare("INSERT INTO forecasts (latitude, longitude, forecast_date, sources, temperature, wind_speed, wind_direction, rain_chance) SELECT latitude, longitude, weather_date, COUNT(staging_id) AS sources, AVG(temperature) AS avg_temperature, AVG(wind_speed) AS avg_wind_speed, AVG(wind_direction) AS avg_wind_direction, AVG(rain_chance) AS avg_rain_chance FROM staging_forecasts sf WHERE latitude=? AND longitude=? AND NOT EXISTS (SELECT 1 FROM forecasts WHERE latitude=? AND longitude=? AND forecast_date=sf.weather_date) GROUP BY 1,2,3;");
	$sql->execute($latitude, $longitude, $latitude, $longitude);
	$sql->finish();
}

sub normalize_wind_speed {
	my $wind_speed = shift;
	$wind_speed =~ s/mph//g; #Get rid of "mph" unit
	$wind_speed =~ s/ //g; #Get rid of spaces
	$wind_speed =~ s/(\d+)to(\d+)/($1+$2)\/2/eg; #Replace "XtoY" with the average of the two
	return $wind_speed;
}
sub normalize_rain_chance {
	my $rain_chance = shift;
	if ($rain_chance > 1) { #Not in decimal form
		$rain_chance = $rain_chance / 100;
	}
	return $rain_chance;
}
sub normalize_wind_direction {
	my $direction = shift;
	my %cardinal_degrees = (
		'N'   => 0,
		'NNE' => 22.5,
		'NE'  => 45,
		'ENE' => 67.5,
		'E'   => 90,
		'ESE' => 112.5,
		'SE'  => 135,
		'SSE' => 157.5,
		'S'   => 180,
		'SSW' => 202.5,
		'SW'  => 225,
		'WSW' => 247.5,
		'W'   => 270,
		'WNW' => 292.5,
		'NW'  => 315,
		'NNW' => 337.5
	);
	if (exists $cardinal_degrees{$direction}) {
		return $cardinal_degrees{$direction};
	} else {
		return $direction;
	}
}


#End database connection
$dbh->commit();
$dbh->disconnect();
