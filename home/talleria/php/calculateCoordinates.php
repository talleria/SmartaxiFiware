<?php
include_once("/var/www/phpserver/scriptsPhp/include.inc.php");

function convertDegMinSec2DecimalDegrees($coordinates){
	$lat = $coordinates['lat'];
	$long = $coordinates['long'];

	$newLat = $lat[0] + $lat[1]/60 + $lat[2]/3600;
	$newLong = $long[0] + $long[1]/60 + $long[2]/3600;

	return array('lat'=>$newLat,'long'=>$newLong);

}

function convertDecimalDegrees2DegMinSec($coordinates){
	$lat = $coordinates['lat'];
	$long = $coordinates['long'];

	$grado = (int)$lat;
	$temp = ($lat - $grado)*60;
	$min = (int)$temp;
	$sec = ($temp - $min)*60;
	$newLat = array($grado,$min,$sec);

	$grado = (int)$long;
	$temp = ($long - $grado)*60;
	$min = (int)$temp;
	$sec = ($temp - $min)*60;
	$newLong = array($grado,$min,$sec);

	return array('lat'=>$newLat,'long'=>$newLong);
}


// diferencia en lat brng = 0
// diferencia en long = 90
function getGeoPoint2Distance($geoPoint, $brng, $distance){

	$radiusEarth = 6371;
	$geoPointRad['lat'] = deg2rad($geoPoint['lat']);
	$geoPointRad['long'] = deg2rad($geoPoint['long']);
	$brng = deg2rad($brng);

	$lat2 = asin( sin($geoPointRad['lat'])* cos($distance/$radiusEarth) + 
		cos($geoPointRad['lat'])*sin($distance/$radiusEarth) * cos($brng));

	$long2 = $geoPointRad['long'] + atan2(sin($brng)*sin($distance/$radiusEarth)*cos($geoPointRad['lat']), 
		cos($distance/$radiusEarth)-sin($geoPointRad['lat'])*sin($lat2));

	return array('lat'=>rad2deg($lat2),'long'=>rad2deg($long2));
}

function decimalsDegrees2Areas($nameCsv,$nameTable){
	global $db;

	$areaNum = 1;
	$file = fopen($nameCsv,'r');
	$query = "insert into `$nameTable` (area_number,maxlat,minlat,maxlong,minlong) values";
	while(($bufer = fgets($file))!==false){
		
		$area = string2Areas($bufer);
		$query .= "(".$areaNum.",".$area['maxlat'].",".$area['minlat'].",".$area['maxlong'].",".$area['minlong']."),";
		$areaNum++;
	}

	//Inserta los datos en la bd
	$query = substr($query,0,-1);
	$db->Execute($query);

	echo "$query \n";
}

function string2Areas($coordenadas){
	$coord = split(",",$coordenadas);
	$geoPoint = array('lat'=>(double)$coord[1],'long'=>(double)$coord[0]);

	// Comprobaciones de los datos
	var_dump($geoPoint);
	var_dump(convertDecimalDegrees2DegMinSec($geoPoint));

	//Dif lat
	$newLat = getGeoPoint2Distance($geoPoint, 0, 0.1);

	//Dif long
	$newLong = getGeoPoint2Distance($geoPoint, 90, 0.1);


	$difLat = abs($newLat['lat'] - $geoPoint['lat']);
	$difLong = abs($newLong['long'] - $geoPoint['long']);

	return array('maxlat'=>($geoPoint['lat'] + $difLat),
		'minlat'=>($geoPoint['lat'] - $difLat),
		'maxlong'=>($geoPoint['long'] + $difLong),
		'minlong'=>($geoPoint['long'] - $difLong));
}

function geoTaxitronic2DecimalDegrees($geoPoint){

	$lat = (float)substr($geoPoint['lat'], 0,-1);
	$long = (float)substr($geoPoint['long'], 0,-1);

	$lat2 = (int)($lat / 100);
	$long2 = (int)($long /100);

	$lat2 += ($lat%100) / 60;
	$long2 += ($long%100)/ 60;

	$lat2 += ($lat - floor($lat))/60;
	$long2 += ($long - floor($long))/60;

	$typeLat = substr($geoPoint['lat'], strlen($geoPoint['lat'])-1);
	$typeLong = substr($geoPoint['long'], strlen($geoPoint['long'])-1);

	if(strtolower($typeLat)=="s")
		$lat2*=-1;

	if(strtolower($typeLong)=="w")
		$long2*=-1;

	echo "/* $lat, $long */\n";

	return array('lat' => $lat2,'long'=>$long2);

}

function csvTaxitronic2DecimalDegrees($nameFile,$nameTable){
	global $db;
	$cont = 0;
	$file = fopen($nameFile,'r');

	$geoPoints = array();
	while(($bufer = fgets($file)) !== false){

		/******************Puntos en un array*****************************/

		$arrayCoor = split(",",$bufer);
		$coor = array('lat' => $arrayCoor[2],'long'=>$arrayCoor[3]);
		$geoPoints[] = geoTaxitronic2DecimalDegrees($coor);
		echo $geoPoints['lat']." --> ".$geoPoints['long'];
		$cont++;

	}

	return $geoPoints;
}



function csvTxtronic2csv($source,$dest){
	global $db;
	$cont = 0;
	$fSource = fopen($source,'r');
	$fDest = fopen($dest,'w');

	$geoPoints = array();
	while(($bufer = fgets($fSource))!==false){
		
		$arrayCoor = split(",",$bufer);
		$coor = array('lat' => $arrayCoor[2],'long'=>$arrayCoor[3]);
		$geoPoints = geoTaxitronic2DecimalDegrees($coor);

		$arrayCoor[2] = $geoPoints['lat'];
		$arrayCoor[3] = $geoPoints['long'];

		if($geoPoints['long'] > 0 && $geoPoints['long'] <10 ){
			$rowString = implode(",",$arrayCoor);
			$result = @fputs($fDest,$rowString);
		}

		$cont++;
	}

	fclose($fSource);
	fclose($fDest);

	return $geoPoints;
}
function convertFiles(){
	
	$dir = "/var/www/phpserver/Madrid/";

	$files = scandir($dir);

	foreach ($files as $file) {

		if(strlen($file) < 3) continue;

		$date = split("_",$file);

		$source = $dir."".$file;
		$dest = $dir."rawData_".$date[1];
		echo "$source \n$dest \n";
		csvTxtronic2csv($source,$dest);
	}
}

function getObjectsDB($query){
	global $db;

	return $db->getObjects($query);
}	

function readCsvTxTronic($nameFile){
	global $db;
	$cont = 0;

	$file = fopen($nameFile,'r');

	$geoPoints = array();
	while(($bufer = fgets($file)) !== false){

		$arrayCoor = split(",",$bufer);
		$geoPoints[] = array('idTaxi'=>$arrayCoor[1],'time' => $arrayCoor[2],'lat' => $arrayCoor[3],'long' => $arrayCoor[4],'status' => $arrayCoor[5]);
		$cont++;

	}

	fclose($file);

	return $geoPoints;
}

function insertCsvBD($pathDir){
	global $db;
	
	$files = scandir($pathDir);

	foreach ($files as $file) {

		if(strlen($file) < 3) continue;
		$file = $pathDir."".$file;

		$rows = readCsvTxTronic($file);

		echo "rows: ".count($rows)."\n";

		$cont = 0;
		$query = "insert into taxitronic2 (idTaxi,time,lat,lon,status) values";
		foreach ($rows as $row){
			if($cont > 3000){
				$query = substr($query,0,-1);
				$db->Execute($query);
				$query = "insert into taxitronic2 (idTaxi,time,lat,lon,status) values";
				echo "$cont";
				$cont = 0;
			}
			$query.="(".implode(",",$row)."),";
			$cont++;
		}

		$query = substr($query,0,-1);
		echo $query."\n";

		echo "$cont";
		$db->Execute($query);
	}
}

function processingDataRaw($nameFile){
	global $db,$format;

	$prev = array();
	$file = fopen($nameFile, "w");
	$file2 = fopen($nameFile."0","w");

	$query = "select distinct idTaxi from taxitronic where lon > 0 order by idTaxi";
	$taxis = $db->getArrays($query);
	

	foreach($taxis as $taxi){
		$taxi = $taxi['idTaxi'];

		$query = "select * from taxitronic where lon > 0 and idTaxi = $taxi order by time asc";
		echo "$query \n";
		$rows = $db->getArrays($query);
		

		$first = TRUE;
		$timeFin = $rows[count($rows)-1]['time'];
		$tBase = 0;
		$tFin = 0;

		$prev = array();

		$datoPrevio = $rows[0];

		foreach($rows as $row){

			if($first){
				$prev = $row;
				$tBase = $row['time'] + 300;
				$first = FALSE;

				continue;
			}
			
			for(;$tBase <= $timeFin+300; $tBase+=300)
			{
				echo "$tBase ----> ".($tBase+300)." ".date($format,$tBase)."\n";

				if(($tBase <= $row['time']) && ($row['time'] < ($tBase+300))){
					echo $row['time']."  ";

					$id = $row['idTaxi'];	
					$statusF = 0;

					if($prev == null){
						if(((int)$row['status']) == 0) $statusF = 3;
						else $statusF = 1;

						echo "dentro def: $statusF \n";
					}
					else{
						if((int)($prev['status'] == 0) && ((int)$row['status'] == 0)) $statusF = 3;
						elseif((int)($prev['status'] == 0) && ((int)$row['status'] == 1)) $statusF = 1;
						elseif((int)($prev['status'] == 1) && ((int)$row['status'] == 0)) $statusF = 2;
						elseif((int)($prev['status'] == 1) && ((int)$row['status'] == 1)) $statusF = 0;
						echo $prev['status']."-->".$row['status']." = $statusF \n";
					}

					if($statusF == 1){
						$nTaxi = $row;
						$nTaxi['status'] = $statusF;
						$rowString = implode(",", $nTaxi)."\n";
						fputs($file,$rowString);
						echo "Pick: \n";
					}
					else{
						$nTaxi = $row;
						$nTaxi['status'] = $statusF;
						$rowString = implode(",", $nTaxi)."\n";
						fputs($file2,$rowString);
						echo "All: \n";
					}

					$prev = $row;
					$tBase += 300;
					break;

				}
				else{
					$prev = null;
				}

			}
			if($row["time"]-$datoPrevio["time"] < 300)
				echo "diferencia de tiempo : ".($row["time"]-$datoPrevio["time"])."\n";
			$datoPrevio = $row;
		}
		echo "$id \n";
	}


	fclose($file);
	fclose($file2);
}


function processingDataRawPrev($nameFile){
	global $db,$format;

	$prev = array();
	$file = fopen($nameFile, "w");
	$file2 = fopen($nameFile."0","w");

	$query = "select distinct idTaxi from taxitronic where lon > 0 order by idTaxi";
	$taxis = $db->getArrays($query);
	

	foreach($taxis as $taxi){
		$taxi = $taxi['idTaxi'];

		$query = "select * from taxitronic where lon > 0 and idTaxi = $taxi order by time asc";
		echo "$query \n";
		$rows = $db->getArrays($query);
		

		$first = TRUE;
		$timeFin = $rows[count($rows)-1]['time'];
		$tBase = 0;
		$tFin = 0;

		$prev = array();

		$datoPrevio = $rows[0];
		ob_start();
		foreach($rows as $row){

			if($first){
				$prev = $row;
				$tBase = $row['time'] + 300;
				$first = FALSE;

				continue;
			}
			
			for(;$tBase <= $timeFin+300; $tBase+=300){

				if(($tBase <= $row['time']) && ($row['time'] < ($tBase+300))){
					echo $row['time']."  ";
					$id = $row['idTaxi'];	
					$statusF = 0;

					if($prev == null){
						if(((int)$row['status']) == 0) $statusF = 3;
						else $statusF = 1;

						echo "dentro def: $statusF \n";
					}
					else{
						if((int)($prev['status'] == 0) && ((int)$row['status'] == 0)) $statusF = 3;
						elseif((int)($prev['status'] == 0) && ((int)$row['status'] == 1)) $statusF = 1;
						elseif((int)($prev['status'] == 1) && ((int)$row['status'] == 0)) $statusF = 2;
						elseif((int)($prev['status'] == 1) && ((int)$row['status'] == 1)) $statusF = 0;
						echo $prev['status']."-->".$row['status']." = $statusF \n";
					}

					if($statusF == 1){
						$nTaxi = $prev;
						$nTaxi['status'] = $statusF;
						// $nTaxi['lat'] = ($row['lat'] + $prev['lat']) / 2;
						// $nTaxi['lon'] = ($row['lon'] + $prev['lon']) / 2;
						$rowString = implode(",", $nTaxi)."\n";
						fputs($file,$rowString);
						echo "Pick: \n";
					}else{
						$nTaxi = $prev;
						$nTaxi['status'] = $statusF;
						$rowString = implode(",", $nTaxi)."\n";
						fputs($file2,$rowString);
						echo "All: \n";
					}

					$prev = $row;
					$tBase += 300;
					break;

				}
				else{
					$prev = null;
				}

			}
			if($row["time"]-$datoPrevio["time"] < 300)
				echo "diferencia de tiempo : ".($row["time"]-$datoPrevio["time"])."\n";
			$datoPrevio = $row;
		}
		ob_get_clean();
		echo "$id \n";

	}


	fclose($file);
	fclose($file2);
}

decimalsDegrees2Areas("/home/josep/Descargas/paradasbarcelona49_c.csv","Barcelona-Kmean-Areas");

?>
