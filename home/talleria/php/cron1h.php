<?php
/**
* Script que hace las predicciones de 24H, se ejecuta cada 1H.
**/

include_once("include.inc.php");

$timeIni = microtime(TRUE);
$memIni  =  memory_get_usage(TRUE);

$rows = $db->getObjects("select layerName,city from layers where periodicy = 60;");

foreach($rows as $row){

	$city = $row->city;
	$layer = $row->layerName;

	echo "\n\n<b>############# $city - $layer  ".gmdate($time)."</b>\n";
	
	checkingDirectories($city,$layer);
	checkingSql($city,$layer);

	showMessage('r');
	switch ($city) {
		case 'Moscu':
			$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/24h/Prediction24h.R');
			suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
		 	system($comando);
			break;
		
		case 'Barcelona':
			$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/24h/Prediction24hBarcelona.R');
			suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
		 	system($comando);
			break;
	}

 	//Escribe el resulta en un json
 	showMessage('json');
 	writeJson($city,$layer);

}

//Comprueba si ha habido algún fallo durante la ejecucion de R
checkErrorByFile();

$timeFin = microtime(TRUE);
$memFin = memory_get_usage(TRUE);
$timeCPU = $timeFin - $timeIni;


echo "Tiempo ejecuion: $timeCPU , uso de moemoria ini: $memIni ,fin: $memFin, pico de memoria: ".memory_get_peak_usage(TRUE);


?>