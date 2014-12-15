<?php
/**
* Script que hace el mantenimiento de la base de datos
**/

include_once("include.inc.php");

$timeIni = microtime(TRUE);
$memIni  =  memory_get_usage(TRUE);


showMessage('csvs');
$rows = $db->getObjects("select layerName,city from layers;");

foreach($rows as $row){
	$city = $row->city;
	$layer = $row->layerName;

	echo "\n\n<b>############# $city - $layer  ".gmdate($time)."</b>\n";

	checkingDirectories($city,$layer);
	checkingSql($city,$layer);

	writeCsvs($city,$layer);

}

$timeFin = microtime(TRUE);
$memFin = memory_get_usage(TRUE);
$timeCPU = $timeFin - $timeIni;


echo "Tiempo ejecuion: $timeCPU , uso de moemoria ini: $memIni ,fin: $memFin, pico de memoria: ".memory_get_peak_usage(TRUE);


?>