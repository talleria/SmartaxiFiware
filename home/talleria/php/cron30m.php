<?php 
/**
* Script que inserta los picks cada 30 minutos.
**/

include_once("include.inc.php");

$timeIni = microtime(TRUE);
$memIni  =  memory_get_usage(TRUE);

$rows = $db->getObjects("select layerName,city from layers where layer = 'Map'");

foreach($rows as $row){
	$city = $row->city;
	$layer = $row->layerName;

	echo "\n\n<b>############# $city - $layer  ".gmdate($time)."</b>\n";

	checkingDirectories($city,$layer);
	checkingSql($city,$layer);

	showMessage('pick');
	insertPicks($city,$layer);
}

//Comprueba que no haya habido algún error en php
checkErrorByFile();

$timeFin = microtime(TRUE);
$memFin = memory_get_usage(TRUE);
$timeCPU = $timeFin - $timeIni;


echo "Tiempo ejecuion: $timeCPU , uso de moemoria ini: $memIni ,fin: $memFin, pico de memoria: ".memory_get_peak_usage(TRUE);

?>