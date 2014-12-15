<?php
/**
* Script que ejecuta los scripts de Learn y sincroniza los datos con softlayer cada dia.
**/
include_once("include.inc.php");

$timeIni = microtime(TRUE);
$memIni  =  memory_get_usage(TRUE);

$rows = $db->getObjects("select layerName,city from layers where periodicy != -1 order by city,layerName asc");
foreach($rows as $row){

	$city = $row->city;
	$layer = $row->layerName;

	echo "\n\n<b>############# $city - $layer  ".gmdate($time)."</b>\n";
	
	$startPHP = microtime(true);
	checkingDirectories($city,$layer);
	
	if($layer == "Kmean" && $city == "Barcelona"){
		showMessage('iniwait');
		$comando = "python $pythonDirectory/IniWaitTime.py $city $layer";
		system($comando);
	}

	showMessage('r');
	switch ($city) {
		case 'Moscu':
			if($layer == "Map"){
				$command="echo \"setwd('$rDirectory');source('Scripts_R/crones/24h/LearnCiudad.R');
				suppressWarnings(LearnAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
				system($command);
			}
			elseif($layer == "24H"){
				$command="echo \"setwd('$rDirectory');source('Scripts_R/crones/24h/Learn24h.R');
				suppressWarnings(Learn24h('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
				system($command);
			}
			elseif($layer == "Kmean"){}
			break;

		case 'Barcelona':
			$command="echo \"setwd('$rDirectory');source('Scripts_R/crones/24h/LearnBarcelona.R');
			suppressWarnings(LearnAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
			system($command);
			break;
	}

	$dif = microtime(true) - $startPHP;
 	insertLog("Cron 24H PHP",$layer."-".$city,$dif); 
}


//Cambia los permisos
$command = "chown -R t4lleriatest ".$cityDirectoryPath."/";
system($command);

$command = "chgrp -R t4lleriatest ".$cityDirectoryPath."/";
system($command);

//Hace el mantenimiento de la tabla LOG
csvLog();

//Elimina los jsones de s3 viejos
deleteOldJsonsInS3();


//Comprueba que no haya habido algún error en php
checkErrorByFile();


$timeFin = microtime(TRUE);
$memFin = memory_get_usage(TRUE);
$timeCPU = $timeFin - $timeIni;


echo "Tiempo ejecuion: $timeCPU , uso de moemoria ini: $memIni ,fin: $memFin, pico de memoria: ".memory_get_peak_usage(TRUE);

?>
