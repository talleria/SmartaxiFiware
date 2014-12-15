<?php
/**
* Script que hace los aggregados,variables,predicciones de 5 minutos y escribe los resultados.
**/
include_once("include.inc.php");

$timeIni = microtime(TRUE);
$memIni  =  memory_get_usage(TRUE);

define( 'LOCK_FILE', "$rootDirectory/".basename( $argv[0], ".php" ).".lock" );

if(isLocked()){
	writeErrorPhp("Error se estaba ejecutando este mismo proceso.");
	die();
}


$rows = $db->getObjects("select layerName,city from layers where periodicy = 5 order by city asc");
foreach($rows as $row){
	$city = $row->city;
	$layer = $row->layerName;
	$startPHP = microtime(true);

	echo "\n\n<b>############# $city - $layer  ".gmdate($time)."</b>\n";

 	//Comprobaciones
	checkingDirectories($city,$layer);
	checkingSql($city,$layer); 	

	//LEE LOS DATOS DE SDB
	sdb2CBr($city,300);

	switch($layer){
		case "Map":
			//Comprobacion del trigger
			showMessage('sdb');
			checkingTrigger($city,$layer);

			//Genera los datos pretratados
			showMessage('python');
			$comando = "python $pythonDirectory/prev_csv_generator.py $city $layer";
			system($comando);

			//Genera los aggregados y los inserta en la BD
			$comando = "python $pythonDirectory/rawCSVtoAggregate.py $city $layer";
			system($comando);

			showMessage('r');
			switch ($city) {
				case 'Moscu':
				 	//Lanza el proc de MySQL que genera las variables a partir de los aggregados
					$query="call ProcVariablesAreasCiudadText('$city','$layer')";
					$db->Execute($query);
					
					// Lanza los el script de predicción de R
					$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/5m/PredictionCiudad.R');suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla ";
					system($comando);
				break;

				case 'Barcelona':
					// Lanza los el script de predicción de R
					$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/5m/PredictionBarcelona.R');suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
					system($comando);
				break; 
			}


			//write Points table in json
			showMessage('json');
			writeJson($city,$layer);

			//Libreria de Moscu
			if($city == "Moscu"){
				$file = $cityDirectoryPath."/".$city."/".$layer."/".$layer.".json";

				if(file_exists($file)){
					$sourceFile = makeZip($file);
					$dest = $tmpDirectory."/Moscu.json.zip";

					$rCopyJsonZip = copy($sourceFile,$dest);
					if($rCopyJsonZip) 
						echo "copiado el archivo $sourceFile ---> $dest    OK \n";
					else 
						writeErrorPhp("copiado el archivo $sourceFile ---> $dest    ERROR");
				}
			}
		break;

		case "Kmean":

 			//Genera los datos pretratados
			showMessage('python');
			$comando = "python $pythonDirectory/prev_csv_generator.py $city $layer";
			system($comando);

			//Genera los aggregados y los inserta en la BD
			$comando = "python $pythonDirectory/rawCSVtoAggregate.py $city $layer";
			system($comando);

			showMessage('r');
			switch ($city) {
				case 'Moscu':
				 	//Lanza el proc de MySQL que genera las variables a partir de los aggregados
					$query="call ProcVariablesAreasCiudadText('$city','$layer')";
					$db->Execute($query);

					// Lanza los el script de predicción de R
					$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/5m/PredictionKmean.R');suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
					system($comando);
				break;

				case 'Barcelona':
					// Lanza los el script de predicción de R
					$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/5m/PredictionBarcelona.R');suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
					system($comando);		
				break; 
			}

			//write Points table in json 	
			showMessage('json');
			writeJson($city,$layer);
		break;

		case "Money":
 			//Genera los datos pretratados
			showMessage('python');
			$comando = "python $pythonDirectory/prev_csv_generator.py $city $layer";
			system($comando);

			//Genera los aggregados y los inserta en la BD
			$comando = "python $pythonDirectory/rawCSVtoAggregate.py $city $layer";
			system($comando);

			showMessage('r');
			$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/5m/PredictionBarcelona.R');suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
			system($comando);


			//write Points table in json
			showMessage('json');
			writeJson($city,$layer);
		break;
	
		case "Map20":
			// Lanza los el script de predicción de R
			showMessage('r');
			$comando="echo \"setwd('$rDirectory');source('Scripts_R/crones/5m/PredictionBarcelona.R');suppressWarnings(PredictAreas('$city','$layer'))\"|/usr/bin/R --slave --vanilla";
			system($comando);

			//write Points table in json
			showMessage('json');
			writeJson($city,$layer);
		break;
	}

$dif = microtime(true) - $startPHP;
insertLog("Cron 5m PHP","$layer-$city",$dif); 

}

// Sube todos los archivos del directorio tmp a S3
showMessage('s3');
moveAllToS3();
unlink(LOCK_FILE);

//Comprueba que no haya habido algún error en php
checkErrorByFile();

$timeFin = microtime(TRUE);
$memFin = memory_get_usage(TRUE);
$timeCPU = $timeFin - $timeIni;


echo "Tiempo ejecuion: $timeCPU , uso de moemoria ini: $memIni ,fin: $memFin, pico de memoria: ".memory_get_peak_usage(TRUE);


function sdb2CBr(){

	$rows = array();
	$sort = array("idUser","time","lat","lon","status");
	$data = querySDB("Barcelona",300);

	foreach ($data['rows'] as $key => $value) {
		$row  = "";
		foreach ($sort as $key)
			$row .= $value[$key].",";

		$rows[] = substr($row,0,-1);
	}

	$data = implode(";",$rows);
	echo "DATA: $data \n";

	$fields = array();
	$fields["user"] = "";	// Insert user
	$fields["key"] = "";	// Insert password
	$fields["data"] = $data;


	$ch = curl_init();
	curl_setopt($ch,CURLOPT_URL, "130.206.80.46" );
	curl_setopt($ch,CURLOPT_POST, 1);
	curl_setopt($ch,CURLOPT_RETURNTRANSFER, 1 );
	curl_setopt($ch,CURLOPT_POSTFIELDS, $fields);

	//execute post
	$result = curl_exec($ch);

	//close connection
	curl_close($ch);


	echo "Result: \n";
	var_dump($result);
}

?>