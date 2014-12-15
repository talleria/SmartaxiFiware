<?php

function checkingDirectories($city,$layer){
	global $cityDirectoryPath;

	$folders = array();
	$folders[] = $cityDirectoryPath; 
	$folders[] = $cityDirectoryPath."/tmp";
	
	// Apuntamos al directorio de la ciudad
	$baseDir= $cityDirectoryPath."/".$city;
	$folders[] = $baseDir;
	
	// Apuntamos al directorio de la capa
	$baseDir = $baseDir."/".$layer;
	$folders[] = $baseDir;


	switch($layer){
		case "Map":
			$folders[] = $baseDir."/Previous";
			$folders[] = $baseDir."/Aggregates";
			$folders[] = $baseDir."/Variables";
			$folders[] = $baseDir."/Results";
			$folders[] = $baseDir."/NETs";
		break;

		case "Kmean":
			$folders[] = $baseDir."/Previous";
			$folders[] = $baseDir."/Corrected_IniWaitingTime";
			$folders[] = $baseDir."/Aggregates";
			$folders[] = $baseDir."/Variables";
			$folders[] = $baseDir."/Results";
			$folders[] = $baseDir."/NETs";		
		break;

		case "Map20":
			$folders[] = $baseDir."/Results";
			$folders[] = $baseDir."/NETs";
		break;

		case "24H":
			$folders[] = $baseDir."/Results";
			$folders[] = $baseDir."/NETs";
		break;		
		case "Money":
			$folders[] = $baseDir."/Previous";
			$folders[] = $baseDir."/Aggregates";
			$folders[] = $baseDir."/Results";
			$folders[] = $baseDir."/NETs";
		break;

	}
	makeDirectories($folders);

}

function makeDirectories($folders){	
	
	foreach ($folders as $folder){

		if(!file_exists($folder)){
			mkdir($folder);		
			echo "$folder ---> OK \n";
		}
	}
	echo "\n";		
}

function makeQueryByCity($file,$table,$search="_CITY_"){	
	$gestor = fopen($file, "r");
	
	$contenido = fread($gestor, filesize($file));
	$contenido = str_replace($search,$table,$contenido);

	fclose($gestor);

	return $contenido;
}


function checkingSql($city,$layer){
		switch($layer){
		case "Map":
			$sqlFiles = array("aggregates.sql","areas.sql","picks.sql","points.sql","predictions.sql","variables.sql","aggregatesExtendsTotal.sql",
				"log.sql");
			makeSql($city,$layer,$sqlFiles);
			filVariablesTable($city,$layer);
		break;

		case "Map20":
			$sqlFiles = array("points.sql","predictions.sql");
			makeSql($city,$layer,$sqlFiles);
		break;

		case "Kmean":
			$sqlFiles = array("aggregates.sql","areas.sql","predictions.sql","variables.sql");
			makeSql($city,$layer,$sqlFiles);
			filVariablesTable($city,$layer);
		break;

		case "24H":
			$sqlFiles = array("predictions24H.sql");
			makeSql($city,$layer,$sqlFiles);
		break;		

		case "Money":
			$sqlFiles = array("aggregatesMoney.sql","areas.sql","picks.sql","points.sql","predictions.sql");
			makeSql($city,$layer,$sqlFiles);
		break;

	}
}

function makeSql($city,$layer,$sqlFiles){
	global $db,$phpDirectory;
	$table = $city."-".$layer."-";

	foreach ($sqlFiles as $file){		
		$filePath = "$phpDirectory/sql/".$file;		
		$query = makeQueryByCity($filePath,$table);
		$db->Execute($query);
	}	
}

function filVariablesTable($city,$layer){

	global $db,$format;
	$query = "select count(t) as count from `$city-$layer-Variables`";
	$numRows = $db->getObjects($query);
	
	if($numRows[0]->count == 0){
		
		$query = "select min(area_number) as min,max(area_number) as max from `$city-$layer-Areas`";
		$result = $db->getObjects($query);
		
		$query = "select min(dtime) as minimo from `$city-$layer-Aggregates`";
		$minTime = $db->getObjects($query);
		$minTime = $minTime[0]->minimo;
		echo $minTime."\n";


		if($minTime == null){
			$time = gmdate($format,(((int)(time()/300))*300 - 300));
		}
		else{
			$time = strtotime($minTime." UTC");
			//Hago que los minutos sean multiplo de 5 y le quito 5 minutos
			$time = gmdate($format, (((int)($time/300))*300 - 300)); 
		}
		echo $time."\n";

		$query = "insert into `$city-$layer-Variables` values";

		$variables = "0";
		for($i = 0;$i < 1152;$i++)
			$variables.=",0";

		for($i = $result[0]-> min; $i <= $result[0]-> max; $i++ ){
			$query .= "($i,'$time',\"$variables \"),";	
		}
		if($i>1){
			$query = substr($query, 0,-1);
			// echo $query;
			$db->Execute($query);	
		}
	}
}

//TODO revisar con fede
function checkingTrigger($city,$layer){
	global $db,$phpDirectory;

	$nombre = $city."".$layer;

	//trigger no tiene if not exist por eso hacemos la comprobacion si existe
	$r = $db->getObjects("SELECT TRIGGER_NAME as name FROM information_schema.triggers
		WHERE TRIGGER_SCHEMA = 'smartaxi' AND TRIGGER_NAME = 'triggerPuntos".$nombre."'");
	
	if(count($r) == 0){		
		$query = makeQueryByCity("$phpDirectory/sql/triggerPuntosCiudad.sql",$city."-".$layer."-");
		$query = str_replace("_NAME_",$nombre,$query);
		// echo"$query\n";

		$db->Execute($query);
		echo "triggerPuntos".$city." ---> OK\n\n";
	}
}

function checkErrorByFile(){
	global $errorDirectory,$formatFitch,$logDirectory;

	$fileErrors = array();
	$fileErrors[] = array("err"=>"phpErr5m.log","read"=>"std5m.log");
	$fileErrors[] = array("err"=>"phpErr30m.log","read"=>"std30m.log");
	$fileErrors[] = array("err"=>"phpErr1h.log","read"=>"std1h.log");
	$fileErrors[] = array("err"=>"phpErr3h.log","read"=>"std3h.log");
	$fileErrors[] = array("err"=>"phpErr24h.log","read"=>"std24h.log");

	foreach ($fileErrors as $fileError) {
		$fError  = $fileError['err'];

		$exten = split("\.",$fError ,2);
		$fError = $logDirectory."/".$fError;
		$newErrorFile = $errorDirectory."/".basename($fError,".".$exten[1])."_".date($formatFitch).".log";

		if(file_exists($fError)){
			$error = file_get_contents($fError);

			if(strlen(trim($error)) > 1){			
				echo "$fError\n";
				echo "$newErrorFile\n";
			
				$fTrace = $logDirectory."/".$fileError['read'];
				$body = file_get_contents($fTrace);
				$body = formatError($body,$error);
			
				$result = sendEmail($body , $fileError['read']);

				if ($result){
					if(!file_exists($errorDirectory))
						mkdir($errorDirectory);

					$file = fopen($newErrorFile, "a+");
					$resultWrite = fwrite($file, $body);
					fclose($file);

					if($resultWrite !== FALSE)
						unlink($fError);
				}
			}
		}
	}
}


function formatError($trace,$error){
	$errores = split("\n", $error);
	foreach($errores as $err){
		$trace = str_replace($err,"<font color='red'><b>$err</b></font>", $trace);
	}

	$msgError = showMessage('error');
	$newTrace = $msgError."".$error."\n\n\n".$trace;
	return $newTrace;
}

function isLocked(){
    # If lock file exists, check if stale.  If exists and is not stale, return TRUE
	 # Else, create lock file and return FALSE.
	global $logDirectory;

	if( file_exists( LOCK_FILE )){
		$difTime = time() - filectime(LOCK_FILE);	
		$error = "El archivo ".LOCK_FILE." hace ".difTime2String($difTime)." que se ha creado \n";

        # check if it's stale
		$lockingPID = trim( file_get_contents( LOCK_FILE ) );

       # Get all active PIDs.
		$pids = explode( "\n", trim( `ps -e | awk '{print $1}'` ) );

        # If PID is still active, return true
		if( in_array( $lockingPID, $pids )){			
			if($difTime > 599){

				$command = "kill -9 $lockingPID";
				exec($command,$output,$result);

				if($result == 0)
					unlink( LOCK_FILE );
				
				$error.="El pid del proceso es: ".$lockingPID." y se ha borrado el archivo .lock eliminad el proceso enganchado \n"; 
				$fTrace = $logDirectory."/std5m.log";
				$body = file_get_contents($fTrace);
				$body = $error."".$body;
				$result = sendEmail($body , "Cron 5m LOCK");

			}
			return true;
		}
        # Lock-file is stale, so kill it.  Then move on to re-creating it.
		echo "Removing stale lock file.\n";
		unlink( LOCK_FILE );
	}
	file_put_contents( LOCK_FILE, getmypid() . "\n" );
	return false;
}


function getTables(){
	global $db;
	$tables = array();

	$rows = $db->getArrays("show tables");	
	foreach($rows as $row)
		$tables[]=$row["Tables_in_smartaxi"];

	return $tables;
}


function checkIntervalTimesServer(){
	global $db;

	$tables = getTables();
	$structTbls = array();
	$structTbls[] = array("table"=>"_CITY_-_LAYER_-Aggregates","nameColTime"=>"dtime");
	$structTbls[] = array("table"=>"_CITY_-_LAYER_-Predictions","nameColTime"=>"datetime_ini"); 

	$rows = $db->getObjects("select layerName,city from layers where periodicy = 5 order by city asc");

	foreach ($rows as $row){
		$city = $row->city;
		$layer = $row->layerName;

		//Recorre las tablas
		foreach($structTbls as $structTbl){

			$nameTable = $structTbl["table"];
			$nameTable = str_replace("_LAYER_",$layer,$nameTable);
			$nameTable = str_replace("_CITY_",$city,$nameTable);
			
			// Si existe la tabla mira los intervalos
			if(in_array($nameTable,$tables)){
				echo "\n\n<b>############# $nameTable </b> \n";
				checkIntervalsTimes($nameTable,$structTbl["nameColTime"]);
			}

		}
	}
}

function checkIntervalsTimes($table,$nameColTime,$typeTimeStr = TRUE){
	global $db,$format;
	$query = "select distinct $nameColTime as time from `$table` order by $nameColTime asc";
	echo $query."\n";

	if(!$typeTimeStr){
		$db->execute("SET time_zone = '+0:00'");
		$query = "select distinct from_unixtime($nameColTime) as time from `$table` order by $nameColTime asc";
	}

	$times = $db->getObjects($query);
	$first = TRUE;
	
	//Recorre las filas de 
	foreach($times as $rowTime){
		$rowTime = $rowTime->time;
		$cTime = strtotime($rowTime."UTC");
		$cTime =  $cTime - $cTime%300;

		if($first){
			$timeRef = $cTime + 300;
			$first = False;
			continue;
		}

		// echo "$timeRef == $cTime\n";
		for(; $timeRef < $cTime; $timeRef+=300) 
			writeErrorPhp("Falta el intervalo: ".gmdate($format,$timeRef)." --> $timeRef en la tabla: $table");
		
		$timeRef += 300;
	}
}
?>