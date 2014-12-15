<?php

function writeCsv($nameFile,$data,$mode="w+",$order=FALSE){

	//Abre el fichero
	$file = fopen($nameFile,$mode);
	$result = 0;
	$cont = 0;	
	
	if($order !== FALSE && count($data) > 0)
		$result = @fputs($file,implode(",",$order)."\n");

	foreach($data as $row){
		$rowString = "";

		if($order !== FALSE){
			foreach ($order as $column)
				$rowString .= $row[$column].",";

			$rowString = substr($rowString, 0,-1)."\n";
		}
		else{
			$rowString = implode(",",$row)."\n";
		}

		$result = @fputs($file,$rowString);
		$cont++;
	}
	fclose($file);

	if($result === FALSE)
		return $result;

	return $cont;
}

function writeCsvFromTable($nameFile,$table,$where=""){
	global $db;

	$query = 'select * from `'.$table.'` '.$where;
	$rows = $db->getArrays($query);
	echo $query."\n";
	
	return writeCsv($nameFile,$rows,$mode="a+");
}

function csvAggregates($city,$layer){

	global $db,$format,$cityDirectoryPath,$formatFitch,$parameters;

	$start=microtime(true); 

	$tablaNameAgg = $city."-".$layer."-Aggregates";
	$tablaNameVar = $city."-".$layer."-Variables";

	$arrayAggregates = array();
	
	switch($layer){
		case "Map":
			$arrayAggregates[] = "Aggregates";
			$arrayAggregates[] = "AggregatesExtendsTotal";

			$timeSecurity = $parameters['t_security'] * 60; //Minutes
			$delayTime = $parameters['t_aggregates'] * 60 * 60; //Hours

			if($city == "Moscu"){
				$query = "select max(dtime) as maximo from `".$tablaNameAgg."`";
				$lastTimeAgregado = $db->getObjects($query);
				$lastTimeAgregado = $lastTimeAgregado[0]->maximo;
				$lastTimeAgregado = strtotime($lastTimeAgregado." UTC") - $timeSecurity ;

				$query = "select max(t) as maximo from `".$tablaNameVar."`";
				$lastTimeVariables = $db->getObjects($query);
				$lastTimeVariables = $lastTimeVariables[0]->maximo;
				$lastTimeVariables = strtotime($lastTimeVariables." UTC") - $timeSecurity ;

				
				$finishTime = Time() - $delayTime;

				$maxTime = min($lastTimeAgregado,$lastTimeVariables,$finishTime);
				$maxTime = gmdate($format,$maxTime);

				echo("Tiempo actual: ".gmdate($format,$finishTime).
					" Tiempo maximo agregado: ".gmdate($format,$lastTimeAgregado).
					" Tiempo maximo variables: ".gmdate($format,$lastTimeVariables).
					" Selected:  $maxTime \n");
			}
			else{
				$maxTime = gmdate($format,Time()-$delayTime);
				echo " Selected:  $maxTime \n";
			}
		break;
	
		default:
			$arrayAggregates[] = "Aggregates";
			$delayTime = $parameters['t_aggregates'] * 60 * 60; //Hours
			$maxTime = gmdate($format,Time() - $delayTime);
		break;
	}

	foreach ($arrayAggregates as $typeAggregates) {
		
		$path = $cityDirectoryPath."/".$city."/".$layer."/Aggregates/".strtolower($typeAggregates)."_".date($formatFitch).".csv";	
		$tablaNameAgg = $city."-".$layer."-".$typeAggregates;

		$where = "where dtime < '$maxTime' order by dtime asc";
		$numRows = writeCsvFromTable($path,$tablaNameAgg,$where);

		if($numRows === FALSE){
			$time = microtime(true) - $start; 
			insertLog("csv".$typeAggregates."-".$city."-".$layer,-1,$time);
			echo "\n\nERROR al intentara escribir en el archivo $path \n\n";
			return;
		}

	    //Borra las filas guardadas en el csv
		$query = "delete from `$tablaNameAgg` where dtime < '$maxTime'";    
		$db->Execute($query);

		$time = microtime(true) - $start; 
		insertLog("csv".$typeAggregates."".$city."-".$layer,$numRows,$time);
		commit();
	}
	
}

function csvVariables($city,$layer)
{
	global $db,$format,$cityDirectoryPath,$formatFitch,$parameters;
	$start=microtime(true); 	

	$path = $cityDirectoryPath."/".$city."/".$layer."/Variables/variables_".date($formatFitch).".csv";
	$tablaNameVar = $city."-".$layer."-Variables";

	//Pasamos los tiempos a segundos para trabajar con ellos después
	$timeSecurity = $parameters['t_security'] * 60; //Minutes
	$delayTime = $parameters['t_variables'] * 60 * 60; //Hours



	$query = "select max(t) as maximo from `".$tablaNameVar."`";
	$lastTimeVariables = $db->getObjects($query);
	$lastTimeVariables = $lastTimeVariables[0]->maximo;
	$lastTimeVariables = strtotime($lastTimeVariables." UTC") - $timeSecurity ;

	
	$finishTime = Time() - $delayTime;
	$maxTime = min($lastTimeVariables,$finishTime);
	$maxTime = gmdate($format,$maxTime);



	echo("Tiempo actual: ".gmdate($format,$finishTime).
		" Tiempo maximo variables: ".gmdate($format,$lastTimeVariables).
		" Selected:  $maxTime \n");
	$where = "where t < '$maxTime' order by t asc";

	$numRows = writeCsvFromTable($path,$tablaNameVar,$where);	

	if($numRows === FALSE){
		$time = microtime(true) - $start; 
		insertLog("csvVariables".$city."-".$layer,-1,$time);
		echo "\n\nERROR al intentara escribir en el archivo $path \n\n";
		return;
	}
	//Borra las filas guardadas en el csv
	$query = "delete from `$tablaNameVar` where t < '$maxTime'";    
	$db->Execute($query);

	$time = microtime(true) - $start; 
	insertLog("csvVariables".$city."-".$layer,$numRows,$time); 
	commit();   
}

function csvPredictions($city,$layer,$nameColTime="datetime_fin"){
	global $db,$format,$cityDirectoryPath,$parameters,$formatFitch;
	$start=microtime(true);
	
	$path = $cityDirectoryPath."/".$city."/".$layer."/Results/predictions_".date($formatFitch).".csv";
	$tabla = $city."-".$layer."-Predictions";
	$timeSecurity = $parameters['t_security'];

	//Pasamos los tiempos a segundos para trabajar con ellos después
	$timeSecurity = 48 * 60 * 60;
	
	$query = "select max( $nameColTime ) as maximo from `".$tabla."`";
	$lastTimePredictions = $db->getObjects($query);
	$lastTimePredictions = $lastTimePredictions[0]->maximo;
	$lastTimePredictions = strtotime($lastTimePredictions) - $timeSecurity ;
	$maxTime = date($format,$lastTimePredictions);


	$where = "where $nameColTime < '$maxTime' order by $nameColTime asc";
	$numRows = writeCsvFromTable($path,$tabla,$where);	

	if($numRows === FALSE){
		$time = microtime(true) - $start; 
		insertLog("CsvPrediccion".$city."-".$layer,-1,$time);
		echo "\n\nERROR al intentara escribir en el archivo $path \n\n";
		return;
	}

    //Borra las filas guardadas en el csv
	$query = "delete from `$tabla` where $nameColTime < '$maxTime'";    
	$db->Execute($query);

	$time = microtime(true) - $start; 
	insertLog("CsvPrediccion".$city."-".$layer,$numRows,$time);    
	commit();
}

function csvLog()
{
	global $db,$format,$cityDirectoryPath,$parameters,$formatFitch;
	$start=microtime(true);

	$path = $cityDirectoryPath."/Logs";
	$nameFile = $path."/logs_".date($formatFitch).".csv";

	if(!file_exists($path))
		mkdir($path);

	$tabla = "log";
	$timeSecurity = $parameters['t_security'];

	//Pasamos los tiempos a segundos para trabajar con ellos después
	$timeSecurity = $timeSecurity * 60;
	
	$query = "select max(time) as maximo from ".$tabla;
	$lastTime = $db->getObjects($query);
	$lastTime = $lastTime[0]->maximo;
	
	$lastTime = strtotime($lastTime) - $timeSecurity ;
	$maxTime = gmdate($format,$lastTime);

	$where = "where time < '$maxTime' order by funcion asc,time asc";
	$numRows = writeCsvFromTable($nameFile,$tabla,$where);	

	if($numRows === FALSE){
		$time = microtime(true) - $start; 
		insertLog("CsvLog",-1,$time);
		echo "\n\nERROR al intentara escribir en el archivo $path \n\n";
		return;
	}
    //Borra las filas guardadas en el csv
	$query = "delete from `$tabla` where time < '$maxTime' ";    
	$db->Execute($query);
	
	$time = microtime(true) - $start; 
	insertLog("CsvLog",$numRows,$time);    
	commit();
}

function writeCsvs($city,$layer){
	switch($layer){
		case "Map":
		case "Kmean":
			csvAggregates($city,$layer);
			csvVariables($city,$layer);

			csvPredictions($city,$layer);
			
		break;
		case "24H":
			csvPredictions($city,$layer,"datetime");
		break;
		case "Money":
			csvAggregates($city,$layer);
			csvPredictions($city,$layer);
		break;
		case "Map20":
			csvPredictions($city,$layer);
		break;
	}
}

function data2Json($query){
	global $db;

	$rows = $db->getArrays($query);

	$arrayPoints =  array();

	foreach($rows as $row)	{
		$point = implode(",",$row);
		array_push($arrayPoints,$point);
	}

	$json = implode(";",$arrayPoints);
	return $json;
}

function writeData2Json($city,$layer,$data){
	global $cityDirectoryPath,$tmpDirectory,$phpDirectory;

	$nameLayerJson = $cityDirectoryPath."/".$city."/".$layer."/$layer.json";
	$nameGenericJson = $cityDirectoryPath."/".$city."/".$city.".json";

	if($layer == "Map" && $city == "Moscu"){
		$data = $data.";".file_get_contents("$phpDirectory/sql/fakeData.txt");
	}

	$layer = strtolower($layer);
	$obj = new stdClass();
	$time = time();
	$obj->time = $time;
	$obj->$layer = 	$data;


	$jsonEncode = json_encode($obj);

	//Abre el fichero
	$file = fopen($nameLayerJson,"w");
	
	//Escribe el fichero
	fwrite($file, $jsonEncode);

 	//Cierra el fichero	
	fclose($file);

	//Lee el json generico de la ciudad y si no existe lo crea con los datos
	$genericJson = file_get_contents($nameGenericJson);
	
	if($genericJson !== FALSE){
		$json = json_decode($genericJson);
		$json->time = $time;
		$json->$layer = $data;

		$jsonEncode = json_encode($json);
	}

	//Abre el fichero
	$file = fopen($nameGenericJson,"w");

	//Escribe el fichero
	fwrite($file, $jsonEncode);

 	//Cierra el fichero	
	fclose($file);

	//Comprime el json
	$zipFile = makeZip($nameGenericJson);


	$dest = $tmpDirectory."/".$city."_temp.json.zip";

	if(file_exists($dest))unlink($dest);

	$rCopyJsonZip = copy($zipFile,$dest);	
	if($rCopyJsonZip)
		echo "copiado el archivo $zipFile ---> $dest OK \n";
	else 
		writeErrorPhp("Error al copiar el archivo $zipFile ---> $dest");
	
}

function makeZip($file){
	if(!file_exists($file)){
		writeErrorPhp("No existe el fichero ".$file);
		return FALSE;	
	}

	$nameFile = explode("/", $file);
	$nameFile = $nameFile[count($nameFile)-1];

	$zipFile = $file.".zip";


	$zip = new ZipArchive;

	if ($zip->open($zipFile,ZIPARCHIVE::OVERWRITE) === TRUE){
		$zip->addFile($file,$nameFile);

		$zip -> close();
		echo "\nZip $nameFile ---> OK \n";	
		
		return $zipFile;
	}
	else{
		echo "\nZip $nameFile ---> ERROR \n";
		return FALSE;
	}
}

function moveAllToS3(){
	global $tmpDirectory,$s3Directory,$formatFitch,$jsonTest;

	$files = scandir($tmpDirectory);

	foreach ($files as $file) {

		if(strlen($file) < 3) continue;

		$srcFile = $tmpDirectory."/".$file;

		//MD5
		if($file != "Moscu.json.zip"){
			$city = split("_", $file,2);
			$city = $city[0];
			$nameFile = $city."-Smartaxi-".date($formatFitch);
			$file = md5($nameFile).".json.zip";
			echo "$nameFile \n";
		}
		

		$destFile = $s3Directory."/".$jsonTest."".$file;
		echo $srcFile." ---> ".$destFile;
		
		if(file_exists($destFile)){
			unlink($destFile);
		}
	
		$rCopy = copy($srcFile,$destFile);
		if($rCopy){
			echo "  OK\n";
			unlink($srcFile);
		}
		else{
			echo "  ERROR\n";			
		} 
	}
}

function deleteOldJsonsInS3(){
	global $s3Directory,$formatFitch;

	$timeReference = date($formatFitch,time() - (24 * 60 *60));
	$timeReference = strtotime($timeReference."23:59:59");

	echo $timeReference."\n";

	$files = scandir($s3Directory);
	foreach($files as $file){
		$fullNameFile = $s3Directory."/".$file;
		$writeTime = filemtime($fullNameFile);

		if($writeTime <= $timeReference && strlen($file) > 40 && strlen($file) < 43 ){
			echo "El archivo $file es antiguo y se va a eliminar\n";
			unlink($fullNameFile);
		}
	}
}

function writeJson($city,$layer){
	switch($layer){
		case "Map":
		case "Map20":
		case "Money":
			writePointsInJson($city,$layer);
		break;
		case "24H":
			writeJson24H($city,$layer);
		break;
		case "Kmean":
			writeJsonKmean($city,$layer);	
		break;
	}
}

function writePointsInJson($city,$layer){
	global $db,$format;
	$query = "select lat,lon from `$city-$layer-Points`";
	$json = data2Json($query);

	//Si no hay resultados no se crea el fichero y se avisa
	if(strlen($json) < 20){
		writeErrorPhp("No hay predicciones en $city,$layer para el tiempo: ".gmdate($format));
		return FALSE; 
	}	
	writeData2Json($city,$layer,$json);
	$db->Execute("truncate `$city-$layer-Points`");
}

function writeJson24H($city,$layer){
	global $cityDirectoryPath, $db,$format;

	$table = "$city-$layer-Predictions";
	$timeIni = gmdate($format,time()- 55*60);
	$timeFin = gmdate($format);
	$query = "select predicted_number from `$table` where datetime between '$timeIni' and '$timeFin' order by datetime desc ,hora asc limit 24";
	echo "$query \n";
	$rows = $db->getArrays($query);


	$json24H = array();
	foreach($rows as $row){
		$json24H[] = $row["predicted_number"];	
	}
	
	//Media
	$mean = array_sum($json24H)/count($json24H);
	$min = min($json24H);
	$max = max($json24H);

	$result = max(abs($min-$mean),abs($max-$mean));

	$result = 50/$result;

	$points = array();
	foreach($json24H as $point){
		$points[] = (string)(($point - $mean) * $result);	
	}

	//Si no hay resultados no se crea el fichero y se avisa
	if(count($points) != 24){		
		writeErrorPhp("No hay predicciones en $city,$layer para el intervalo: $timeIni, $timeFin");
		return;
	}
	writeData2Json($city,$layer,$points);

}


function writeJsonKmean($city,$layer){
	global $db,$format;

	if($city == "Moscu"){
		$time = time();
		$time -= $time % 300;
		$timeIni = gmdate($format,$time);
		$timeFin = gmdate($format,$time+299);	
		$query = "select (maxlat+minlat)/2 as lat,(maxlong+minlong)/2 as lon ,predicted_number 
		from  `$city-$layer-Predictions` p left join `$city-$layer-Areas` a on a.area_number=p.area_number 
		where datetime_ini between '$timeIni' and '$timeFin'";
		echo $query."\n";

		$json = data2Json($query,false);

		//Si no hay resultados no se crea el fichero y se avisa
		if(strlen($json) < 20){
			writeErrorPhp("No hay predicciones en $city,$layer para el intervalo: $timeIni, $timeFin");
			return;
		}
		writeData2Json($city,$layer,$json);

	}
	else{
		$time = time();
		$time -= $time % 300;
		$timeIni = gmdate($format,$time);
		$timeFin = gmdate($format,$time+299);
		$timeIniDelay20 = gmdate($format,($time - (20 * 60)));

		$query = "select (maxlat+minlat)/2 as lat,(maxlong+minlong)/2 as lon ,predicted_number as pNumber, p.area_number
		from  `$city-$layer-Predictions` p left join `$city-$layer-Areas` a on a.area_number=p.area_number 
		where p.datetime_ini between '$timeIni' and '$timeFin' and p.area_number in 
		(select areanum from `$city-$layer-Aggregates` where statusf = 1 and meanwait > 0 and dtime > '$timeIniDelay20') ";
		echo "$query \n";
		$predictedList = $db->getObjects($query);
		if(count($predictedList) < 1){
			echo "No hay predicciones en $city,$layer para el intervalo: $timeIni, $timeFin";
			return;
		}

		
		$query = "select sum(aggregate) as suma ,areanum from `$city-$layer-Aggregates` 
		where statusf = 1 and dtime > '$timeIniDelay20' and meanwait > 0
		group by areanum order by suma desc limit 1";
		echo "$query \n";
		$maxSumAggByArea = $db->getObjects($query);
		$areaMaxSumAgg = $maxSumAggByArea[0]->areanum;


		$query = "select predicted_number from `$city-$layer-Predictions` where area_number=$areaMaxSumAgg and datetime_ini";
		$predictedMaxArea = $db->getObjects($query);
		$predictedMaxArea = $predictedMaxArea[0]->predicted_number;


		$query= "select meanwait from `$city-$layer-Aggregates` where areanum = $areaMaxSumAgg and meanwait > 0 order by dtime desc limit 1";
		echo "$query \n";
		$meanwaitMaxArea = $db->getObjects($query);
		$meanwaitMaxArea = $meanwaitMaxArea[0]->meanwait;

		$coeficiente = $predictedMaxArea * $meanwaitMaxArea;
		echo "coeficiente: $coeficiente \n";

		$json = array();
		foreach ($predictedList as $predicted){
			$predicted->pNumber = ceil(($coeficiente / $predicted->pNumber) / 60);
			$json[] = $predicted->lat.",".$predicted->lon.",".$predicted->pNumber;
		}

		$json = implode(";",$json);
		writeData2Json($city,$layer,$json);
	}
}

function writeJsonAreas($city,$layer){
	global $db,$cityDirectoryPath;

	$obj = new stdClass();
	$query  = "select area_number,maxlat,minlat,maxlong,minlong from `$city-$layer-Areas`";
	$lcLayer = strtolower($layer);
	$obj->$lcLayer = data2Json($query,false);
	$jsonEncode = json_encode($obj);

	//Abre el fichero
	$nameFile=$cityDirectoryPath."/".$city."/".$layer."/".$layer."Areas.json";	
	$file = fopen($nameFile,"w");
	
	//Escribe el fichero
	fwrite($file, $jsonEncode);

 	//Cierra el fichero	
	fclose($file);

	makeZip($nameFile);
}

/**
* Funcion que sincroniza los datos del server con los de softlayer
**/
function syncCities(){
	global $db,$cityDirectoryPath,$softLayerDirectory,$formatFitch;
	$syncDirectories = array();
	$syncDirectories[] = "Previous"; 
	$syncDirectories[] = "Aggregates"; 


	$rows = $db->getObjects("select layerName,city from layers  where periodicy = 5;");

	foreach ($rows as $row) {
		$city = $row->city;
		$layer = $row->layerName;

		foreach($syncDirectories as $syncDirectory){

			$srcDir =  $cityDirectoryPath."/".$city."/".$layer."/".$syncDirectory;
			$destDir =  $softLayerDirectory."/".$city."/".$layer."/".$syncDirectory;

			$files = scandir($srcDir);


			//Quita del array de archivos los archivos que no queremos sincronizar, jsons y los de hoy
			$syncFiles = array();
			foreach($files as $file){
				if(FALSE === strpos($file, "json") && FALSE === strpos($file,date($formatFitch))){
					$syncFiles[] = $file;
				}
			}
			// $syncFiles = $files;


			foreach($syncFiles as $file){
				if(strlen($file) < 3) continue;

				$destFile = $destDir."/".$file;

				if(!file_exists($destFile)){
					$srcFile = $srcDir."/".$file;
					$result = copy($srcFile,$destFile);

					if($result) echo "$srcFile --> $destFile\n";
				}
			}			
		}
	}
}

/**
* Funcion que escribe un error en el archivo de errorPhp
**/
function writeErrorPhp($error){
	fwrite(STDERR, $error."\n");
}

function zipGroupFiles($srcDir,$pattern){
	$result = chdir($srcDir);

	$nameZip = "zip/".$pattern.'.zip';
	echo "$pattern --> $nameZip \n";

	$zip = new ZipArchive;
	$zip->open($nameZip, ZipArchive::CREATE);

	foreach (glob($pattern."*") as $file) {
	    $zip->addFile($file);
	    echo $file."\n";
	}
	$zip->close();
}

?>