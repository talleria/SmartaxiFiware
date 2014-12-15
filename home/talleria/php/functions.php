<?php

// Conexion a bbdd
function conexion(){
	global $_base,$_servidor,$_usuario,$_contrasena,$_conexion;
	if($_conexion == NULL){
		$_conexion = new dbmysql($_base,$_servidor,$_usuario,$_contrasena);
		$_conexion->Execute("SET NAMES 'utf8'");
	}
	return $_conexion;
}

function commit(){
	global $db;
	$db->Execute("commit");
}

function insertLog($function,$numRows,$time){
	global $db,$format;
	$now = gmdate($format);
	$query = "insert into log values('$function','numero de iteraciones: $numRows','$now',$time)";
	$db->Execute($query);
}

function getParameters(){
	global $db;
	$parameters = $db->getArrays("select Name,Value from Parametros");
	$final =  array();
	
	foreach ($parameters as $parameter) 
		$final[$parameter["Name"]] = $parameter["Value"] ;
	
	return $final;
}

function insertPicks($city,$layer){
	$t1 = microtime(true);
	global $db,$format;

	$i = 0;
	$pick = 1;
	$cabecera = true;	
	$maxInsert = 3000;
	$table = "$city-$layer-Picks";
	
	$time = getTimeReferencePicks($table);
	$date_ini = gmdate($format,$time);
	$date_fin = gmdate($format);
	
	$files = selectFiles($city,$layer,$time);
	
	foreach ($files as $nameFile) {

		echo "Abriendo: $nameFile \n";	
		$file = fopen($nameFile,'r');
		$data = array();

		while (($bufer = fgets($file)) !== false) {
			$bufer = trim($bufer);
			$array = explode(",", $bufer);

			//Saca las posiciones de la cabecera
			if($cabecera){
				// echo "$bufer\n";
				$arrayC = array();
				$arrayC["lat"] = array_search('oldLat',$array);
				$arrayC["long"] = array_search('oldLong',$array);
				$arrayC["areanum"] = array_search('areanum',$array);
				$arrayC["dtime"] = array_search('dtime',$array);
				$arrayC["statusf"] = array_search('statusf',$array);

				$cabecera = false;
				continue;
			}

			//Hace inserts del numero Maximo de inserts
			if($i >= $maxInsert){
				$query = "insert into `$table` (lat,lon,area_number,date,date_fin,date_ini) values(".implode("),(", $data).")";
				echo "insertadas: $i filas\n";
				$db->Execute($query);
				$data = array();
				$i = 0;
			}
			if($array[$arrayC["dtime"]] >=  $time && $array[$arrayC["statusf"]] == $pick && $array[$arrayC["areanum"]] != 9999 && $array[$arrayC["lat"]] != 0 && $array[$arrayC["long"]] != 0){
				$data[]= $array[$arrayC["lat"]].",".$array[$arrayC["long"]].",".$array[$arrayC["areanum"]].",'".gmdate($format,$array[$arrayC["dtime"]])."','".$date_fin."','".$date_ini."'";
				$i++;
			}
		}

		fclose($file);
		
		if(count($data)>0){
			$query = "insert into `$table` (lat,lon,area_number,date,date_fin,date_ini) values(".implode("),(", $data).")";
			$db->Execute($query);			
			echo "insertadas: $i filas\n";
			$i = 0;
		}

	}

	if($city == "Barcelona"){
		$timeDel = time() - 2*24*3600;
	}
	else{
		$timeDel = time() - 5*3600;
	}

	$query = "delete from `$table` where date < '".gmdate($format,$timeDel)."'"; 
	echo $query."\n";
	$db->Execute($query);
	commit();

	$time = microtime(true) - $t1;
	echo "Tiempo:  $time \n";
}

function selectFiles($city,$layer,$timeReference){
	global $formatFitch,$cityDirectoryPath;
	$files = array();
	$path = "$cityDirectoryPath/$city/$layer/Previous/Previous-";

	$time = time();
	$files[] = $path."".gmdate($formatFitch,$time).".csv";

	for($i = $time - 24*3600; $i >= $timeReference; $i -= (24*3600)){
		$file2 = gmdate($formatFitch,$i);
		$files[] = $path."".$file2.".csv";
	}
	
	return $files;
}

function getTimeReferencePicks($table){
	global $db,$format;

	if($table == "Barcelona-Map-Picks"){
		$nowLeast5h = time() - 2*24*3600;
	}
	else{
		$nowLeast5h = time() - 5*3600;
	}

	$query = "select max(date) as maximo from `$table`";
	$maxPickTime = $db->getObjects($query);
	$maxPickTime = $maxPickTime[0]->maximo;
	if($maxPickTime == null){
		echo "select: $nowLeast5h \n";
		return $nowLeast5h;
	}

	$maxPickTime = strtotime($maxPickTime." UTC");	
	$result = max($nowLeast5h,$maxPickTime);
	echo "select: $result, maxtimeTable:$maxPickTime ".gmdate($format,$maxPickTime). " minimoTiempo: $nowLeast5h ahora: ".time()."\n";
	return $result;
}


function sendEmail($body,$subject,$to = array("")){
	require_once 'class.phpmailer.php';
	global $format;

	$subject = str_replace("std", "Cron ", $subject);
	$subject = str_replace(".log","", $subject);

	$mail = new phpmailer(); 
	$mail->Username = ""; 		// Cuenta de e-mail
	$mail->Password = ""; 		// Password
	$mail->SMTPAuth = true; // True para que verifique autentificación de la cuenta o de lo contrario False
	$mail->SMTPDebug = true;

	$mail->Mailer = "smtp";
	$mail->Host = "";		// Insert SMTP Host
	$mail->Port= 465;

	$mail->IsHTML(true);
	$mail->From = "";	// Cuenta de e-mail
	$mail->FromName = gethostname();
	$mail->Subject = $subject." ".gethostname()." ".gmdate($format);
	$mail->Body = nl2br(str_replace("\r\n","\n",$body));

	foreach ($to as $key => $value) {
		$mail->AddAddress($value);
	}

	$mail->Timeout=30;

	$result = $mail->Send();
	if($result === false)
	{
		$mail->Subject = "MAILSERVER: ".$subject." ".gethostname();
		$mail->IsMail();
		return $mail->Send();
	}

	return $result;
}

function difTime2String($difTime){
	$horas =(int)($difTime / 3600);
	$minutos = (int)(($difTime % 3600) / 60);	
	$segundos = (int)($difTime % 60);
	return "$horas:$minutos:$segundos";
}

function showOutput(&$output){
	foreach($output as $out){
		echo "$out\n";
	}
	
	$output = array();
}

function showMessage($key){
	$messages = array();
	$messages['python'] = "SCRIPTS PYTHON";
	$messages['r'] = "SCRIPTS R";
	$messages['json'] = "ESCRIBIENDO DATA IN JSON";
	$messages['s3'] = "MOVIENDO A S3";
	$messages['error'] = "ERROR";
	$messages['pick'] = "INSERTANDO LOS PICKS";
	$messages['csvs'] = "REALIZANDO MANTENIMIENTO DE LA BD";
	$messages['iniwait'] = "SCRIPTS PYTHON GROUP BY INTERVARINIWAITINGTIME";
	$messages['sdb'] = "LEYENDO DATOS DE SDB";

	$message ="\n<b>## ".$messages[$key]."</b>\n";
	echo $message;
	return $message;
}
?>
