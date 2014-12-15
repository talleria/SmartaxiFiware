<?php
chdir(dirname(__FILE__));
ob_start();
	
if ($_POST["data"] != "" ) {

	//Obtenemos y limpiamos los cifs del campo de texto
	$datos = $_REQUEST["data"];
	
	//Eliminamos los saltos de linea
	$datos = str_replace("\r\n",";",$datos);
	
	// Convertimos a array
	$filas = explode(";",$datos);
	
	$i = 0;

	// Procesamos las filas
	$general = array();
	
	foreach($filas as $fila){
		$vector = explode(",", $fila);
		
		if(count($vector) == 5) {
			$result = geoTaxitronic2DecimalDegrees(array('lat'=> $vector[2],'long' => $vector[3]));
			$vector[1] = time();
			$vector[2] = $result['lat'];
			$vector[3] = $result['long'];
			$vector[4] = (int)$vector[4];
			$general[] = "'".implode("','", $vector)."'";
			$i++;
		}
	}
	
	$result = file_put_contents("rawData.csv",implode("\n",$filas));
	$result = file_put_contents("data.csv",implode("\n",$general));

	
	if($result)
		echo "OK-".$i;
	else
		echo "ERROR";
} 
else {
	echo "ERROR";
}


$out = ob_get_contents();
file_put_contents("post.txt",$out);

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

	// echo "/* $lat, $long */\n";

	return array('lat' => $lat2,'long'=>$long2);
}


?>