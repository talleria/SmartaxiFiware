<?php
	//Declaración de variables
	$rootDirectory=dirname(dirname(__FILE__));
	
	$cityDirectoryPath = $rootDirectory."/cities";
	$phpDirectory = $rootDirectory."/php";
	$pythonDirectory = $rootDirectory."/python";
	$rDirectory=$rootDirectory;
	$tmpDirectory = $cityDirectoryPath."/tmp";
	$errorDirectory = $rootDirectory."/Error";
	$logDirectory = $rootDirectory."/log";
	$s3Directory = "/mnt/s3";
	$softLayerDirectory = "/mnt/softlayer/cities";

	$jsonTest = "t";        
    $format = "Y-m-d H:i:s";
    $time ="H:i:s";
    $formatFitch ="Y-m-d";


	// Importamos las clases
	ob_start();
	include_once($phpDirectory."/datos.php");
	include_once($phpDirectory."/class.dbmysql.php");
	include_once($phpDirectory."/functions.php");
	include_once($phpDirectory."/functionsChecking.php");
	include_once($phpDirectory."/functionsIO.php");
	include_once($phpDirectory."/functionsSDB.php");
	include_once($phpDirectory."/herramientas.php");
	ob_end_clean();
	
	
	// $db = conexion();
 //    $parameters = getParameters();
?>