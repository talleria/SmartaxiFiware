<?php
class dbmysql{
  
	/* variables de conexion */
	var $BaseDatos;
	var $Servidor;
	var $Usuario;
	var $Clave;
	/* Identificador de Conexion*/
	var $Conexion_ID=0;
	var $Consulta_ID=0;
	/* numero de error y texto de error*/
	var $Errno=0;
	var $Error="";

	/*Método constructor: cada vez que creamos una varible DB_mysql se ejecuta este metodo */
	function dbmysql($bd="",$host="",$user="",$pass=""){
	    $this->BaseDatos=$bd;
	    $this->Servidor=$host;
	    $this->Usuario=$user;
	    $this->Clave=$pass;
	    $this->conectar($bd,$host,$user,$pass);
	}

	/*CONEXION A LA BASE DE DATOS*/
	function conectar($bd="",$host="",$user="",$pass=""){
		if($bd !="")$this->BaseDatos=$bd;
		if($host !="")$this->Servidor=$host;
		if($user !="")$this->Usuario=$user;
		if($pass !="")$this->Clave=$pass;
		//conectamos al server
		$this->Conexion_ID=mysql_connect($this->Servidor,$this->Usuario,$this->Clave);
		if(!$this->Conexion_ID){
			$this->Error="Ha fallado la conexión al servidor";
			return 0;
		}
		//seleccionamos la base de datos
		if(!@mysql_select_db($this->BaseDatos,$this->Conexion_ID)){
			$this->Error="Imposible abrir ".$this->BaseDatos;
			return 0;
		}
		//si hemos tenido exito devuelve el id de la conexion, sino 0
		return $this->Conexion_ID;
	}
	  
	/*EJECUTAR UNA CONSULTA*/
	function Execute($sql=""){
		global $querys;
		if ($sql==""){
			$this->Error="No se especifico un query";
			return 0;
		}
		// echo $sql."\n";
		$querys++;

		if(!mysql_ping($this->Conexion_ID)){
			$this->CerrarConexion();
			$this->Conexion_ID = $this->conectar();
		}

		//ejecutamos la consulta
		$this->Consulta_ID=mysql_query($sql,$this->Conexion_ID);
		$this->ultimo_id=mysql_insert_id($this->Conexion_ID);
		if (!$this->Consulta_ID){
			$this->Errno=mysql_errno();
			$this->Error=mysql_error();
			$this->Error .= "\n$sql\n";
			// echo "<br><font color='red'><b>ERROR:</b></font> $this->Error<br>";
			writeErrorPhp("<br><font color='red'><b>ERROR:</b></font> $this->Error<br>");
			return 0;
		}

		//si tuvimos exito devuelvo el id de la consulta
		return $this;
	}

	//RETORNA EL NUMERO DE CAMPOS
	function numCampos(){
		return mysql_num_fields($this->Consulta_ID);
	}

	//RETORNA EL NUMERO DE REGISTROS
	function numRegistros(){
		return mysql_num_rows($this->Consulta_ID);
	}
	 
	//RETORNA EL NUMERO DE REGISTROS
	function nombreCampo($numCampo){
		return mysql_field_name($this->Consulta_ID,$numCampo);
	}
	 
	function FetchRow(){
		return mysql_fetch_array($this->Consulta_ID, MYSQL_ASSOC);
	}
	 

	 /*MUESTRA EL RESULTADO DE UNA CONSULTA*/
	function verConsulta(){
		echo "<table border=1><tr>\n";
		// mostramos los nombres de los campos
		for ($i=0; $i<$this->numCampos();$i++){
			echo "<td><b>".$this->nombreCampo($i)."</b></td>\n";
		}
		echo "</tr>\n";
		while ($row=mysql_fetch_row($this->Consulta_ID)){
			echo "<tr>\n";
			for ($i=0; $i<$this->numCampos();$i++){
				echo "<td>".$row[$i]."</td>\n";
			}
			echo "</tr>\n";
		}
		echo "</table>\n";
	}

	 /* DEVUELVE OBJETOS POR CADA CONSULTA */
	function getObjects($sql=""){
		if($sql == "") return array();
		$sql = $this->Prepare($sql);
		$result = $this->Execute($sql);
		$coll = array();
		while($row = $result->FetchRow()){
			$obj = new stdClass;
			foreach($row as $key => $value){
				$obj->$key = null;
				$obj->$key = $value;
			}
			$coll[]=$obj;
		}
		return $coll;
	}
	
	 /* DEVUELVE UNA MATRIZ POR CADA CONSULTA */
	function getArrays($sql=""){
		if($sql == "") return array();
		$sql = $this->Prepare($sql);
		$result = $this->Execute($sql);
		$coll = array();
		while($row = $result->FetchRow())
		{		
			$coll[] = $row;
		}
		return $coll;
	}

	//RETORNA LAS DBS DE UNA CONEXION
	function dbs(){
		return mysql_list_dbs($this->Conexion_ID);
	}
	
	//RETORNA LAS TABLAS DE UNA DB
	function tables(){
		return mysql_list_tables($this->BaseDatos,$this->Conexion_ID);
	}
	 
	function Insert_ID(){
		return $this->ultimo_id; 
	}

	function Prepare($sql){
		return $sql;
	}

	function ErrorNo(){
		return $this->Error;
	}
	
	function CerrarConexion(){
		mysql_close($this->Conexion_ID);
		$this->Conexion_ID = null;
	}

} //end de la clase


?>
