<?php

function open_db($host,$user,$pass,$db){
	return odbc_connect("Driver={SQL Server Native Client 11.0};Server=$host;Database=$db;", $user, $pass);
}

function close_db($con){
	odbc_close($con);
}

function _get_name_only($name){
	$a_name = explode("\\",$name);
	return array_pop($a_name);
}

function _get_db_info($con,$db_id){
	$sql  = "SELECT a.db_name_source AS name,a.db_name_target AS name_target,b.host,b.username,b.pass,c.last_lsn,a.dir_source,a.dir_target 
			 FROM [database] a 
			 LEFT JOIN [instance] b on a.instance_id = b.id
			 LEFT JOIN [database_stat] c on a.id = c.database_id
			 WHERE a.id=$db_id";
	$res = odbc_exec($con,$sql);
	$row = odbc_fetch_array($res);
	
	
	
	return $row;
}

function gen_database_stat($con,$db_id){
	
	$row = _get_db_info($con,$db_id);
	
	$db_name = $row['name_target'];
	
		
	$sql = "SELECT TOP 1 b.type, b.first_lsn, b.last_lsn, b.checkpoint_lsn, b.database_backup_lsn
			FROM msdb..restorehistory a
			INNER JOIN msdb..backupset b ON a.backup_set_id = b.backup_set_id
			WHERE a.destination_database_name = '".$db_name."'";
	
	$res = odbc_exec($con,$sql);
	$row = odbc_fetch_array($res);
	
	$lsn = $row['last_lsn'];
	
	$sql = "SELECT count(*) as JML
			FROM database_stat
			WHERE database_id=$db_id";
	$res = odbc_exec($con,$sql);
	$row = odbc_fetch_array($res);
	if($row['JML'] > 0){
		$sql = "UPDATE database_stat 
				SET update_at=getdate()
					, last_lsn=$lsn
				WHERE database_id=$db_id";
		
	}else{
		$sql = "INSERT INTO database_stat(database_id,last_lsn,update_at)
				VALUES($db_id,$lsn,getdate())";
	}
	odbc_exec($con,$sql);
	
	
}

function gen_apply_log_backup($con,$db_id){
	
	$row = _get_db_info($con,$db_id);
	
	$db_name  = $row['name'];
	$db_host  = $row['host'];
	$db_user  = $row['username'];
	$db_pass  = $row['pass'];
	$last_lsn = $row['last_lsn'];
	
	$con_remote = odbc_connect("Driver={SQL Server Native Client 11.0};Server=$db_host;Database=$db_name;", $db_user, $db_pass);
	
	$backup = array();
	
	$sql = "select  s.last_lsn,
					f.physical_device_name as name
			from    msdb..backupset s join msdb..backupmediafamily f
					on s.media_set_id = f.media_set_id
			where   s.database_name = '$db_name' 
					AND type = 'L' 
					AND last_lsn > $last_lsn
			order by s.backup_finish_date ASC";
					
	$res = odbc_exec($con_remote,$sql);
	while($row = odbc_fetch_array($res)){
		$backup[] = $row;
	}
	
	odbc_close($con_remote);
	
	// MENGINSERT KE TABEL APPLY LOG BACKUP!
	
	foreach($backup as $r){
		$sql = "SELECT count(*) as JML
				FROM apply_log_backup
				WHERE database_id=$db_id
				AND backup_name='"._get_name_only($r['name'])."'";
		$res = odbc_exec($con,$sql);
		$row = odbc_fetch_array($res);
		
		
	
		if($row['JML'] == 0){
			$sql = "INSERT INTO apply_log_backup(database_id,backup_name,update_at,log_status_id,last_lsn)
					VALUES($db_id,'"._get_name_only($r['name'])."',getdate(),1,".$r['last_lsn'].")";
			odbc_exec($con,$sql);
		}
	}
}

function do_apply_log_backup($con,$db_id){
	
	
	$db = _get_db_info($con,$db_id);
	
	$sql = "SELECT top 1 id,backup_name
			FROM apply_log_backup
			WHERE database_id=$db_id
			AND log_status_id IN (1,4)
			ORDER BY update_at ASC ";
	$res = odbc_exec($con,$sql);
	
	if(odbc_num_rows($res) > 0){
		$row = odbc_fetch_array($res);

		$id = $row['id'];
		$backup_name = $row['backup_name'];

		$sql = "UPDATE apply_log_backup
				SET log_status_id = 2
				WHERE id=$id";
		odbc_exec($con,$sql);

		_do_copy_backup_file($db['dir_source'].$backup_name,$db['dir_target'].$backup_name);

		_do_restore_log($con,$db_id,$id,$db['dir_target'],$backup_name,$db['name_target']);

		_do_clean_backup_file($db['dir_target'].$backup_name);
		
	}	
}

function _do_copy_backup_file($source,$dest){
	copy($source,$dest);
}

function _do_clean_backup_file($dest){
	unlink($dest);
}

function _do_restore_log($con,$db_id,$log_id,$dir_target,$backup_name,$dbname){
	
	
	$sql = "RESTORE LOG [$dbname] FROM  DISK = N'".$dir_target.$backup_name."' WITH  FILE = 1,  NORECOVERY,  NOUNLOAD,  STATS = 10";
	
	$res = odbc_exec($con,$sql);
	
	if($res){
		$sql = "UPDATE apply_log_backup
			SET log_status_id = 3
			WHERE id=$log_id";
		
	}else{
		$sql = "UPDATE apply_log_backup
			SET log_status_id = 4
			, error_message = '".odbc_errormsg($con)."'
			WHERE id=$id";
		
	}
	
	odbc_exec($con,$sql);
}

function sys_lock($con,$db_id){
	
	$key = time();
	
	$sql = "INSERT INTO sys_lock(database_id,key_lock,update_at) values($db_id,$key,getdate())";
	
	$res = odbc_exec($con,$sql);
	
	$sql = "SELECT key_lock FROM sys_lock WHERE database_id = $db_id";
	
	$res = odbc_exec($con,$sql);
	
	$row = odbc_fetch_array($res);
	
	if($row['key_lock'] == $key) return true;
	
	return false;
}

function sys_lock_release($con,$db_id){
	
	$sql = "DELETE FROM sys_lock WHERE database_id = $db_id";
	
	odbc_exec($con,$sql);
	
}