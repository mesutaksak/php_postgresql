<?php

namespace PhpPostgresql;


class PgConnection
{
    /**
     * @var resource|false|null
     */
    protected $connection;

    /**
     * @var resource|false|null
     */
    protected $transactions;
    
    /**
     * @var bool
     */
    protected $debug;

    /**
     * Executed Queries 
     * @var null|array
     */
    protected $queries;

    /**
     * Fetch Mode Default 0 is Object,  PGSQL_ASSOC = 1, PGSQL_NUM = 2, PGSQL_BOTH = 3
     * @var int
     */
    public $fetchMode = 0;

    /**
     * Places params in query build executable sql string 
     * this is for debug able to see query
     * @param $query
     * @param $params
     * @return mixed
     */
    public function buildSql($query, $params)
    {
        //TODO: should be better
        $query_string = $query;

        foreach($params as $index => $value){
            
            $value_str = is_numeric($value) ? $value : sprintf("'%s'", $value);
            $query_string = str_replace('$' .($index+1), $value_str, $query_string);
        }

        return $query_string;
    }
    
    /**
     * Returns active connection link
     * @return resource|false|null
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     *   $settings['host']
     *   $settings['port']
     *   $settings['dbname']
     *   $settings['user']
     *   $settings['password']
     *   $settings['debug']
     *
     * @param $settings array Connection settings
     */
    public function __construct($settings)
    {
        $connectionString =
             ' host='		. $settings['host']
            .' port='		. $settings['port']
            .' dbname='		. $settings['dbname']
            .' user='		. $settings['user']
            .' password='	. $settings['password']
            .' options=\'--client_encoding=UTF8\'' ;

        $this->connection = pg_connect($connectionString);
        $this->islemler = false;
        $this->debug = isset($settings['debug']) ? $settings['debug'] : false;
        if($this->debug){
            $this->queries = [];
        }
    }

    public function __destruct()
    {
        $this->commit();
        $this->close();
    }

    /**
     * Closes active connection if exists
     * @return bool returns True if closed a open connection , otherwise return false when not closed any connection
     */
    public function close(){
        $closed = false;
        if($this->connection){
            if(is_resource($this->connection) && pg_close($this->connection)){
                $closed = true;
                $this->connection = null;
            }
        }
        return $closed;
    }

    /**
     * Executes statements
     * @param $query
     * @param null $params
     * @return resource
     */
    public function exec($query, $params = null)
    {
        if($this->debug) {
            $this->queries[] = $this->buildSql($query, $params);
        }
        
        if($params === null) {
            
            return pg_query($this->connection, $query);
        }
        
        return pg_query_params($this->connection, $query, $params);
    }

    /**
     * Starts database transaction
     * @return bool|resource işlem başarılı olursa hareket nesnesi alsi durumda false döner
     */
    public function begin(){
        $this->transactions = !!$this->exec('BEGIN');
        
        return $this->transactions;
    }

    /**
     * Commits database transaction
     * @return bool success result 
     */
    public function commit(){
        $ended = false;
        if($this->transactions){
            $ended = $this->exec('COMMIT');
            $this->transactions = !$ended;
        }
        
        return !!$ended;
    }

    /**
     * Rollbacks database transaction
     * @return bool işlem başarı durumu
     */
    public function rollback(){
        $rollbackResult = false;
        if($this->islemler){
            $rollbackResult = $this->exec('ROLLBACK');
            $this->islemler = !$rollbackResult;
        }
        
        return !!$rollbackResult;
    }

    /**
     * Executes SELECT statement and returns @see PgReader Object or FALSE if errror accour
     * @param $query
     * @param null $params
     * @return false|PgReader
     */
    public function select($query, $params = null)
    {
        $result = $this->exec($query, $params);

        if($result) {

            return new PgReader($result, $this->fetchMode);
        }

        return $result;
    }

    public function insert($query, $params = null)
    {
        $result = false;

        $resource = $this->exec($query, $params);

        if($resource){
            if(is_resource($resource)){
                if($this->fetchMode == 0){
                    $result = pg_fetch_object($resource);
                } else {
                    $result = pg_fetch_array($resource, null, $this->fetchMode);
                }
                pg_free_result($resource);
            } else {
                $result = pg_affected_rows($this->connection);
            }
        }

        return $result;
    }

    public function update($query, $params = null)
    {
        return $this->insert($query, $params);
    }

    public function delete($query, $params = null)
    {
        return $this->insert($query, $params);
    }

    /**
     * Error Message
     * @return string
     */
    public function errorMessage(){
        return pg_errormessage($this->connection);
    }

    /**
     * Last Error Message
     * @return string
     */
    public function lastError(){
        return pg_last_error($this->connection);
    }
    
}