<?php

namespace PhpPostgresql;


class PgConnection
{
    /**
     * @var resource|false|null
     */
    protected $connection;

    /**
     * @var int Transaction count
     */
    protected $transactions = 0;

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
     * @var float Query executing time ms
     */
    protected $queryTime;

    /**
     * Fetch Mode Default 0 is Object,  PGSQL_ASSOC = 1, PGSQL_NUM = 2, PGSQL_BOTH = 3
     * @var int
     */
    public $fetchMode = 0;

    /**
     * Places params in query build executable sql string
     * this is for debug able to see query
     * @param string $query prepared statement name
     * @param array $params query paramerters array
     * @return mixed
     */
    private function buildSql($query, $params)
    {
        if(is_array($params) && count($params) > 0){
            $query_string = $query;
            foreach($params as $index => $value){
                $value_str = is_null($value) ? 'NULL' : sprintf("E'%s'", str_replace( '\\', '\\\\', $value ));
                $query_string = str_replace('$' .($index+1), $value_str, $query_string);
            }

            return $query_string;
        }

        return $query;
    }

    /**
     * @param string $query prepared statement name
     * @param array $params query paramerters array
     * @param array $info extra information for log
     */
    private function logQuery($query, $params, $info = [])
    {
        if(!isset($info['prepared'])){
            $info['query'] = $this->buildSql($query, $params);
        }
        $info['params'] = $params;
        $info['queryTime'] = $this->getQueryTime();

        $this->queries[] = $info;
    }

    /**
     * Returns statement execute result
     * For (SUM , COUNT, MAX, RETURNING * after INSERT|UPDATE|DELETE)  return single row,
     * For Non query results statement like UPDATE, DELETE, INSERT return affected rows
     * @param $resource
     * @return array|bool|int|object
     */
    private function statementResult($resource)
    {
        $result = false;

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
        $this->debug = isset($settings['debug']) ? $settings['debug'] : false;
        if($this->debug){
            $this->queries = [];
        }
    }

    public function __destruct()
    {
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
     * @param string $query prepared statement name
     * @param array|null $params query paramerters array
     * @return resource
     */
    public function exec($query, $params = null)
    {

        $startTime = microtime(true);

        $result = is_null($params)
            ? pg_query($this->connection, $query)
            : pg_query_params($this->connection, $query, $params);

        $this->queryTime = microtime(true) - $startTime;

        if($this->debug){
            $this->logQuery($query, $params);
        }

        if($result === false){
            throw new \ErrorException(pg_last_error ($this->connection));
        }

        return $result;
    }

    /**
     * Starts database transaction
     * @return bool|resource işlem başarılı olursa hareket nesnesi alsi durumda false döner
     */
    public function beginTransaction(){
        $result = $this->exec('BEGIN');

        if($result){
            $this->transactions++;
        }

        return $result;
    }

    /**
     * Commits database transaction
     * @return bool success result
     */
    public function commit(){

        if($this->transactions){
            $result = $this->exec('COMMIT');
            $this->transactions--;
            if($this->transactions < 0){
                $this->transactions = 0;
            }

            return $result;
        }

        return false;
    }

    /**
     * Rollbacks database transaction
     * @return bool işlem başarı durumu
     */
    public function rollBack(){
        if($this->transactions){
            $result = $this->exec('ROLLBACK');
            $this->transactions--;
            if($this->transactions < 0){
                $this->transactions = 0;
            }

            return $result;
        }

        return false;
    }

    /**
     * For single row result like (SUM , COUNT, MAX) queries and Non query results statement like UPDATE, DELETE, INSERT
     * @param string $query prepared statement name
     * @param array|null $params query paramerters array
     * @return mixed if result is resource then returns first row from result set, if statement non query returns affected rows count
     * on error return FALSE
     */
    public function statement($query, $params = null)
    {
        $resource = $this->exec($query, $params);

        return $this->statementResult($resource);
    }

    /**
     * Executes SELECT statement and returns @see PgReader Object or FALSE if errror accour
     * @param string $query prepared statement name
     * @param array|null $params query paramerters array
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

    /**
     * @param string $query prepared statement name
     * @param array|null $params query paramerters array
     * @return mixed
     */
    public function insert($query, $params = null)
    {
        return $this->statement($query, $params);
    }

    /**
     * @param string $query prepared statement name
     * @param array|null $params query paramerters array
     * @return mixed
     */
    public function update($query, $params = null)
    {
        return $this->statement($query, $params);
    }

    /**
     * @param string $query prepared statement name
     * @param array|null $params query paramerters array
     * @return mixed
     */
    public function delete($query, $params = null)
    {
        return $this->statement($query, $params);
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

    /**
     * Returns number of affected records
     * @return int
     */
    public function affectedRows()
    {
        return pg_affected_rows($this->connection);
    }

    /**
     * @param string $name prepared statement name
     * @param string $query query string will be prepare for execute later
     * @return resource
     */
    public function prepare($name, $query)
    {
        $startTime = microtime(true);
        $result = pg_prepare($this->connection, $name, $query);
        $this->queryTime = microtime(true) - $startTime;

        if($this->debug){
            $this->logQuery($query, null, ['name' => $name]);
        }

        return $result;
    }

    /**
     * Executes prepared statement
     * @see $this->statement
     * @param string $name prepared statement name
     * @param array $params query paramerters array
     * @return mixed if result is resource then returns first row from result set,
     * if statement non query returns affected rows count on error return FALSE
     */
    public function executeStatement($name , $params)
    {
        $startTime = microtime(true);
        $result = pg_execute($this->connection, $name, $params);
        $this->queryTime = microtime(true) - $startTime;

        if($this->debug){
            $this->logQuery($name, $params, ['name' => $name, 'prepared' => true]);
        }

        return $this->statementResult($result);
    }

    /**
     * Executes prepared Select query
     * @see $this->select
     * @param string $name prepared statement name
     * @param array $params query paramerters array
     * @return PgReader|bool
     */
    public function executeSelect($name , $params)
    {
        $startTime = microtime(true);
        $result = pg_execute($this->connection, $name, $params);
        $this->queryTime = microtime(true) - $startTime;

        if($this->debug){
            $this->logQuery($name, $params, ['name' => $name, 'prepared' => true]);
        }

        if(is_resource($result)) {

            return new PgReader($result, $this->fetchMode);
        }

        return $result;
    }

    /**
     * Executes prepared INSERT statement
     * @see $this->insert
     * @param string $name prepared statement name
     * @param array $params query paramerters array
     * @return mixed
     */
    public function executeInsert($name , $params)
    {
        return $this->executeStatement($name, $params);
    }

    /**
     * Executes prepared UPDATE statement
     * @see $this->update
     * @param string $name prepared statement name
     * @param array $params query paramerters array
     * @return mixed
     */
    public function executeUpdate($name, $params)
    {
        return $this->executeStatement($name, $params);
    }

    /**
     * Executes prepared DELETE statement
     * @see $this->delete
     * @param string $name prepared statement name
     * @param array $params query paramerters array
     * @return mixed
     */
    public function executeDelete($name, $params)
    {
        return $this->executeStatement($name, $params);
    }

    /**
     * Returns executed queries for watching
     * @return array|null
     */
    public function getQueries()
    {
        return $this->queries;
    }

    /**
     * Returns last query executing time
     * @return float
     */
    public function getQueryTime()
    {
        return round($this->queryTime, 8);
    }
}