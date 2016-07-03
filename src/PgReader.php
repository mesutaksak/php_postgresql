<?php

namespace PhpPostgresql;


class PgReader
{
    /**
     * @var mixed
     */
    protected $resource;

    /**
     * Fetch Mode Default 0 is Object,  PGSQL_ASSOC = 1, PGSQL_NUM = 2, PGSQL_BOTH = 3
     * @var int
     */
    public $fetchMode;

    /**
     * @param mixed $resource
     * @param int $fetchMode
     */
    public function __construct($resource, $fetchMode = 0)
    {
        $this->resource = $resource;
        $this->fetchMode = $fetchMode;
    }

    public function __destruct()
    {
        return pg_free_result($this->resource);
    }

    /**
     * Fetch next row in resource
     * @param null $class_name
     * @param null $class_params
     * @return array|object
     */
    public function fetch($class_name = null, $class_params = null)
    {
        if($this->fetchMode){
            $row = pg_fetch_array($this->resource, null, $this->fetchMode);
        }
        else {
            
            if($class_name){
                $row = $class_params 
                    ? pg_fetch_object($this->resource, null, $class_name, $class_params) 
                    : pg_fetch_object($this->resource, null, $class_name);
            }
            else
                $row = pg_fetch_object($this->resource);
        }
        
        return $row;
    }

    /**
     * Fetch all , executes $closure function and pass row to closure as param
     * @param $closure
     */
    public function fetchAll($closure)
    {
        if($this->fetchMode){
            while( $row = pg_fetch_array($this->resource, null, $this->fetchMode) ) {
                $closure( $row );
            }
        }
        else {
            while( $row = pg_fetch_object($this->resource) ) {
                $closure( $row );
            }
        }
    }

    /**
     * Reads all rows and returns rows array
     * @return array|bool
     */
    public function readAll()
    {
        $rows = false;

        if($this->fetchMode){
            while( $row = pg_fetch_array($this->resource, null, $this->fetchMode) ) {
                $rows[] = $row;
            }
        }
        else {
            while( $row = pg_fetch_object($this->resource) ) {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    /**
     * Returns row count from result set
     * @return int
     */
    public function rowCount()
    {
        return pg_num_rows($this->resource);
    }

    /**
     * Returns field count from result set
     * @return int
     */
    public function fieldCount()
    {
        return pg_num_fields($this->resource);
    }

    /**
     * Returns field name with given index
     * @param int $field_index
     * @return string|false
     */
    public function fieldName($field_index)
    {
        return pg_field_name($this->resource, $field_index);
    }

    /**
     * Return field index given name
     * @param string $field_name
     * @return int stün numarası veya -1
     */
    public function fieldIndex($field_name)
    {
        return pg_field_num($this->resource, $field_name);
    }
}