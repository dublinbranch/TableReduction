<?php


class TableReduction{

    private array $attributes = array(
        'sourceTable' =>        array( 'required' => true  , 'removeBackTick' => true  , 'value' => "" ) ,
        'destinationTable' =>   array( 'required' => true  , 'removeBackTick' => true  , 'value' => "" ) ,
        /* https://github.com/dublinbranch/phpMysql */
        'db' =>                 array( 'required' => true  , 'removeBackTick' => false , 'value' => "" ) ,
        'select' =>             array( 'required' => false , 'removeBackTick' => false , 'value' => "*" ) ,
        'where' =>              array( 'required' => false , 'removeBackTick' => false , 'value' => "" ) ,
        'groupBy' =>            array( 'required' => false , 'removeBackTick' => false , 'value' => "" ) ,
        'orderBy' =>            array( 'required' => false , 'removeBackTick' => false , 'value' => "" ) ,
        'limit' =>              array( 'required' => false , 'removeBackTick' => false , 'value' => "" ) ,
        'TTL' =>                array( 'required' => false , 'removeBackTick' => false , 'value' => 3600 ) ,
    );
    private int $lastExecutionTime;

    public function __construct( array $parameters ){
        foreach( $this->attributes as $attributeName => $attributeSettings ){
            if( $attributeSettings["required"] && ! isset( $parameters[$attributeName] ) ) {
                $this->throwError("Missing Required Parameter => {$attributeName}");
            }
            $value = $attributeSettings["value"];
            if( isset( $parameters[$attributeName] ) ) {
                if( is_string( $parameters[$attributeName] ) && $attributeSettings["removeBackTick"] ) {
                    $parameters[$attributeName] = $this->removeBacktick( $parameters[$attributeName] );
                }
                if( isset( $parameters[$attributeName] ) && ! empty( $parameters[$attributeName] ) ) {
                    $value = $parameters[$attributeName];
                }
            }
            $this->$attributeName = $value;
        }
    }

    public function generateCacheTable( bool $force = false )  : void {
        $start = time();
        $secondsFromGeneration = $this->getLastGenerationTS();
        if( $secondsFromGeneration < 0 || $secondsFromGeneration > $this->TTL || $force ){

            $create = "CREATE TABLE IF NOT EXISTS  {$this->destinationTable} AS SELECT 1" ;
            $this->db->query( $create );

            $temp = "{$this->destinationTable}_TEMP";
            $neu = $this->destinationTable;
            $old = "{$this->destinationTable}_old";

            $drop = "DROP TABLE IF EXISTS {$temp}";
            $this->db->query( $drop );

            $create = <<<EOD
CREATE TABLE %s
ENGINE=InnoDB DEFAULT CHARSET=utf8
AS SELECT
    %s
FROM
    %s
/* Where */
%s
/* Group By */
%s
/* Order By */
%s
/* Limit */
%s
EOD;
            $sqlWhere = "";
            if( ! empty( $this->where ) ){
                $sqlWhere = " WHERE {$this->where} ";
            }
            $sqlGroupBy = "";
            if( ! empty( $this->groupBy ) ){
                $sqlGroupBy = " GROUP BY {$this->groupBy} ";
            }
            $sqlOrderBy = "";
            if( ! empty( $this->orderBy ) ){
                $sqlOrderBy = " ORDER BY {$this->orderBy} ";
            }
            $sqlLimit = "";
            if( ! empty( $this->limit ) ){
                $sqlLimit = " LIMIT {$this->limit} ";
            }
            $create = sprintf( $create ,
                $temp ,
                $this->select ,
                $this->sourceTable ,
                $sqlWhere ,
                $sqlGroupBy ,
                $sqlOrderBy ,
                $sqlLimit
            );
            $this->db->query( $create );

            $rename = "RENAME TABLE $neu TO $old , $temp To $neu;";
            $this->db->query( $rename );

            $dropold = "DROP TABLE {$old}";
            $this->db->query( $dropold );
        }
        $end = time();
        $this->lastExecutionTime = $end - $start;
    }

    private function getLastGenerationTS() : int{
        $table = $this->destinationTable;
        $tableSchema = "";
        $queryValues = array();
        if( strpos( $this->destinationTable , '.' ) !== false ){
            $table = explode( '.' , $table );
            $tableSchema = "table_schema = FROM_BASE64( '%s' ) AND";
            foreach( $table as $elements ){
                $queryValues[] = $elements;
            }
        }else{
            $queryValues[] = $table;
        }
        $queryValues = array_map( 'base64_encode' , $queryValues );
        $skel = <<<EOD
SELECT
    UNIX_TIMESTAMP( create_time ) AS ts
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    {$tableSchema} table_name = FROM_BASE64( '%s' )
EOD;
        $query = vsprintf( $skel , $queryValues );
        $queryResult = $this->db->getLine( $query );
        if( empty( $queryResult ) ){
            return -1;
        }else{
            return time() - $queryResult->ts;
        }
    }

    private function removeBacktick( string $word ) : string {
        return str_replace( '`' , '' , $word );
    }

    private function throwError( string $error ) : void {
        throw new Exception( $error );
    }

    public function getLastExecutionTime() : string {
        return $this->lastExecutionTime;
    }
}
