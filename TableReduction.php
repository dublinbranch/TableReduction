<?php


class TableReduction{

    private string $sourceTable;
    private string $destinationTable;
    private string $select;
    private string $where;
    private string $groupBy;
    /* https://github.com/dublinbranch/phpMysql */
    private DBWrapper $db;
    private int $TTL;

    public function __construct(
        string $sourceTable ,
        string $destinationTable ,
        string $select ,
        string $where ,
        string $groupBy ,
        DBWrapper $db ,
        int $TTL
    ){
        $this->sourceTable = $this->removeBacktick( $sourceTable );
        $this->destinationTable = $this->removeBacktick( $destinationTable );
        $this->select = $select;
        $this->where = $where;
        $this->groupBy = $groupBy;
        $this->db = $db;
        $this->TTL = $TTL;
    }

    public function generateCacheTable( bool $force = false )  : void{
        $secondsFromGeneration = $this->getLastGenerationTS();
        if( $secondsFromGeneration < 0 || $secondsFromGeneration > $this->TTL || $force ){

            $create = "CREATE TABLE IF NOT EXISTS  {$this->destinationTable} AS SELECT 1" ;
            $this->db->query( $create );

            $temp = "{$this->destinationTable}_TEMP";
            $neu = "$this->destinationTable";
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
EOD;
            $sqlWhere = "";
            if( ! empty( $this->where ) ){
                $sqlWhere = " WHERE {$this->where} ";
            }
            $sqlGroupBy = "";
            if( ! empty( $this->groupBy ) ){
                $sqlGroupBy = " GROUP BY {$this->groupBy} ";
            }
            $create = sprintf( $create , $temp , $this->select , $this->sourceTable , $sqlWhere , $sqlGroupBy );
            $this->db->query( $create );

            $rename = "RENAME TABLE $neu TO $old , $temp To $neu;";
            $this->db->query( $rename );

            $dropold = "DROP TABLE {$old}";
            $this->db->query( $dropold );
        }
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
}
