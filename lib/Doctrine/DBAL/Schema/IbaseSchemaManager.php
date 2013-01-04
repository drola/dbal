<?php

namespace Doctrine\DBAL\Schema;

use Doctrine\DBAL\Event\SchemaIndexDefinitionEventArgs;
use Doctrine\DBAL\Events;

class IbaseSchemaManager extends AbstractSchemaManager {

    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        $tableColumn = \array_change_key_case($tableColumn, CASE_LOWER);

        $dbType = trim(strtolower($tableColumn['rdb$type_name']));
        $type = array();
        $length = $unsigned = $fixed = null;
        
        if ( ! empty($tableColumn['rdb$character_length'])) {
            $length = $tableColumn['rdb$character_length'];
        }

        if (stripos($tableColumn['rdb$default_value'], 'NULL') !== null) {
            $tableColumn['rdb$default_value'] = null;
        }

        $precision = null;
        $scale = null;

        $type = $this->_platform->getDoctrineTypeMapping($dbType);
        $type = $this->extractDoctrineTypeFromComment($tableColumn['rdb$description'], $type);
        $tableColumn['rdb$description'] = $this->removeDoctrineTypeFromComment($tableColumn['rdb$description'], $type);
        switch($dbType) {
            case 'long': //integers
                if($tableColumn['rdb$field_length'] == 4) {
                    $type = 'integer';
                }
        }
        /*switch ($dbType) {
            case 'number':
                if ($tableColumn['data_precision'] == 20 && $tableColumn['data_scale'] == 0) {
                    $precision = 20;
                    $scale = 0;
                    $type = 'bigint';
                } elseif ($tableColumn['data_precision'] == 5 && $tableColumn['data_scale'] == 0) {
                    $type = 'smallint';
                    $precision = 5;
                    $scale = 0;
                } elseif ($tableColumn['data_precision'] == 1 && $tableColumn['data_scale'] == 0) {
                    $precision = 1;
                    $scale = 0;
                    $type = 'boolean';
                } elseif ($tableColumn['data_scale'] > 0) {
                    $precision = $tableColumn['data_precision'];
                    $scale = $tableColumn['data_scale'];
                    $type = 'decimal';
                }
                $length = null;
                break;
            case 'pls_integer':
            case 'binary_integer':
                $length = null;
                break;
            case 'varchar':
            case 'varchar2':
            case 'nvarchar2':
                $length = $tableColumn['char_length'];
                $fixed = false;
                break;
            case 'char':
            case 'nchar':
                $length = $tableColumn['char_length'];
                $fixed = true;
                break;
            case 'date':
            case 'timestamp':
                $length = null;
                break;
            case 'float':
                $precision = $tableColumn['data_precision'];
                $scale = $tableColumn['data_scale'];
                $length = null;
                break;
            case 'clob':
            case 'nclob':
                $length = null;
                break;
            case 'blob':
            case 'raw':
            case 'long raw':
            case 'bfile':
                $length = null;
                break;
            case 'rowid':
            case 'urowid':
            default:
                $length = null;
        }*/

        $options = array(
            'notnull'    => !$tableColumn['rdb$null_flag'],
            'fixed'      => (bool) $fixed,
            'unsigned'   => false,
            'default'    => $tableColumn['rdb$default_value'],
            'length'     => $length,
            'precision'  => $precision,
            'scale'      => $scale,
            'comment'       => $tableColumn['rdb$description'],
            'platformDetails' => array(),
        );

        return new Column(trim($tableColumn['rdb$field_name']), \Doctrine\DBAL\Types\Type::getType($type), $options);
    }

    /**
     * {@inheritdoc}
     *
     * @override
     */
    public function createDatabase($database) {
        $params = $this->_conn->getParams();

        $quote_string = function($str) {
                    return str_replace('\'', '\'\'', $str);
                };
        $database_escaped = $quote_string($this->_formatDbConnString($database, $params['host'], $params['port']));
        $user_escaped = $quote_string($params['username']);
        $password_escaped = $quote_string($params['password']);
        $isql_command = "echo \"CREATE DATABASE '$database_escaped' user '$user_escaped' password '$password_escaped'; QUIT;\" | isql-fb";

        $data = array();
        $result = 0;
        exec($isql_command, $data, $result);
        if ($result) {
            throw new \DBALException("Creating Firebird/Interbase database failed. " . implode("\n", $data));
        }
    }

    /**
     * {@inheritdoc}
     *
     * @override
     */
    public function dropDatabase($database) {
        $params = $this->_conn->getParams();

        $quote_string = function($str) {
                    return str_replace('\'', '\'\'', $str);
                };
        $database_escaped = $quote_string($this->_formatDbConnString($database, $params['host'], $params['port']));
        $user_escaped = $quote_string($params['username']);
        $password_escaped = $quote_string($params['password']);
        $isql_command = "echo \"CONNECT '$database_escaped' user '$user_escaped' password '$password_escaped'; DROP DATABASE; QUIT;\" | isql-fb";

        $data = array();
        $result = 0;
        exec($isql_command, $data, $result);
        if ($result) {
            throw new \DBALException("Dropping Firebird/Interbase database failed. " . implode("\n", $data));
        }
    }

    /**
     * Format a connection string
     * 
     * @param string $dbname Database name/path
     * @param type $host Host
     * @param string $port Port
     * @return string Connection string
     */
    protected function _formatDbConnString($dbname, $host, $port) {
        if (is_numeric($port)) {
            $port = '/' . (integer) $port;
        }
        if ($dbname) {
            $dbname = ':' . $dbname;
        }
        return $host . $port . $dbname;
    }
    
    protected function _getPortableTableDefinition($table)
    {
        return trim($table['RDB$RELATION_NAME']);
    }
    
    /**
     * @license New BSD License
     * @link http://ezcomponents.org/docs/api/trunk/DatabaseSchema/ezcDbSchemaPgsqlReader.html
     * @param  array $tableIndexes
     * @param  string $tableName
     * @return array
     */
    protected function _getPortableTableIndexesList($tableIndexes, $tableName=null)
    {
        $indexBuffer = array();
        foreach ( $tableIndexes as $tableIndex ) {
            $tableIndex = \array_change_key_case($tableIndex, CASE_LOWER);

            $keyName = strtolower($tableIndex['name']);

            if ( $tableIndex['is_primary'] ) {
                $keyName = 'primary';
                $buffer['primary'] = true;
            } else {
                $buffer['primary'] = false;
            }
            $buffer['non_unique'] = ( $tableIndex['is_unique'] == 0 ) ? true : false;
            $buffer['key_name'] = $keyName;
            $buffer['column_name'] = $tableIndex['column_name'];
            $indexBuffer[] = $buffer;
        }
        return parent::_getPortableTableIndexesList($indexBuffer, $tableName);
    }

}
