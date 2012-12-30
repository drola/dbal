<?php

namespace Doctrine\DBAL\Platforms;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\TableDiff;

class IbasePlatform extends AbstractPlatform {

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings() {
        $this->doctrineTypeMapping = array(
            'int64' => 'bigint',
            'char' => 'string',
            'timestamp' => 'datetime',
            'decimal' => 'decimal',
            'float' => 'float',
            'blob' => 'blob',
            'integer' => 'integer',
            'blob sub_type_text' => 'string',
            'numeric' => 'decimal',
            'varchar' => 'string',
            'varying' => 'string',
            'double' => 'float',
            'smallint' => 'smallint',
            'date' => 'date',
            'time' => 'time'
        );
    }

    public function getBigIntTypeDeclarationSQL(array $columnDef) {
        return 'INT64';
    }

    public function getBlobTypeDeclarationSQL(array $field) {
        return 'BLOB';
    }

    public function getBooleanTypeDeclarationSQL(array $columnDef) {
        return 'CHAR(1)';
    }

    public function getClobTypeDeclarationSQL(array $field) {
        return 'BLOB SUB_TYPE TEXT';
    }

    public function getIntegerTypeDeclarationSQL(array $columnDef) {
        return 'INTEGER';
    }

    public function getName() {
        return 'ibase';
    }

    public function getSmallIntTypeDeclarationSQL(array $columnDef) {
        return 'SMALLINT';
    }

    /**
     * {@inheritDoc}
     */
    public function getRegexpExpression() {
        return 'SIMILAR TO';
    }

    /**
     * {@inheritDoc}
     */
    public function getGuidExpression() {
        return 'GEN_UUID()';
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression($str, $substr, $startPos = false) {
        if ($startPos == false) {
            return 'POSITION(' . $substr . ', ' . $str . ')';
        }

        return 'POSITION(' . $substr . ', ' . $str . ', ' . $startPos . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getConcatExpression() {
        $args = func_get_args();
        return '(' . join(' || ', (array) $args) . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddDaysExpression($date, $days) {
        return 'DATE_ADD(' . $days . ' DAY, ' . $date . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubDaysExpression($date, $days) {
        return $this->getDateAddDaysExpression($date, -1 * $days);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddMonthExpression($date, $months) {
        return 'DATE_ADD(' . $months . ' MONTH, ' . $date . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubMonthExpression($date, $months) {
        return $this->getDateAddMonthExpression($date, -1 * $months);
    }

    public function getListTableConstraintsSQL($table) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableIndexesSQL($table, $currentDatabase = null) {
        return 'SELECT INDICES.RDB$INDEX_NAME NAME, RDB$FIELD_NAME COLNAME, RDB$UNIQUE_FLAG UNIQUERULE
            FROM RDB$INDICES INDICES
            LEFT JOIN RDB$INDEX_SEGMENTS SEGMENTS ON INDICES.RDB$INDEX_NAME=SEGMENTS.RDB$INDEX_NAME
            WHERE INDICES.RDB$RELATION_NAME=\'' . $table . '\'';
    }

    public function getListViewsSQL($database) {
        return 'SELECT RDB$RELATION_NAME VIEW_NAME
            FROM RDB$RELATIONS REL
            JOIN RDB$TYPES T ON REL.RDB$RELATION_TYPE=T.RDB$TYPE AND T.RDB$TYPE_NAME=\'VIEW\' AND T.RDB$FIELD_NAME=\'RDB$RELATION_TYPE\'';
    }

    public function getCreateViewSQL($name, $sql) {
        return 'CREATE VIEW ' . $name . ' AS ' . $sql;
    }

    public function getDropViewSQL($name) {
        return 'DROP VIEW ' . $name;
    }

    /**
     * {@inheritDoc}
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed) {
        return $fixed ? ($length ? 'CHAR(' . $length . ')' : 'CHAR(255)') : ($length ? 'VARCHAR(' . $length . ')' : 'VARCHAR(255)');
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(array $fieldDeclaration) {
        return 'TIMESTAMP';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(array $fieldDeclaration) {
        return 'DATE';
    }

    /**
     * {@inheritDoc}
     */
    public function getTimeTypeDeclarationSQL(array $fieldDeclaration) {
        return 'TIME';
    }

    /**
     * Obtain DBMS specific SQL code portion needed to set the COLLATION
     * of a field declaration to be used in statements like CREATE TABLE.
     *
     * @param string $collation   name of the collation
     *
     * @return string  DBMS specific SQL code portion needed to set the COLLATION
     *                 of a field declaration.
     */
    public function getCollationFieldDeclaration($collation) {
        return 'COLLATE ' . $collation;
    }

    public function getListTablesSQL() {
        return 'SELECT * FROM RDB$RELATIONS REL JOIN
            RDB$TYPES T ON REL.RDB$RELATION_TYPE=T.RDB$TYPE AND T.RDB$TYPE_NAME!=\'VIEW\' AND T.RDB$FIELD_NAME=\'RDB$RELATION_TYPE\'
            WHERE REL.RDB$SYSTEM_FLAG=0;';
    }

    
    
    

}
