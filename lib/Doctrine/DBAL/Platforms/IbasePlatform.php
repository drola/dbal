<?php

namespace Doctrine\DBAL\Platforms;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Table;

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
            'long' => 'bigint',
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
            'time' => 'time',
            'text' => 'text'
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

    /**
     * {@inheritDoc}
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef) {
        return '';
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
        return 'DATEADD(' . $days . ' DAY TO ' . $date . ')';
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
        return 'DATEADD(' . $months . ' MONTH TO ' . $date . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableForeignKeysSQL($table) {
        $table = strtoupper($table);

        return 'SELECT
    FROM_TABLE.RDB$RELATION_NAME AS FROM_TABLE,
    FROM_FIELD.RDB$FIELD_NAME AS FROM_FIELD,
    TO_TABLE.RDB$RELATION_NAME TO_TABLE,
    TO_FIELD.RDB$FIELD_NAME AS TO_FIELD,
    REF.RDB$UPDATE_RULE AS UPDATE_RULE,
    REF.RDB$DELETE_RULE AS DELETE_RULE,
    RCS.RDB$CONSTRAINT_NAME AS CONSTRAINT_NAME,
    FROM_FIELD.RDB$FIELD_POSITION AS FIELD_POSITION
FROM
    RDB$INDICES FROM_TABLE INNER JOIN RDB$INDEX_SEGMENTS FROM_FIELD ON FROM_FIELD.RDB$INDEX_NAME = FROM_TABLE.RDB$INDEX_NAME
    INNER JOIN RDB$INDICES TO_TABLE ON TO_TABLE.RDB$INDEX_NAME = FROM_TABLE.RDB$FOREIGN_KEY
    INNER JOIN RDB$INDEX_SEGMENTS TO_FIELD ON TO_TABLE.RDB$INDEX_NAME = TO_FIELD.RDB$INDEX_NAME
    INNER JOIN RDB$RELATION_CONSTRAINTS RCS ON RCS.RDB$INDEX_NAME = FROM_FIELD.RDB$INDEX_NAME
    INNER JOIN RDB$REF_CONSTRAINTS REF ON REF.RDB$CONSTRAINT_NAME = RCS.RDB$CONSTRAINT_NAME
WHERE
    FROM_TABLE.RDB$FOREIGN_KEY IS NOT NULL
    AND FROM_TABLE.RDB$RELATION_NAME=\'' . $table . "'";
    }

    /**
     * {@inheritDoc}
     */
    public function getBitAndComparisonExpression($value1, $value2) {
        return 'BIN_AND(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getBitOrComparisonExpression($value1, $value2) {
        return 'BIN_OR(' . $value1 . ', ' . $value2 . ')';
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
        return 'SELECT INDICES.RDB$INDEX_NAME name, RDB$FIELD_NAME column_name, RDB$UNIQUE_FLAG is_unique, CASE cs.RDB$CONSTRAINT_TYPE when \'PRIMARY KEY\' then 1 else 0 end is_primary
            FROM RDB$INDICES INDICES
            LEFT JOIN RDB$INDEX_SEGMENTS SEGMENTS ON INDICES.RDB$INDEX_NAME=SEGMENTS.RDB$INDEX_NAME
            LEFT JOIN RDB$RELATION_CONSTRAINTS cs ON cs.RDB$INDEX_NAME = INDICES.RDB$INDEX_NAME AND cs.RDB$CONSTRAINT_TYPE=\'PRIMARY KEY\'
            WHERE INDICES.RDB$RELATION_NAME=UPPER(\'' . $table . '\') AND RDB$SYSTEM_FLAG=0 ORDER BY RDB$INDEX_ID, RDB$FIELD_POSITION';
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

    /**
     * {@inheritDoc}
     */
    public function getSubstringExpression($value, $position, $length = null) {
        if ($length !== null) {
            return "SUBSTR($value, $position, $position+$length)";
        }

        return "SUBSTR($value, $position)";
    }

    /**
     * {@inheritDoc}
     */
    public function getNowExpression($type = 'timestamp') {
        switch ($type) {
            case 'date':
                return 'CURRENT_DATE';
            case 'time':
                return 'CURRENT_TIME';
            case 'timestamp':
            default:
                return 'CURRENT_TIMESTAMP';
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateSequenceSQL(\Doctrine\DBAL\Schema\Sequence $sequence) {
        if ($sequence->getAllocationSize() != 1 || $sequence->getInitialValue() != 1) {
            throw DBALException::notSupported(__METHOD__ . ': Only sequences with step of 1 are supported.');
        }

        return 'CREATE SEQUENCE ' . $sequence->getQuotedName($this);
        //     ' ALTER SEQUENCE ' . $sequence->getQuotedName($this) . ' RESTART WITH ' . $sequence->getInitialValue();
    }

    /**
     * {@inheritDoc}
     */
    public function getDropSequenceSQL($sequence) {
        if ($sequence instanceof \Doctrine\DBAL\Schema\Sequence) {
            $sequence = $sequence->getQuotedName($this);
        }
        return 'DROP SEQUENCE ' . $sequence;
    }

    /**
     * {@inheritDoc}
     */
    public function quoteSingleIdentifier($str) {
        $c = $this->getIdentifierQuoteCharacter();

        //Uppercase and escape
        return $c . str_replace($c, $c . $c, strtoupper($str)) . $c;
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset) {
        $clause = '';
        if ($limit !== null) {
            $clause .= ' FIRST ' . $limit;
        }

        if ($offset !== null) {
            $clause .= ' SKIP ' . $offset;
        }

        return preg_replace("/^\s*SELECT/i", "SELECT " . $clause, $query);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateDiffExpression($date1, $date2) {
        return "DATEDIFF(day, CAST($date2 AS timestamp), CAST($date1 AS timestamp))";
    }

    public function getSequenceNextValSQL($sequenceName) {
        return 'SELECT NEXT VALUE FOR \'' . $sequenceName . '\' FROM RDB$DATABASE';
    }

    public function getListSequencesSQL($database) {
        return 'SELECT RDB$GENERATOR_NAME sequence_name FROM RDB$GENERATORS WHERE RDB$SYSTEM_FLAG=0';
    }

    /**
     * {@inheritDoc}
     */
    public function getTruncateTableSQL($tableName, $cascade = false) {
        return 'DELETE FROM ' . $tableName;
    }

    /**
     * {@inheritDoc}
     */
    public function getDummySelectSQL() {
        return 'SELECT 1 FROM RDB$DATABASE';
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL($table, array $columns, array $options = array()) {
        $indexes = isset($options['indexes']) ? $options['indexes'] : array();
        $options['indexes'] = array();
        $sql = parent::_getCreateTableSQL($table, $columns, $options);

        foreach ($columns as $name => $column) {
            if (isset($column['sequence'])) {
                $sql[] = $this->getCreateSequenceSQL($column['sequence'], 1);
            }

            if (isset($column['autoincrement']) && $column['autoincrement'] ||
                    (isset($column['autoinc']) && $column['autoinc'])) {
                $sql = array_merge($sql, $this->getCreateAutoincrementSql($name, $table));
            }
        }

        if (isset($indexes) && !empty($indexes)) {
            foreach ($indexes as $index) {
                $sql[] = $this->getCreateIndexSQL($index, $table);
            }
        }

        return $sql;
    }

    /**
     * {@inheritDoc}

      public function getCreateDatabaseSQL($name)
      {
      return 'CREATE DATABASE \'' . $name.'\'';
      } */

    /**
     * Firebird does not support this feature.
     *
     * @return boolean
     */
    public function supportsCreateDropDatabase() {
        return false;
    }

    public function supportsForeignKeyConstraints() {
        return false;
    }

    /**
     * Does this platform views ?
     *
     * @return boolean
     */
    public function supportsViews() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function getReservedKeywordsClass() {
        return 'Doctrine\DBAL\Platforms\Keywords\IbaseKeywords';
    }

    protected function quote($string) {
        return str_replace('\'', '\'\'', $string);
    }

    public function getListTableColumnsSQL($table, $database = null) {
        return 'select
                    RF.RDB$RELATION_NAME, \'\', RF.RDB$FIELD_NAME, RF.RDB$DESCRIPTION, T.RDB$TYPE_NAME, ST.RDB$TYPE_NAME RDB$SUB_TYPE_NAME,
                    RF.RDB$DEFAULT_VALUE, RF.RDB$DEFAULT_SOURCE, RF.RDB$NULL_FLAG, RF.RDB$FIELD_POSITION, F.RDB$FIELD_LENGTH,
                    F.RDB$CHARACTER_LENGTH, F.RDB$FIELD_SCALE, F.RDB$FIELD_PRECISION,
                    IXS.RDB$FIELD_POSITION, IXS.RDB$FIELD_POSITION, F.RDB$FIELD_SUB_TYPE
                from RDB$RELATION_FIELDS RF
                left join RDB$RELATION_CONSTRAINTS RC
                    on (RF.RDB$RELATION_NAME = RC.RDB$RELATION_NAME and RC.RDB$CONSTRAINT_TYPE = \'PRIMARY KEY\')
                left join RDB$INDEX_SEGMENTS IXS
                    on (IXS.RDB$FIELD_NAME = RF.RDB$FIELD_NAME and RC.RDB$INDEX_NAME = IXS.RDB$INDEX_NAME)
                inner join RDB$FIELDS F on (RF.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME)
                inner join RDB$TYPES T on (T.RDB$TYPE = F.RDB$FIELD_TYPE and T.RDB$FIELD_NAME = \'RDB$FIELD_TYPE\')
                left join RDB$TYPES ST on (ST.RDB$TYPE = F.RDB$FIELD_SUB_TYPE and ST.RDB$FIELD_NAME = \'RDB$FIELD_SUB_TYPE\')
                where (UPPER(RF.RDB$RELATION_NAME) = UPPER(\'' . $this->quote($table) . '\')) 
                order by RF.RDB$FIELD_POSITION';
    }

    public function getCreateAutoincrementSql($name, $table, $start = 1) {
        $table = strtoupper($table);
        $sql = array();

        $indexName = $table . '_AI_PK';

        $idx = new Index($indexName, array($name), true, true);

        $sql[] = 'execute block AS
DECLARE VARIABLE cnt INTEGER = 0;
BEGIN
  SELECT COUNT(*)
    FROM RDB$RELATION_CONSTRAINTS WHERE RDB$RELATION_NAME=\'' . $table . '\' AND RDB$CONSTRAINT_TYPE=\'PRIMARY KEY\'
    INTO :cnt;
  IF (:cnt = 0) then
    EXECUTE STATEMENT \'' . $this->getCreateConstraintSQL($idx, $table) . '\'
      WITH AUTONOMOUS TRANSACTION;
  SUSPEND;
END;';

        $sequenceName = $table . '_SEQ';
        $sequence = new Sequence($sequenceName, $start);
        $sql[] = $this->getCreateSequenceSQL($sequence);

        $triggerName = $table . '_AI_PK';
        $sql[] = 'CREATE TRIGGER ' . $triggerName . ' FOR ' . $table . '
   ACTIVE BEFORE INSERT POSITION 0
   AS
BEGIN
   IF (NEW.' . $name . ' IS NULL)
    then BEGIN
      NEW.' . $name . ' = GEN_ID(' . $sequenceName . ', 1);
      RDB$SET_CONTEXT(\'USER_SESSION\', \'LAST_INSERT_ID\', NEW.' . $name . ');
     END
END;';

        return $sql;
    }

    public function getDropAutoincrementSql($table) {
        $table = strtoupper($table);
        $trigger = $table . '_AI_PK';

        $sql[] = 'DROP TRIGGER ' . $trigger;
        $sql[] = $this->getDropSequenceSQL($table . '_SEQ');

        $indexName = $table . '_AI_PK';
        $sql[] = $this->getDropConstraintSQL($indexName, $table);

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(TableDiff $diff) {
        $sql = array();
        $commentsSQL = array();
        $columnSql = array();

        foreach ($diff->addedColumns as $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $sql[] = 'ALTER TABLE ' . $diff->name . ' ADD ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $column->toArray());
            if ($comment = $this->getColumnComment($column)) {
                $commentsSQL[] = $this->getCommentOnColumnSQL($diff->name, $column->getName(), $comment);
            }
        }


        foreach ($diff->changedColumns as $columnDiff) {
            if ($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)) {
                continue;
            }

            $column = $columnDiff->column;
            $sql[] = 'ALTER TABLE ' . $diff->name . ' ALTER COLUMN ' . $column->getQuotedName($this) . ' ' . $this->getColumnDeclarationSQL('', $column->toArray());
            if ($columnDiff->hasChanged('comment') && $comment = $this->getColumnComment($column)) {
                $commentsSQL[] = $this->getCommentOnColumnSQL($diff->name, $column->getName(), $comment);
            }
        }

        foreach ($diff->renamedColumns as $oldColumnName => $column) {
            if ($this->onSchemaAlterTableRenameColumn($oldColumnName, $column, $diff, $columnSql)) {
                continue;
            }

            $sql[] = 'ALTER TABLE ' . $diff->name . ' ALTER COLUMN ' . $oldColumnName . ' TO ' . $column->getQuotedName($this);
        }

        foreach ($diff->removedColumns as $column) {
            if ($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }

            $sql[] = 'ALTER TABLE ' . $diff->name . ' DROP ' . $column->getQuotedName($this);
        }


        $tableSql = array();

        if (!$this->onSchemaAlterTable($diff, $tableSql)) {
            if ($diff->newName !== false) {
                $sql[] = 'ALTER TABLE ' . $diff->name . ' RENAME TO ' . $diff->newName;
            }

            $sql = array_merge($sql, $this->_getAlterTableIndexForeignKeySQL($diff), $commentsSQL);
        }

        return array_merge($sql, $tableSql, $columnSql);
    }
    
    /**
     * {@inheritDoc}
     */
    public function getDropForeignKeySQL($foreignKey, $table)
    {
        if ($foreignKey instanceof ForeignKeyConstraint) {
            $foreignKey = $foreignKey->getQuotedName($this);
        }

        if ($table instanceof Table) {
            $table = $table->getQuotedName($this);
        }

        return 'ALTER TABLE ' . $table . ' DROP CONSTRAINT ' . $foreignKey;
    }

}
