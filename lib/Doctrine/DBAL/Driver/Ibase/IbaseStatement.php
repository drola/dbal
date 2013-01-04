<?php

namespace Doctrine\DBAL\Driver\Ibase;

use \Doctrine\DBAL\Driver\Statement;

class IbaseStatement implements \IteratorAggregate, Statement {

    private $_stmt = null;
    private $_stmtResult = null;
    private $_bindParam = array();
    private $_defaultFetchMode = \PDO::FETCH_BOTH;
    private $_stmtRowCount = 0;
    private $_connection = null;

    public function __construct($stmt, IbaseConnection $connection) {
        $this->_stmt = $stmt;
        $this->_connection = $connection;
    }

    public function bindParam($column, &$variable, $type = null, $length = null) {
        $this->_bindParam[$column] = $variable;
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function bindValue($param, $value, $type = null) {
        return $this->bindParam($param, $value, $type);
    }

    /**
     * {@inheritdoc}
     */
    public function closeCursor() {
        if (!$this->_stmt) {
            return false;
        }

        $this->_bindParam = array();
        ibase_free_result($this->_stmtResult);
        $ret = ibase_free_query($this->_stmt);
        $this->_stmt = false;
        return $ret;
    }

    public function columnCount() {
        if (!$this->_stmtResult) {
            return false;
        }
        return ibase_num_fields($this->_stmtResult);
    }

    public function errorCode() {
        return ibase_errcode();
    }

    public function errorInfo() {
        return ibase_errmsg();
    }

    /**
     * {@inheritdoc}
     */
    public function execute($params = null) {
        if (!$this->_stmt) {
            return false;
        }


        if ($params === null) {
            ksort($this->_bindParam);
            $params = array_values($this->_bindParam);
        }

        if ($params) {
            array_unshift($params, $this->_stmt);
            $retval = @call_user_func_array(
                            'ibase_execute', $params
            );
        } else {
            $retval = @ibase_execute($this->_stmt);
        }

        if ($retval === false) {
            throw new IbaseException(ibase_errmsg());
        }

        $this->_stmtResult = $retval;

        if ($trans = $this->_connection->getTransaction()) {
            $this->_stmtRowCount = ibase_affected_rows($trans);
        } else {
            $this->_stmtRowCount = ibase_affected_rows($this->_connection->getConnection());
        }
        
        ibase_commit_ret($this->_connection->getConnection()); 

        return $retval;
    }

    /**
     * {@inheritdoc}
     */
    public function fetch($fetchMode = null) {
        $fetchMode = $fetchMode ? : $this->_defaultFetchMode;
        switch ($fetchMode) {
            case \PDO::FETCH_BOTH:
                $row = ibase_fetch_assoc($this->_stmtResult, IBASE_TEXT);
                return array_merge($row, array_values($row));
            case \PDO::FETCH_ASSOC:
                return ibase_fetch_assoc($this->_stmtResult, IBASE_TEXT);
            case \PDO::FETCH_NUM:
                return ibase_fetch_row($this->_stmtResult, IBASE_TEXT);
            case \PDO::FETCH_OBJ:
                return ibase_fetch_object($this->_stmtResult, IBASE_TEXT);
            default:
                throw new IbaseException("Given Fetch-Style " . $fetchMode . " is not supported.");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll($fetchMode = null) {
        $rows = array();
        while ($row = $this->fetch($fetchMode)) {
            $rows[] = $row;
        }
        return $rows;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchColumn($columnIndex = 0) {
        $row = $this->fetch(\PDO::FETCH_NUM);
        if ($row && isset($row[$columnIndex])) {
            return $row[$columnIndex];
        }
        return false;
    }

    public function getIterator() {
        $data = $this->fetchAll();
        return new \ArrayIterator($data);
    }

    public function rowCount() {
        return $this->_stmtRowCount ? $this->_stmtRowCount : 0;
    }

    /**
     * {@inheritdoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null) {
        $this->_defaultFetchMode = $fetchMode;
    }

}
