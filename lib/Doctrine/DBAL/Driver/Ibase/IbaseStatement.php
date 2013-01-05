<?php

namespace Doctrine\DBAL\Driver\Ibase;

use \Doctrine\DBAL\Driver\Statement;

class IbaseStatement implements \IteratorAggregate, Statement {

    private $_stmt = null;
    private $_stmtResult = null;
    private $_bindParam = array();
    private $_defaultFetchMode = \PDO::FETCH_BOTH;
    private $_fetchModeParam = null;
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
            ibase_commit_ret($this->_connection->getConnection()); //TODO! THIS IS DIRTY!
        }

        //ibase_commit_ret($this->_connection->getConnection()); //TODO! THIS IS DIRTY!


        return $retval;
    }

    /**
     * {@inheritdoc}
     */
    public function fetch($fetchMode = null, $param = null) {
        $fetchMode = $fetchMode ? : $this->_defaultFetchMode;
        $param = $param ? $param : $this->_fetchModeParam;
        $rtrim = function($s) {
                    return is_string($s) ? rtrim($s) : $s;
                };

        switch ($fetchMode) {
            case \PDO::FETCH_BOTH:
                $row = ibase_fetch_assoc($this->_stmtResult, IBASE_TEXT);

                if (is_array($row)) {
                    if ($this->_connection->isRtrimPortabilityRequired()) {
                        $row = array_map($rtrim, $row);
                    }
                    return array_merge($row, array_values($row));
                } else {
                    return false;
                }
            case \PDO::FETCH_ASSOC:
                $row = ibase_fetch_assoc($this->_stmtResult, IBASE_TEXT);
                if (is_array($row)) {
                    if ($this->_connection->isRtrimPortabilityRequired()) {
                        $row = array_map($rtrim, $row);
                    }
                    return $row;
                } else {
                    return false;
                }
            case \PDO::FETCH_NUM:
                $row = ibase_fetch_row($this->_stmtResult, IBASE_TEXT);
                if (is_array($row)) {
                    if ($this->_connection->isRtrimPortabilityRequired()) {
                        $row = array_map($rtrim, $row);
                    }
                    return $row;
                } else {
                    return false;
                }
            case \PDO::FETCH_OBJ:
                $obj = ibase_fetch_object($this->_stmtResult, IBASE_TEXT);
                if ($obj && $this->_connection->isRtrimPortabilityRequired()) {
                    $vars = get_object_vars();
                    foreach ($vars as $k => $v) {
                        $obj->$k = $rtrim($v);
                    }
                }
                return $obj;
            case \PDO::FETCH_CLASS:
                $row = ibase_fetch_assoc($this->_stmtResult, IBASE_TEXT);
                if ($row) {
                    $instance = new $param();
                    $vars = get_class_vars($param);
                    $original_keys = array_keys($vars);
                    $lowercase_keys = array_keys(array_change_key_case($vars, CASE_LOWER));
                    $vars = array_combine($lowercase_keys, $original_keys);
                    foreach ($row as $key => $val) {
                        if (isset($vars[strtolower($key)])) {
                            if ($this->_connection->isRtrimPortabilityRequired()) {

                                $instance->$vars[strtolower($key)] = $rtrim($val);
                            } else {
                                $instance->$vars[strtolower($key)] = $val;
                            }
                        }
                    }
                    return $instance;
                } else
                    return false;
            default:
                throw new IbaseException("Given Fetch-Style " . $fetchMode . " is not supported.");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll($fetchMode = null, $param = null) {
        switch ($fetchMode) {
            case \PDO::FETCH_COLUMN:
                $data = $this->fetchAll(\PDO::FETCH_NUM, $param);
                return array_map(function($row) {
                                    return $row[0];
                                }, $data);
            default:
                $rows = array();
                while ($row = $this->fetch($fetchMode, $param)) {
                    $rows[] = $row;
                }
                return $rows;
        }
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
        $this->_fetchModeParam = $arg2;
    }

}
